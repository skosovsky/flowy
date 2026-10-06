package redis

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	goredis "github.com/redis/go-redis/v9"

	"github.com/skosovsky/flowy"
	redislease "github.com/skosovsky/flowy/adapters/lease/redis"
	"github.com/skosovsky/flowy/checkpoint"
)

func TestSnapshotWriteRejectsStaleSameOwnerLease(t *testing.T) {
	t.Parallel()
	// Arrange: the successor has the same owner and an unchanged checkpoint revision.
	mr := miniredis.RunT(t)
	client := goredis.NewClient(&goredis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	ctx := context.Background()
	cp := NewCheckpointer[state, string](client, Options{}, checkpoint.JSONSerializer[state]{})
	manager := redislease.NewLeaseManager(client, redislease.Options{})
	saveTestSnapshot(t, cp, 0, 1, "original")
	old, err := manager.Acquire(ctx, "t1", "same-owner", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	mr.FastForward(time.Minute)
	current, err := manager.Acquire(ctx, "t1", "same-owner", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	// Act: neither a stale handle nor an unfenced caller may modify the history.
	for _, writeCtx := range []context.Context{flowy.WithExecutionLease(ctx, old), ctx} {
		_, saveErr := cp.Save(writeCtx, 1, testSnapshot(2, "stale"))
		if !errors.Is(saveErr, flowy.ErrLeaseLost) {
			t.Fatalf("write accepted: %v", saveErr)
		}
	}
	// Assert: rejection did not rely on OCC and did not mutate the checkpoint.
	loaded, revision, err := cp.Load(ctx, "t1")
	if err != nil || revision != 1 || loaded.State.Value != "original" {
		t.Fatalf("checkpoint mutated: %+v %d %v", loaded, revision, err)
	}
	if _, saveErr := cp.Save(flowy.WithExecutionLease(ctx, current), 1, testSnapshot(2, "current")); saveErr != nil {
		t.Fatalf("current handle rejected: %v", saveErr)
	}
	mr.FastForward(time.Minute)
	if _, saveErr := cp.Save(
		flowy.WithExecutionLease(ctx, current),
		2,
		testSnapshot(3, "expired"),
	); !errors.Is(
		saveErr,
		flowy.ErrLeaseLost,
	) {
		t.Fatalf("expired handle accepted: %v", saveErr)
	}
	history, err := cp.GetHistory(ctx, "t1", 0)
	if err != nil || len(history) != 2 || history[0].State.Value != "current" {
		t.Fatalf("history changed by rejected write: %+v %v", history, err)
	}
}
