//go:build integration

package redis

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/skosovsky/flowy/internal/testdocker"

	goredis "github.com/redis/go-redis/v9"

	"github.com/skosovsky/flowy"
	redislease "github.com/skosovsky/flowy/adapters/lease/redis"
	"github.com/skosovsky/flowy/checkpoint"
)

func TestIntegrationRedisServerFencingAndExactOCC(t *testing.T) {
	t.Parallel()
	address := testdocker.Redis(t)
	// Arrange: independent clients use a unique isolated namespace on a real server.
	first := goredis.NewClient(&goredis.Options{Addr: address})
	second := goredis.NewClient(&goredis.Options{Addr: address})
	t.Cleanup(func() { _ = first.Close(); _ = second.Close() })
	ctx := context.Background()
	prefix := "flowy-task22:" + t.Name() + ":" + time.Now().UTC().Format("20060102150405.000000000")
	cp := NewCheckpointer[state, string](first, Options{Prefix: prefix}, checkpoint.JSONSerializer[state]{})
	a := redislease.NewLeaseManager(first, redislease.Options{Prefix: prefix})
	b := redislease.NewLeaseManager(second, redislease.Options{Prefix: prefix})
	const revision = uint64(9_007_199_254_740_992)
	record, err := checkpoint.EncodeRecord(testSnapshot(revision, "original"), checkpoint.JSONSerializer[state]{})
	if err != nil {
		t.Fatal(err)
	}
	payload, err := marshalRecord(record)
	if err != nil {
		t.Fatal(err)
	}
	if seedErr := first.LPush(ctx, cp.historyKey("t1"), payload).Err(); seedErr != nil {
		t.Fatal(seedErr)
	}
	old, err := a.Acquire(ctx, "t1", "same-owner", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if expireErr := first.PExpire(ctx, cp.leaseKey("t1"), -time.Millisecond).Err(); expireErr != nil {
		t.Fatal(expireErr)
	}
	current, err := b.Acquire(ctx, "t1", "same-owner", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	// Act: exercise all server-side compare/mutate operations, not local mocks.
	_, renewErr := a.Renew(ctx, old, time.Hour)
	releaseErr := a.Release(ctx, old)
	_, staleErr := cp.Save(flowy.WithExecutionLease(ctx, old), revision, testSnapshot(0, "stale"))
	// Assert.
	if current.Incarnation <= old.Incarnation || !errors.Is(renewErr, flowy.ErrLeaseLost) ||
		!errors.Is(releaseErr, flowy.ErrLeaseLost) ||
		!errors.Is(staleErr, flowy.ErrLeaseLost) {
		t.Fatalf(
			"stale server handle accepted: old=%+v current=%+v renew=%v release=%v save=%v",
			old,
			current,
			renewErr,
			releaseErr,
			staleErr,
		)
	}
	writeCtx := flowy.WithExecutionLease(ctx, current)
	if _, saveErr := cp.Save(writeCtx, revision, testSnapshot(0, "current")); saveErr != nil {
		t.Fatal(saveErr)
	}
	if _, saveErr := cp.Save(
		writeCtx,
		revision,
		testSnapshot(0, "aliased"),
	); !errors.Is(
		saveErr,
		flowy.ErrConcurrencyConflict,
	) {
		t.Fatalf("Lua revision alias: %v", saveErr)
	}
	loaded, gotRevision, err := cp.Load(ctx, "t1")
	if err != nil || gotRevision != revision+1 || loaded.State.Value != "current" {
		t.Fatalf("invalid server checkpoint: %+v %d %v", loaded, gotRevision, err)
	}
	if err := b.Release(ctx, current); err != nil {
		t.Fatal(err)
	}
}
