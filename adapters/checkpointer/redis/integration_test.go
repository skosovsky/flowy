//go:build integration

package redis

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	goredis "github.com/redis/go-redis/v9"

	redislease "github.com/skosovsky/flowy/adapters/lease/redis"

	"github.com/skosovsky/flowy"
	"github.com/skosovsky/flowy/checkpoint"
)

// E2E: paired lease adapter blocks DeleteIfIdle until Release.
func TestE2ELeaseAcquireBlocksDeleteUntilRelease(t *testing.T) {
	mr := miniredis.RunT(t)
	client := goredis.NewClient(&goredis.Options{Addr: mr.Addr()})
	defer func() { _ = client.Close() }()

	const prefix = "flowy"
	cp := NewCheckpointer[state, string](client, Options{Prefix: prefix, LeasePrefix: prefix}, checkpoint.JSONSerializer[state]{})
	leaseMgr := redislease.NewLeaseManager(client, redislease.Options{Prefix: prefix})

	if _, err := cp.Save(context.Background(), 0, testSnapshot(1, "v1")); err != nil {
		t.Fatalf("save: %v", err)
	}
	if err := leaseMgr.Acquire(context.Background(), "t1", "worker", time.Minute); err != nil {
		t.Fatalf("acquire: %v", err)
	}
	if err := cp.DeleteIfIdle(context.Background(), "t1"); !errors.Is(err, flowy.ErrThreadLeaseBusy) {
		t.Fatalf("expected ErrThreadLeaseBusy, got %v", err)
	}
	if err := leaseMgr.Release(context.Background(), "t1", "worker"); err != nil {
		t.Fatalf("release: %v", err)
	}
	if err := cp.DeleteIfIdle(context.Background(), "t1"); err != nil {
		t.Fatalf("delete after release: %v", err)
	}
}

func TestOCCConcurrencyConflict(t *testing.T) {
	mr := miniredis.RunT(t)
	client := goredis.NewClient(&goredis.Options{Addr: mr.Addr()})
	defer func() { _ = client.Close() }()

	cp := NewCheckpointer[state, string](client, Options{}, checkpoint.JSONSerializer[state]{})
	if _, err := cp.Save(context.Background(), 0, testSnapshot(1, "v1")); err != nil {
		t.Fatalf("initial save: %v", err)
	}
	_, err := cp.Save(context.Background(), 0, testSnapshot(2, "stale"))
	if !errors.Is(err, flowy.ErrConcurrencyConflict) {
		t.Fatalf("expected ErrConcurrencyConflict, got %v", err)
	}
}
