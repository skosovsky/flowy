package redis

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	goredis "github.com/redis/go-redis/v9"

	"github.com/skosovsky/flowy"
)

func TestReusedOwnerIncarnationSurvivesExpiryAndRelease(t *testing.T) {
	// Arrange.
	server := miniredis.RunT(t)
	client := goredis.NewClient(&goredis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	manager := NewLeaseManager(client, Options{})
	ctx := context.Background()
	old, err := manager.Acquire(ctx, "run", "worker", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	server.FastForward(2 * time.Second)
	current, acquireErr := manager.Acquire(ctx, "run", "worker", time.Minute)
	if acquireErr != nil {
		t.Fatal(acquireErr)
	}
	// Act.
	_, renewErr := manager.Renew(ctx, old, time.Hour)
	releaseErr := manager.Release(ctx, old)
	// Assert: same-owner stale operations do not alter successor ownership or TTL.
	if current.Incarnation <= old.Incarnation || !errors.Is(renewErr, flowy.ErrLeaseLost) ||
		!errors.Is(releaseErr, flowy.ErrLeaseLost) ||
		server.TTL(manager.leaseKey("run")) != time.Minute {
		t.Fatalf("ABA mutated lease: %+v %+v %v %v", old, current, renewErr, releaseErr)
	}
	if validReleaseErr := manager.Release(ctx, current); validReleaseErr != nil {
		t.Fatal(validReleaseErr)
	}
	newest, newestErr := manager.Acquire(ctx, "run", "worker", time.Minute)
	if newestErr != nil || newest.Incarnation <= current.Incarnation {
		t.Fatalf("counter recycled: %+v %v", newest, newestErr)
	}
}

func TestFencePrecisionAboveLuaIntegerRange(t *testing.T) {
	// Arrange: Lua doubles cannot exactly represent the next integer.
	server := miniredis.RunT(t)
	client := goredis.NewClient(&goredis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	manager := NewLeaseManager(client, Options{})
	ctx := context.Background()
	if err := client.Set(ctx, manager.fenceKey("run"), "9007199254740992", 0).Err(); err != nil {
		t.Fatal(err)
	}
	// Act: the script reads the exact decimal counter string after INCR.
	lease, err := manager.Acquire(ctx, "run", "worker", time.Minute)
	// Assert.
	if err != nil || lease.Incarnation != 9007199254740993 {
		t.Fatalf("fence rounded: %+v %v", lease, err)
	}
	if releaseErr := manager.Release(ctx, lease); releaseErr != nil {
		t.Fatal(releaseErr)
	}
}
