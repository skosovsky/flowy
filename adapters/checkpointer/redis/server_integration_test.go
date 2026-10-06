//go:build integration

package redis

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/skosovsky/flowy"
	redislease "github.com/skosovsky/flowy/adapters/lease/redis"
	"github.com/skosovsky/flowy/checkpoint"
)

func TestRedisServerFencingAndExactOCC(t *testing.T) {
	t.Parallel()
	address := os.Getenv("FLOWY_TEST_REDIS_ADDR")
	if address == "" {
		t.Skip("FLOWY_TEST_REDIS_ADDR not set")
	}
	// Arrange: independent clients use a unique isolated namespace on a real server.
	first := goredis.NewClient(&goredis.Options{Addr: address})
	second := goredis.NewClient(&goredis.Options{Addr: address})
	t.Cleanup(func() { _ = first.Close(); _ = second.Close() })
	ctx := context.Background()
	prefix := "flowy-task22:" + t.Name() + ":" + time.Now().UTC().Format("20060102150405.000000000")
	cp := mustCheckpointer[state, string](t, first, Options{Prefix: prefix}, checkpoint.JSONSerializer[state]{})
	a := mustRedisLeaseManager(t, first, redislease.Options{Prefix: prefix})
	b := mustRedisLeaseManager(t, second, redislease.Options{Prefix: prefix})
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

func TestRedisServerMillisecondTTLAndFenceAfterCleanup(t *testing.T) {
	// Arrange: real standalone Redis, unique namespace and separate lease owner.
	address := os.Getenv("FLOWY_TEST_REDIS_ADDR")
	if address == "" {
		t.Skip("FLOWY_TEST_REDIS_ADDR not set; real server TTL not verified")
	}
	ctx := context.Background()
	client := goredis.NewClient(&goredis.Options{Addr: address})
	defer client.Close()
	prefix := "flowy-task26:{" + t.Name() + "}:" + time.Now().UTC().Format("20060102150405.000000000")
	for _, ttl := range []time.Duration{0, 1, 500 * time.Millisecond, 1500 * time.Millisecond, time.Duration(1<<63 - 1)} {
		assertRedisServerTTL(ctx, t, client, prefix, ttl)
	}
	cp := mustCheckpointer[state, string](t, client, Options{Prefix: prefix}, checkpoint.JSONSerializer[state]{})
	manager := mustRedisLeaseManager(t, client, redislease.Options{Prefix: prefix})
	lease, err := manager.Acquire(ctx, "fenced", "owner", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	snapshot := testSnapshot(0, "fenced")
	snapshot.ThreadID = "fenced"
	if _, err = cp.Save(flowy.WithExecutionLease(ctx, lease), 0, snapshot); err != nil {
		t.Fatal(err)
	}
	if err = manager.Release(ctx, lease); err != nil {
		t.Fatal(err)
	}
	if err = cp.DeleteIfIdle(ctx, "fenced"); err != nil {
		t.Fatal(err)
	}
	next, err := manager.Acquire(ctx, "fenced", "owner", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	_, staleSave := cp.Save(flowy.WithExecutionLease(ctx, lease), 0, snapshot)
	if next.Incarnation <= lease.Incarnation || !errors.Is(staleSave, flowy.ErrLeaseLost) {
		t.Fatalf("fence reset=%d/%d stale=%v", lease.Incarnation, next.Incarnation, staleSave)
	}
	if err = manager.Release(ctx, next); err != nil {
		t.Fatal(err)
	}
}

func assertRedisServerTTL(ctx context.Context, t *testing.T, client *goredis.Client, prefix string, ttl time.Duration) {
	t.Helper()
	cp := mustCheckpointer[state, string](
		t,
		client,
		Options{Prefix: prefix, TTL: ttl},
		checkpoint.JSONSerializer[state]{},
	)
	id := "руна:{id}:" + ttl.String()
	snapshot := testSnapshot(0, "input")
	snapshot.ThreadID = id
	// Act: do not rely on a duration-returning client command for the maximum
	// rounded TTL; raw milliseconds fit Redis int64 even if Go duration rounds up.
	revision, err := cp.Save(ctx, 0, snapshot)
	if err != nil {
		t.Fatal(err)
	}
	millis, err := client.Do(ctx, "PTTL", cp.historyKey(id)).Int64()
	if err != nil {
		t.Fatal(err)
	}
	// Assert: zero is persistent, supported positive TTL is expiring or already expired.
	if ttl == 0 && millis != -1 {
		t.Fatalf("zero TTL=%d", millis)
	}
	if ttl > 0 && millis == -1 {
		t.Fatalf("positive TTL became persistent: %v", ttl)
	}
	rounded := ttl.Milliseconds()
	if ttl%time.Millisecond != 0 {
		rounded++
	}
	if ttl > 0 && millis > rounded {
		t.Fatalf("TTL exceeds ceiling: %d > %d", millis, rounded)
	}
	if ttl >= 500*time.Millisecond {
		if _, err = cp.Save(ctx, revision, snapshot); err != nil {
			t.Fatal(err)
		}
		persistent := mustCheckpointer[state, string](
			t,
			client,
			Options{Prefix: prefix},
			checkpoint.JSONSerializer[state]{},
		)
		if _, err = persistent.Save(ctx, revision+1, snapshot); err != nil {
			t.Fatal(err)
		}
		current, pttlErr := client.Do(ctx, "PTTL", cp.historyKey(id)).Int64()
		if pttlErr != nil || current != -1 {
			t.Fatalf("persistent refresh=%d/%v", current, pttlErr)
		}
	}
	if err = cp.Delete(ctx, id); err != nil {
		t.Fatal(err)
	}
}
