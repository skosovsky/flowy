//go:build integration

package redis

import (
	"context"
	"errors"
	"math"
	"os"
	"strconv"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/skosovsky/flowy"
)

func TestLiveLeaseCounterExhaustionAndFencing(t *testing.T) {
	// Arrange: a disposable Redis instance and keys scoped to this test.
	address := os.Getenv("FLOWY_TEST_REDIS_ADDR")
	if address == "" {
		t.Skip("FLOWY_TEST_REDIS_ADDR is required")
	}
	client := goredis.NewClient(&goredis.Options{Addr: address})
	t.Cleanup(func() { _ = client.Close() })
	manager := mustLeaseManager(t, client, Options{})
	ctx := context.Background()
	id := t.Name()
	maximum := strconv.FormatInt(math.MaxInt64, 10)
	if err := client.Set(ctx, manager.fenceKey(id), maximum, 0).Err(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = client.Del(ctx, manager.fenceKey(id), manager.leaseKey(id),
			manager.fenceKey(id+"normal"), manager.leaseKey(id+"normal")).Err()
	})
	// Act.
	_, exhaustedErr := manager.Acquire(ctx, id, "worker", time.Minute)
	counter, getErr := client.Get(ctx, manager.fenceKey(id)).Result()
	exists, existsErr := client.Exists(ctx, manager.leaseKey(id)).Result()
	// Assert: Lua overflow is a typed rejection, not a reset or a successful lease.
	if !errors.Is(exhaustedErr, flowy.ErrExecutionCapability) || getErr != nil || existsErr != nil ||
		counter != maximum || exists != 0 {
		t.Fatalf("acquire=%v get=%v exists=%v counter=%q lease=%d", exhaustedErr, getErr, existsErr, counter, exists)
	}
	first, err := manager.Acquire(ctx, id+"normal", "worker", time.Minute+time.Nanosecond)
	if err != nil {
		t.Fatal(err)
	}
	if releaseErr := manager.Release(ctx, first); releaseErr != nil {
		t.Fatal(releaseErr)
	}
	second, err := manager.Acquire(ctx, id+"normal", "worker", time.Minute+time.Nanosecond)
	_, staleErr := manager.Renew(ctx, first, time.Minute)
	if err != nil || second.Incarnation <= first.Incarnation || !errors.Is(staleErr, flowy.ErrLeaseLost) {
		t.Fatalf("successor=%+v err=%v stale=%v", second, err, staleErr)
	}
}
