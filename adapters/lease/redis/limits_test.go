package redis

import (
	"context"
	"errors"
	"math"
	"strconv"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	goredis "github.com/redis/go-redis/v9"

	"github.com/skosovsky/flowy"
)

func TestLeaseFractionalTTLAndCounterExhaustion(t *testing.T) {
	t.Parallel()
	// Arrange: server clock is controlled independently of Go scheduling.
	server := miniredis.RunT(t)
	client := goredis.NewClient(&goredis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	manager := mustLeaseManager(t, client, Options{})
	const ttl = time.Millisecond + time.Nanosecond
	// Act/Assert: acquire and renew must both round up.
	lease, err := manager.Acquire(context.Background(), "fraction", "worker", ttl)
	if err != nil || server.TTL(manager.leaseKey("fraction")) != 2*time.Millisecond {
		t.Fatalf("lease=%+v err=%v ttl=%v", lease, err, server.TTL(manager.leaseKey("fraction")))
	}
	server.FastForward(time.Millisecond)
	if _, renewErr := manager.Renew(
		context.Background(),
		lease,
		ttl,
	); renewErr != nil ||
		server.TTL(manager.leaseKey("fraction")) != 2*time.Millisecond {
		t.Fatalf("renew=%v ttl=%v", renewErr, server.TTL(manager.leaseKey("fraction")))
	}
	// Arrange: exhausting a retained signed counter must not mutate or create a lease.
	maximum := strconv.FormatInt(math.MaxInt64, 10)
	server.Set(manager.fenceKey("exhausted"), maximum)
	// Act.
	_, exhaustedErr := manager.Acquire(context.Background(), "exhausted", "worker", time.Minute)
	// Assert.
	counter, getErr := server.Get(manager.fenceKey("exhausted"))
	if !errors.Is(exhaustedErr, flowy.ErrExecutionCapability) || getErr != nil || counter != maximum ||
		server.Exists(manager.leaseKey("exhausted")) {
		t.Fatalf("exhausted=%v counter=%q get=%v", exhaustedErr, counter, getErr)
	}
}
