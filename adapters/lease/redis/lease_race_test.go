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

// The barrier sits immediately before the storage mutation. A split GET then
// SET/DEL would validate A before the takeover; a single Eval validates B.
type takeoverClient struct {
	goredis.Cmdable

	takeover func()
	read     bool
}

func (c *takeoverClient) Get(ctx context.Context, key string) *goredis.StringCmd {
	result := c.Cmdable.Get(ctx, key)
	c.read = true
	c.takeover()
	return result
}

func (c *takeoverClient) Eval(ctx context.Context, script string, keys []string, args ...any) *goredis.Cmd {
	c.takeover()
	return c.Cmdable.Eval(ctx, script, keys, args...)
}

//nolint:gocognit // explicit Arrange/Act/Assert preserves both takeover regressions
func TestStaleLeaseMutationPreservesSuccessor(t *testing.T) {
	for _, operation := range []string{"renew", "release"} {
		t.Run(operation, func(t *testing.T) {
			// Arrange.
			server := miniredis.RunT(t)
			client := goredis.NewClient(&goredis.Options{Addr: server.Addr()})
			t.Cleanup(func() { _ = client.Close() })
			ctx := context.Background()
			next := NewLeaseManager(client, Options{})
			old, acquireErr := next.Acquire(ctx, "thread", "A", time.Second)
			if acquireErr != nil {
				t.Fatal(acquireErr)
			}
			barrier := &takeoverClient{Cmdable: client}
			barrier.takeover = func() {
				server.FastForward(2 * time.Second)
				if _, err := next.Acquire(ctx, "thread", "B", time.Minute); err != nil {
					t.Fatal(err)
				}
				barrier.takeover = func() {}
			}
			stale := NewLeaseManager(barrier, Options{})
			// Act.
			var err error
			if operation == "renew" {
				_, err = stale.Renew(ctx, old, time.Hour)
			} else {
				err = stale.Release(ctx, old)
			}
			// Assert.
			if !errors.Is(err, flowy.ErrLeaseLost) {
				t.Fatalf("expected ownership rejection, got %v", err)
			}
			owner, held, err := next.Holder(ctx, "thread")
			if err != nil || !held || owner != "B" {
				t.Fatalf("successor changed: %q %v %v", owner, held, err)
			}
			if server.TTL(next.leaseKey("thread")) != time.Minute {
				t.Fatal("successor TTL changed")
			}
			if barrier.read {
				t.Fatal("mutation used a separate ownership read")
			}
			if _, err := next.Acquire(ctx, "thread", "C", time.Minute); !errors.Is(err, flowy.ErrLeaseHeld) {
				t.Fatalf("third owner acquired: %v", err)
			}
		})
	}
}
