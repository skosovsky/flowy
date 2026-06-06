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

func TestLeaseManagerAcquireRelease(t *testing.T) {
	t.Parallel()
	mr := miniredis.RunT(t)
	client := goredis.NewClient(&goredis.Options{Addr: mr.Addr()})
	defer func() { _ = client.Close() }()

	lm := NewLeaseManager(client, Options{Prefix: "flowy"})
	ctx := context.Background()
	if err := lm.Acquire(ctx, "th-1", "worker-a", time.Minute); err != nil {
		t.Fatalf("acquire: %v", err)
	}
	held, err := lm.IsHeld(ctx, "th-1")
	if err != nil || !held {
		t.Fatalf("expected held, held=%v err=%v", held, err)
	}
	if acquireErr := lm.Acquire(ctx, "th-1", "worker-b", time.Minute); !errors.Is(acquireErr, flowy.ErrLeaseHeld) {
		t.Fatalf("expected ErrLeaseHeld, got %v", acquireErr)
	}
	if releaseErr := lm.Release(ctx, "th-1", "worker-a"); releaseErr != nil {
		t.Fatalf("release: %v", releaseErr)
	}
	held, err = lm.IsHeld(ctx, "th-1")
	if err != nil || held {
		t.Fatalf("expected not held after release, held=%v err=%v", held, err)
	}
}
