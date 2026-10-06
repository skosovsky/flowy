package redis

import (
	"context"
	"errors"
	"math"
	"testing"

	"github.com/alicebob/miniredis/v2"
	goredis "github.com/redis/go-redis/v9"

	"github.com/skosovsky/flowy"
	"github.com/skosovsky/flowy/checkpoint"
)

func TestSaveRejectsInvalidHeadWithoutMutation(t *testing.T) {
	t.Parallel()
	for _, raw := range []string{`{"revision":1}`, `{"revision":"01"}`, `{"revision":"0"}`, `{"revision":"18446744073709551616"}`, `not-json`} {
		t.Run(raw, func(t *testing.T) {
			t.Parallel()
			// Arrange: invalid or pre-break wire metadata is not an empty history.
			mr := miniredis.RunT(t)
			client := goredis.NewClient(&goredis.Options{Addr: mr.Addr()})
			t.Cleanup(func() { _ = client.Close() })
			ctx := context.Background()
			cp := mustCheckpointer[state, string](t, client, Options{}, checkpoint.JSONSerializer[state]{})
			if err := client.LPush(ctx, cp.historyKey("t1"), raw).Err(); err != nil {
				t.Fatal(err)
			}
			// Act.
			_, err := cp.Save(ctx, 0, testSnapshot(0, "overwrite"))
			// Assert.
			if !errors.Is(err, checkpoint.ErrInvalidRecord) {
				t.Fatalf("invalid head accepted: %v", err)
			}
			rows, err := client.LRange(ctx, cp.historyKey("t1"), 0, -1).Result()
			if err != nil || len(rows) != 1 || rows[0] != raw {
				t.Fatalf("head mutated: %v %v", rows, err)
			}
		})
	}
}

func TestSaveRevisionExhaustionDoesNotWrap(t *testing.T) {
	t.Parallel()
	// Arrange.
	mr := miniredis.RunT(t)
	client := goredis.NewClient(&goredis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	cp := mustCheckpointer[state, string](t, client, Options{}, checkpoint.JSONSerializer[state]{})
	// Act.
	_, err := cp.Save(context.Background(), math.MaxUint64, testSnapshot(0, "wrap"))
	// Assert.
	if !errors.Is(err, flowy.ErrExecutionCapability) {
		t.Fatalf("revision wrapped: %v", err)
	}
	if client.Exists(context.Background(), cp.historyKey("t1")).Val() != 0 {
		t.Fatal("exhausted revision mutated storage")
	}
}
