package redis

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	goredis "github.com/redis/go-redis/v9"

	"github.com/skosovsky/flowy"
	"github.com/skosovsky/flowy/checkpoint"
)

func TestSnapshotAdmissionPreservesHeadHistoryAndTTL(t *testing.T) {
	t.Parallel()
	for name, mutate := range map[string]func(*flowy.Snapshot[state, string]){
		"empty thread":    func(s *flowy.Snapshot[state, string]) { s.ThreadID = "" },
		"empty pointer":   func(s *flowy.Snapshot[state, string]) { s.ExecutionPointer = "" },
		"invalid thread":  func(s *flowy.Snapshot[state, string]) { s.ThreadID = "t1\xff" },
		"invalid pointer": func(s *flowy.Snapshot[state, string]) { s.ExecutionPointer = "node\xff" },
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			// Arrange: storage time stays fixed, so any TTL refresh is visible.
			server := miniredis.RunT(t)
			client := goredis.NewClient(&goredis.Options{Addr: server.Addr()})
			t.Cleanup(func() { _ = client.Close() })
			cp := mustCheckpointer[state, string](
				t,
				client,
				Options{TTL: time.Minute},
				checkpoint.JSONSerializer[state]{},
			)
			snapshot := testSnapshot(0, "healthy")
			if _, err := cp.Save(context.Background(), 0, snapshot); err != nil {
				t.Fatal(err)
			}
			server.FastForward(time.Second)
			key := cp.historyKey(snapshot.ThreadID)
			before, err := server.List(key)
			if err != nil {
				t.Fatal(err)
			}
			ttl, keys := server.TTL(key), server.Keys()
			mutate(&snapshot)
			// Act.
			_, saveErr := cp.Save(context.Background(), 1, snapshot)
			// Assert.
			after, listErr := server.List(key)
			if !errors.Is(saveErr, flowy.ErrSnapshotEnvelopeInvalid) || listErr != nil ||
				!slices.Equal(before, after) ||
				server.TTL(key) != ttl ||
				!slices.Equal(keys, server.Keys()) {
				t.Fatalf(
					"save=%v list=%v ttl=%v want=%v keys=%v",
					saveErr,
					listErr,
					server.TTL(key),
					ttl,
					server.Keys(),
				)
			}
			loaded, revision, loadErr := cp.Load(context.Background(), "t1")
			if loadErr != nil || revision != 1 || loaded.State.Value != "healthy" {
				t.Fatalf("loaded=%+v revision=%d err=%v", loaded, revision, loadErr)
			}
		})
	}
}
