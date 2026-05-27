package redis

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	goredis "github.com/redis/go-redis/v9"

	"github.com/skosovsky/flowy"
	"github.com/skosovsky/flowy/checkpoint"
)

type state struct {
	Value string `json:"value"`
}

func testSnapshot(revision int, value string) flowy.Snapshot[state] {
	return flowy.Snapshot[state]{
		ThreadID: "t1",
		Revision: revision,
		NodeID:   "n1",
		State:    state{Value: value},
	}
}

func TestSaveLoadRoundtrip(t *testing.T) {
	t.Parallel()
	mr := miniredis.RunT(t)
	client := goredis.NewClient(&goredis.Options{Addr: mr.Addr()})
	defer func() { _ = client.Close() }()

	cp := NewCheckpointer[state](client, Options{}, checkpoint.JSONSerializer[state]{})
	err := cp.Save(context.Background(), flowy.Snapshot[state]{
		ThreadID: "t1",
		Revision: 1,
		NodeID:   "n1",
		State:    state{Value: "ok"},
		RunMeta: flowy.RunMetadata{
			SegmentStartTime: time.Now().UTC(),
			RetryCounts:      map[string]int{"n1": 1},
			StepCount:        3,
		},
		Effects: []any{"fx"},
	})
	if err != nil {
		t.Fatalf("save: %v", err)
	}

	got, err := cp.Load(context.Background(), "t1")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if got.State.Value != "ok" {
		t.Fatalf("unexpected state: %+v", got.State)
	}
	if got.Revision != 1 {
		t.Fatalf("unexpected revision: %d", got.Revision)
	}
}

func TestLoadNoSnapshot(t *testing.T) {
	t.Parallel()
	mr := miniredis.RunT(t)
	client := goredis.NewClient(&goredis.Options{Addr: mr.Addr()})
	defer func() { _ = client.Close() }()

	cp := NewCheckpointer[state](client, Options{}, checkpoint.JSONSerializer[state]{})
	_, err := cp.Load(context.Background(), "missing")
	if !errors.Is(err, checkpoint.ErrNoSnapshot) {
		t.Fatalf("expected ErrNoSnapshot, got %v", err)
	}
}

func TestGetHistory(t *testing.T) {
	t.Parallel()
	mr := miniredis.RunT(t)
	client := goredis.NewClient(&goredis.Options{Addr: mr.Addr()})
	defer func() { _ = client.Close() }()

	cp := NewCheckpointer[state](client, Options{}, checkpoint.JSONSerializer[state]{})
	_ = cp.Save(context.Background(), testSnapshot(1, "v1"))
	_ = cp.Save(context.Background(), testSnapshot(2, "v2"))
	history, err := cp.GetHistory(context.Background(), "t1", 10)
	if err != nil {
		t.Fatalf("history: %v", err)
	}
	if len(history) != 2 {
		t.Fatalf("expected 2 snapshots, got %d", len(history))
	}
	if history[0].Revision != 2 || history[1].Revision != 1 {
		t.Fatalf("unexpected history order: %+v", history)
	}
}

func TestPruneRetainsLatestN(t *testing.T) {
	t.Parallel()
	mr := miniredis.RunT(t)
	client := goredis.NewClient(&goredis.Options{Addr: mr.Addr()})
	defer func() { _ = client.Close() }()

	cp := NewCheckpointer[state](client, Options{}, checkpoint.JSONSerializer[state]{})
	_ = cp.Save(context.Background(), testSnapshot(1, "v1"))
	_ = cp.Save(context.Background(), testSnapshot(2, "v2"))
	_ = cp.Save(context.Background(), testSnapshot(3, "v3"))

	if err := cp.Prune(context.Background(), "t1", 2); err != nil {
		t.Fatalf("prune: %v", err)
	}
	history, err := cp.GetHistory(context.Background(), "t1", 10)
	if err != nil {
		t.Fatalf("history: %v", err)
	}
	if len(history) != 2 || history[0].Revision != 3 || history[1].Revision != 2 {
		t.Fatalf("unexpected history after prune: %+v", history)
	}
}

func TestPruneDeleteAllWhenRetainNonPositive(t *testing.T) {
	t.Parallel()
	mr := miniredis.RunT(t)
	client := goredis.NewClient(&goredis.Options{Addr: mr.Addr()})
	defer func() { _ = client.Close() }()

	cp := NewCheckpointer[state](client, Options{}, checkpoint.JSONSerializer[state]{})
	_ = cp.Save(context.Background(), testSnapshot(1, "v1"))
	if err := cp.Prune(context.Background(), "t1", 0); err != nil {
		t.Fatalf("prune: %v", err)
	}
	_, err := cp.Load(context.Background(), "t1")
	if !errors.Is(err, checkpoint.ErrNoSnapshot) {
		t.Fatalf("expected ErrNoSnapshot after prune-all, got %v", err)
	}
}
