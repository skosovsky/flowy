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

type state struct {
	Value string `json:"value"`
}

func testSnapshot(revision int, value string) flowy.Snapshot[state, string] {
	return flowy.Snapshot[state, string]{
		ThreadID:         "t1",
		Revision:         revision,
		ExecutionPointer: "n1",
		State:            state{Value: value},
	}
}

func TestSaveLoadRoundtrip(t *testing.T) {
	t.Parallel()
	mr := miniredis.RunT(t)
	client := goredis.NewClient(&goredis.Options{Addr: mr.Addr()})
	defer func() { _ = client.Close() }()

	cp := NewCheckpointer[state, string](client, Options{}, checkpoint.JSONSerializer[state]{})
	err := cp.Save(context.Background(), flowy.Snapshot[state, string]{
		ThreadID:         "t1",
		Revision:         1,
		ExecutionPointer: "n1",
		State:            state{Value: "ok"},
		RunMeta: flowy.RunMetadata{
			SegmentStartTime: time.Now().UTC(),
			RetryCounts:      map[string]int{"n1": 1},
			StepCount:        3,
		},
		Effects: []string{"fx"},
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

func TestExecutionPointerRoundtrip(t *testing.T) {
	t.Parallel()
	mr := miniredis.RunT(t)
	client := goredis.NewClient(&goredis.Options{Addr: mr.Addr()})
	defer func() { _ = client.Close() }()

	cp := NewCheckpointer[state, string](client, Options{}, checkpoint.JSONSerializer[state]{})
	if err := cp.Save(context.Background(), flowy.Snapshot[state, string]{
		ThreadID:         "router-th",
		Revision:         1,
		ExecutionPointer: "router",
		State:            state{Value: "ok"},
	}); err != nil {
		t.Fatalf("save: %v", err)
	}
	got, err := cp.Load(context.Background(), "router-th")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if got.ExecutionPointer != "router" {
		t.Fatalf("execution pointer roundtrip: want router, got %q", got.ExecutionPointer)
	}
}

func TestLoadNoSnapshot(t *testing.T) {
	t.Parallel()
	mr := miniredis.RunT(t)
	client := goredis.NewClient(&goredis.Options{Addr: mr.Addr()})
	defer func() { _ = client.Close() }()

	cp := NewCheckpointer[state, string](client, Options{}, checkpoint.JSONSerializer[state]{})
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

	cp := NewCheckpointer[state, string](client, Options{}, checkpoint.JSONSerializer[state]{})
	if err := cp.Save(context.Background(), testSnapshot(1, "v1")); err != nil {
		t.Fatalf("save v1: %v", err)
	}
	if err := cp.Save(context.Background(), testSnapshot(2, "v2")); err != nil {
		t.Fatalf("save v2: %v", err)
	}
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

	cp := NewCheckpointer[state, string](client, Options{}, checkpoint.JSONSerializer[state]{})
	if err := cp.Save(context.Background(), testSnapshot(1, "v1")); err != nil {
		t.Fatalf("save v1: %v", err)
	}
	if err := cp.Save(context.Background(), testSnapshot(2, "v2")); err != nil {
		t.Fatalf("save v2: %v", err)
	}
	if err := cp.Save(context.Background(), testSnapshot(3, "v3")); err != nil {
		t.Fatalf("save v3: %v", err)
	}

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

func TestDeleteIfIdleBlockedByLeaseKey(t *testing.T) {
	t.Parallel()
	mr := miniredis.RunT(t)
	client := goredis.NewClient(&goredis.Options{Addr: mr.Addr()})
	defer func() { _ = client.Close() }()

	cp := NewCheckpointer[state, string](client, Options{Prefix: "flowy"}, checkpoint.JSONSerializer[state]{})
	if err := cp.Save(context.Background(), testSnapshot(1, "v1")); err != nil {
		t.Fatalf("save: %v", err)
	}
	if err := client.Set(context.Background(), "flowy:lease:t1", "worker-a", 0).Err(); err != nil {
		t.Fatalf("set lease: %v", err)
	}
	err := cp.DeleteIfIdle(context.Background(), "t1")
	if !errors.Is(err, flowy.ErrThreadLeaseBusy) {
		t.Fatalf("expected ErrThreadLeaseBusy, got %v", err)
	}
	_, loadErr := cp.Load(context.Background(), "t1")
	if loadErr != nil {
		t.Fatalf("snapshot should remain: %v", loadErr)
	}
}

func TestDeleteIfIdleSucceedsWhenIdle(t *testing.T) {
	t.Parallel()
	mr := miniredis.RunT(t)
	client := goredis.NewClient(&goredis.Options{Addr: mr.Addr()})
	defer func() { _ = client.Close() }()

	cp := NewCheckpointer[state, string](client, Options{}, checkpoint.JSONSerializer[state]{})
	if err := cp.Save(context.Background(), testSnapshot(1, "v1")); err != nil {
		t.Fatalf("save: %v", err)
	}
	if err := cp.DeleteIfIdle(context.Background(), "t1"); err != nil {
		t.Fatalf("delete if idle: %v", err)
	}
	_, err := cp.Load(context.Background(), "t1")
	if !errors.Is(err, checkpoint.ErrNoSnapshot) {
		t.Fatalf("expected ErrNoSnapshot, got %v", err)
	}
}

func TestDeleteIfIdleLeasePrefixMismatch(t *testing.T) {
	t.Parallel()
	mr := miniredis.RunT(t)
	client := goredis.NewClient(&goredis.Options{Addr: mr.Addr()})
	defer func() { _ = client.Close() }()

	cp := NewCheckpointer[state, string](client, Options{
		Prefix:      "app",
		LeasePrefix: "leases",
	}, checkpoint.JSONSerializer[state]{})
	leaseMgr := redislease.NewLeaseManager(client, redislease.Options{Prefix: "app"})

	if err := cp.Save(context.Background(), testSnapshot(1, "v1")); err != nil {
		t.Fatalf("save: %v", err)
	}
	if err := leaseMgr.Acquire(context.Background(), "t1", "worker", time.Minute); err != nil {
		t.Fatalf("acquire: %v", err)
	}

	if err := cp.DeleteIfIdle(context.Background(), "t1"); err != nil {
		t.Fatalf("delete if idle: %v", err)
	}
	if _, err := cp.Load(context.Background(), "t1"); !errors.Is(err, checkpoint.ErrNoSnapshot) {
		t.Fatalf("prefix mismatch should bypass lease check and delete snapshot, got %v", err)
	}
}

func TestPruneDeleteAllWhenRetainNonPositive(t *testing.T) {
	t.Parallel()
	mr := miniredis.RunT(t)
	client := goredis.NewClient(&goredis.Options{Addr: mr.Addr()})
	defer func() { _ = client.Close() }()

	cp := NewCheckpointer[state, string](client, Options{}, checkpoint.JSONSerializer[state]{})
	if err := cp.Save(context.Background(), testSnapshot(1, "v1")); err != nil {
		t.Fatalf("save v1: %v", err)
	}
	if err := cp.Prune(context.Background(), "t1", 0); err != nil {
		t.Fatalf("prune: %v", err)
	}
	_, err := cp.Load(context.Background(), "t1")
	if !errors.Is(err, checkpoint.ErrNoSnapshot) {
		t.Fatalf("expected ErrNoSnapshot after prune-all, got %v", err)
	}
}
