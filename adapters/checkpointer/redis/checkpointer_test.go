package redis

import (
	"context"
	"encoding/json"
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

func testSnapshot(revision uint64, value string) flowy.Snapshot[state, string] {
	return flowy.Snapshot[state, string]{
		ThreadID:         "t1",
		Revision:         revision,
		ExecutionPointer: "n1",
		State:            state{Value: value},
	}
}

func saveTestSnapshot(
	t *testing.T,
	cp *Checkpointer[state, string],
	expectedRevision uint64,
	revision uint64,
	value string,
) {
	t.Helper()
	if _, err := cp.Save(context.Background(), expectedRevision, testSnapshot(revision, value)); err != nil {
		t.Fatalf("save rev %d: %v", revision, err)
	}
}

func TestSaveOCCConflict(t *testing.T) {
	t.Parallel()
	mr := miniredis.RunT(t)
	client := goredis.NewClient(&goredis.Options{Addr: mr.Addr()})
	defer func() { _ = client.Close() }()

	cp := NewCheckpointer[state, string](client, Options{}, checkpoint.JSONSerializer[state]{})
	saveTestSnapshot(t, cp, 0, 1, "v1")
	_, err := cp.Save(context.Background(), 0, testSnapshot(2, "stale"))
	if !errors.Is(err, flowy.ErrConcurrencyConflict) {
		t.Fatalf("expected ErrConcurrencyConflict, got %v", err)
	}
}

func TestSaveLoadRoundtrip(t *testing.T) {
	t.Parallel()
	mr := miniredis.RunT(t)
	client := goredis.NewClient(&goredis.Options{Addr: mr.Addr()})
	defer func() { _ = client.Close() }()

	cp := NewCheckpointer[state, string](client, Options{}, checkpoint.JSONSerializer[state]{})
	_, err := cp.Save(context.Background(), 0, flowy.Snapshot[state, string]{
		ThreadID:         "t1",
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

	got, _, err := cp.Load(context.Background(), "t1")
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
	if _, err := cp.Save(context.Background(), 0, flowy.Snapshot[state, string]{
		ThreadID:         "router-th",
		ExecutionPointer: "router",
		State:            state{Value: "ok"},
	}); err != nil {
		t.Fatalf("save: %v", err)
	}
	got, _, err := cp.Load(context.Background(), "router-th")
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
	_, _, err := cp.Load(context.Background(), "missing")
	if !errors.Is(err, flowy.ErrThreadNotFound) || !errors.Is(err, checkpoint.ErrNoSnapshot) {
		t.Fatalf("expected ErrThreadNotFound wrapping ErrNoSnapshot, got %v", err)
	}
}

func TestLoadRejectsRecordThreadMismatch(t *testing.T) {
	t.Parallel()
	mr := miniredis.RunT(t)
	client := goredis.NewClient(&goredis.Options{Addr: mr.Addr()})
	defer func() { _ = client.Close() }()

	cp := NewCheckpointer[state, string](client, Options{}, checkpoint.JSONSerializer[state]{})
	record, err := checkpoint.EncodeRecord(flowy.Snapshot[state, string]{
		ThreadID:         "thread-b",
		Revision:         1,
		ExecutionPointer: "n1",
		State:            state{Value: "wrong"},
	}, checkpoint.JSONSerializer[state]{})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	payload, err := json.Marshal(record)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	seedErr := client.LPush(context.Background(), cp.historyKey("thread-a"), string(payload)).Err()
	if seedErr != nil {
		t.Fatalf("seed raw record: %v", seedErr)
	}

	_, _, err = cp.Load(context.Background(), "thread-a")
	if !errors.Is(err, checkpoint.ErrInvalidRecord) ||
		!errors.Is(err, flowy.ErrSnapshotEnvelopeInvalid) {
		t.Fatalf("expected invalid record envelope, got %v", err)
	}
}

func TestGetHistoryRejectsRecordThreadMismatch(t *testing.T) {
	t.Parallel()
	mr := miniredis.RunT(t)
	client := goredis.NewClient(&goredis.Options{Addr: mr.Addr()})
	defer func() { _ = client.Close() }()

	cp := NewCheckpointer[state, string](client, Options{}, checkpoint.JSONSerializer[state]{})
	record, err := checkpoint.EncodeRecord(flowy.Snapshot[state, string]{
		ThreadID:         "thread-b",
		Revision:         1,
		ExecutionPointer: "n1",
		State:            state{Value: "wrong"},
	}, checkpoint.JSONSerializer[state]{})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	payload, err := json.Marshal(record)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	seedErr := client.LPush(context.Background(), cp.historyKey("thread-a"), string(payload)).Err()
	if seedErr != nil {
		t.Fatalf("seed raw record: %v", seedErr)
	}

	_, err = cp.GetHistory(context.Background(), "thread-a", 10)
	if !errors.Is(err, checkpoint.ErrInvalidRecord) ||
		!errors.Is(err, flowy.ErrSnapshotEnvelopeInvalid) {
		t.Fatalf("expected invalid record envelope, got %v", err)
	}
}

func TestSaveAfterLargeRevision(t *testing.T) {
	t.Parallel()
	mr := miniredis.RunT(t)
	client := goredis.NewClient(&goredis.Options{Addr: mr.Addr()})
	defer func() { _ = client.Close() }()

	cp := NewCheckpointer[state, string](client, Options{}, checkpoint.JSONSerializer[state]{})
	const largeRevision = uint64(3_000_000_000)
	record, err := checkpoint.EncodeRecord(flowy.Snapshot[state, string]{
		ThreadID:         "t1",
		Revision:         largeRevision,
		ExecutionPointer: "n1",
		State:            state{Value: "large"},
	}, checkpoint.JSONSerializer[state]{})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	payload, err := json.Marshal(record)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	seedErr := client.LPush(context.Background(), cp.historyKey("t1"), string(payload)).Err()
	if seedErr != nil {
		t.Fatalf("seed raw record: %v", seedErr)
	}

	newRev, err := cp.Save(context.Background(), largeRevision, flowy.Snapshot[state, string]{
		ThreadID:         "t1",
		ExecutionPointer: "n2",
		State:            state{Value: "next"},
	})
	if err != nil {
		t.Fatalf("save after large revision: %v", err)
	}
	if newRev != largeRevision+1 {
		t.Fatalf("new revision: got %d want %d", newRev, largeRevision+1)
	}
	loaded, _, err := cp.Load(context.Background(), "t1")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if loaded.Revision != largeRevision+1 {
		t.Fatalf("loaded revision: got %d want %d", loaded.Revision, largeRevision+1)
	}
}

func TestGetHistory(t *testing.T) {
	t.Parallel()
	mr := miniredis.RunT(t)
	client := goredis.NewClient(&goredis.Options{Addr: mr.Addr()})
	defer func() { _ = client.Close() }()

	cp := NewCheckpointer[state, string](client, Options{}, checkpoint.JSONSerializer[state]{})
	saveTestSnapshot(t, cp, 0, 1, "v1")
	saveTestSnapshot(t, cp, 1, 2, "v2")
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
	saveTestSnapshot(t, cp, 0, 1, "v1")
	saveTestSnapshot(t, cp, 1, 2, "v2")
	saveTestSnapshot(t, cp, 2, 3, "v3")

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
	saveTestSnapshot(t, cp, 0, 1, "v1")
	if err := client.Set(context.Background(), "flowy:lease:t1", "worker-a", 0).Err(); err != nil {
		t.Fatalf("set lease: %v", err)
	}
	err := cp.DeleteIfIdle(context.Background(), "t1")
	if !errors.Is(err, flowy.ErrThreadLeaseBusy) {
		t.Fatalf("expected ErrThreadLeaseBusy, got %v", err)
	}
	_, _, loadErr := cp.Load(context.Background(), "t1")
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
	saveTestSnapshot(t, cp, 0, 1, "v1")
	if err := cp.DeleteIfIdle(context.Background(), "t1"); err != nil {
		t.Fatalf("delete if idle: %v", err)
	}
	_, _, err := cp.Load(context.Background(), "t1")
	if !errors.Is(err, flowy.ErrThreadNotFound) || !errors.Is(err, checkpoint.ErrNoSnapshot) {
		t.Fatalf("expected ErrThreadNotFound wrapping ErrNoSnapshot, got %v", err)
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

	saveTestSnapshot(t, cp, 0, 1, "v1")
	if err := leaseMgr.Acquire(context.Background(), "t1", "worker", time.Minute); err != nil {
		t.Fatalf("acquire: %v", err)
	}

	if err := cp.DeleteIfIdle(context.Background(), "t1"); err != nil {
		t.Fatalf("delete if idle: %v", err)
	}
	if _, _, err := cp.Load(context.Background(), "t1"); !errors.Is(err, flowy.ErrThreadNotFound) ||
		!errors.Is(err, checkpoint.ErrNoSnapshot) {
		t.Fatalf("prefix mismatch should bypass lease check and delete snapshot, got %v", err)
	}
}

func TestPruneDeleteAllWhenRetainNonPositive(t *testing.T) {
	t.Parallel()
	mr := miniredis.RunT(t)
	client := goredis.NewClient(&goredis.Options{Addr: mr.Addr()})
	defer func() { _ = client.Close() }()

	cp := NewCheckpointer[state, string](client, Options{}, checkpoint.JSONSerializer[state]{})
	saveTestSnapshot(t, cp, 0, 1, "v1")
	if err := cp.Prune(context.Background(), "t1", 0); err != nil {
		t.Fatalf("prune: %v", err)
	}
	_, _, err := cp.Load(context.Background(), "t1")
	if !errors.Is(err, flowy.ErrThreadNotFound) || !errors.Is(err, checkpoint.ErrNoSnapshot) {
		t.Fatalf("expected ErrThreadNotFound wrapping ErrNoSnapshot after prune-all, got %v", err)
	}
}
