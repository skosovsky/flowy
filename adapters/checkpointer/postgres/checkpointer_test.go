package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/skosovsky/flowy"
	"github.com/skosovsky/flowy/checkpoint"
)

type fakeDB struct {
	execCalled bool
	row        pgx.Row
	rows       pgx.Rows
}

func (f *fakeDB) Exec(_ context.Context, _ string, _ ...any) (pgconn.CommandTag, error) {
	f.execCalled = true
	return pgconn.CommandTag{}, nil
}

func (f *fakeDB) QueryRow(_ context.Context, _ string, _ ...any) pgx.Row {
	return f.row
}

func (f *fakeDB) Query(_ context.Context, _ string, _ ...any) (pgx.Rows, error) {
	return f.rows, nil
}

type fakeRow struct {
	values []any
	err    error
}

func (r fakeRow) Scan(dest ...any) error {
	if r.err != nil {
		return r.err
	}
	for i := range dest {
		switch d := dest[i].(type) {
		case *string:
			*d = r.values[i].(string)
		case *int:
			*d = r.values[i].(int)
		case *[]byte:
			*d = r.values[i].([]byte)
		case *json.RawMessage:
			*d = append((*d)[:0], r.values[i].([]byte)...)
		case *time.Time:
			*d = r.values[i].(time.Time)
		}
	}
	return nil
}

type fakeRows struct {
	index int
	items []fakeRow
}

func (r *fakeRows) Close()                                       {}
func (r *fakeRows) Err() error                                   { return nil }
func (r *fakeRows) CommandTag() pgconn.CommandTag                { return pgconn.CommandTag{} }
func (r *fakeRows) FieldDescriptions() []pgconn.FieldDescription { return nil }
func (r *fakeRows) Next() bool {
	if r.index >= len(r.items) {
		return false
	}
	r.index++
	return true
}
func (r *fakeRows) Scan(dest ...any) error {
	return r.items[r.index-1].Scan(dest...)
}
func (r *fakeRows) Values() ([]any, error) { return nil, nil }
func (r *fakeRows) RawValues() [][]byte    { return nil }
func (r *fakeRows) Conn() *pgx.Conn        { return nil }

type sampleState struct {
	Value string `json:"value"`
}

func mustRunMetaBytes(t *testing.T, now time.Time, retryCounts map[string]int, stepCount int) []byte {
	t.Helper()
	raw, err := json.Marshal(flowy.RunMetadata{
		SegmentStartTime: now,
		RetryCounts:      retryCounts,
		StepCount:        stepCount,
	})
	if err != nil {
		t.Fatalf("marshal run meta: %v", err)
	}
	return raw
}

func TestCheckpointerSaveAndLoad(t *testing.T) {
	t.Parallel()
	now := time.Now().UTC()
	row := fakeRow{
		values: []any{
			"thread-1",
			2,
			"node-1",
			[]byte(`{"value":"ok"}`),
			mustRunMetaBytes(t, now, map[string]int{"n": 1}, 2),
			[]byte(`["fx"]`),
			now,
		},
	}
	db := &fakeDB{row: row}
	cp := NewCheckpointer[sampleState](db, checkpoint.JSONSerializer[sampleState]{})

	err := cp.Save(context.Background(), flowy.Snapshot[sampleState]{
		ThreadID: "thread-1",
		Revision: 2,
		NodeID:   "node-1",
		State:    sampleState{Value: "ok"},
		RunMeta:  flowy.RunMetadata{SegmentStartTime: now, RetryCounts: map[string]int{"n": 1}, StepCount: 2},
		Effects:  []any{"fx"},
	})
	if err != nil {
		t.Fatalf("save: %v", err)
	}
	if !db.execCalled {
		t.Fatal("expected save exec call")
	}

	snapshot, err := cp.Load(context.Background(), "thread-1")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if snapshot.State.Value != "ok" {
		t.Fatalf("unexpected state: %+v", snapshot.State)
	}
	if snapshot.Revision != 2 {
		t.Fatalf("expected revision=2, got %d", snapshot.Revision)
	}
}

func TestLoadNoSnapshot(t *testing.T) {
	t.Parallel()
	db := &fakeDB{row: fakeRow{err: pgx.ErrNoRows}}
	cp := NewCheckpointer[sampleState](db, checkpoint.JSONSerializer[sampleState]{})
	_, err := cp.Load(context.Background(), "missing")
	if !errors.Is(err, checkpoint.ErrNoSnapshot) {
		t.Fatalf("expected ErrNoSnapshot, got %v", err)
	}
}

func TestGetHistory(t *testing.T) {
	t.Parallel()
	now := time.Now().UTC()
	rows := &fakeRows{
		items: []fakeRow{
			{
				values: []any{
					"thread-1",
					3,
					"node-1",
					[]byte(`{"value":"v3"}`),
					mustRunMetaBytes(t, now, map[string]int{}, 3),
					[]byte(`[]`),
					now,
				},
			},
			{
				values: []any{
					"thread-1",
					2,
					"node-1",
					[]byte(`{"value":"v2"}`),
					mustRunMetaBytes(t, now, map[string]int{}, 2),
					[]byte(`[]`),
					now,
				},
			},
		},
	}
	db := &fakeDB{rows: rows}
	cp := NewCheckpointer[sampleState](db, checkpoint.JSONSerializer[sampleState]{})
	history, err := cp.GetHistory(context.Background(), "thread-1", 10)
	if err != nil {
		t.Fatalf("history: %v", err)
	}
	if len(history) != 2 {
		t.Fatalf("expected 2 history items, got %d", len(history))
	}
	if history[0].Revision != 3 || history[1].Revision != 2 {
		t.Fatalf("unexpected revisions: %+v", history)
	}
}

func TestPruneRetainsLatestN(t *testing.T) {
	t.Parallel()
	db := &fakeDB{}
	cp := NewCheckpointer[sampleState](db, checkpoint.JSONSerializer[sampleState]{})
	if err := cp.Prune(context.Background(), "thread-1", 3); err != nil {
		t.Fatalf("prune: %v", err)
	}
	if !db.execCalled {
		t.Fatal("expected prune exec call")
	}
}

func TestPruneNoopOnMissingThread(t *testing.T) {
	t.Parallel()
	db := &fakeDB{}
	cp := NewCheckpointer[sampleState](db, checkpoint.JSONSerializer[sampleState]{})
	if err := cp.Prune(context.Background(), "missing-thread", 5); err != nil {
		t.Fatalf("prune missing: %v", err)
	}
}
