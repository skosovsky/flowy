package testutil

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/skosovsky/flowy"
)

type memoryState struct {
	Value int
}

//nolint:gocognit // concurrent OCC retry loop is the test subject
func TestMemoryCheckpointerConcurrentAccess(t *testing.T) {
	t.Parallel()
	cp := NewMemoryCheckpointer[memoryState, int]()
	const writes = 50

	var wg sync.WaitGroup
	for i := 1; i <= writes; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for {
				_, rev, err := cp.Load(context.Background(), "thread-1")
				if err != nil && !errors.Is(err, flowy.ErrThreadNotFound) {
					t.Errorf("load: %v", err)
					return
				}
				if errors.Is(err, flowy.ErrThreadNotFound) {
					rev = 0
				}
				_, err = cp.Save(context.Background(), rev, flowy.Snapshot[memoryState, int]{
					ThreadID:         "thread-1",
					ExecutionPointer: "node",
					State:            memoryState{Value: i},
					RunMeta:          flowy.RunMetadata{RetryCounts: map[string]int{"node": i}},
					Effects:          []int{i},
				})
				if errors.Is(err, flowy.ErrConcurrencyConflict) {
					continue
				}
				if err != nil {
					t.Errorf("save %d: %v", i, err)
				}
				return
			}
		}(i)
	}
	wg.Wait()

	latest, _, err := cp.Load(context.Background(), "thread-1")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if latest.Revision == 0 {
		t.Fatal("expected non-zero revision")
	}

	history, err := cp.GetHistory(context.Background(), "thread-1", writes)
	if err != nil {
		t.Fatalf("history: %v", err)
	}
	if len(history) != writes {
		t.Fatalf("expected %d history entries, got %d", writes, len(history))
	}
}

func TestMemoryCheckpointerPruneRetainsLatestN(t *testing.T) {
	t.Parallel()
	cp := NewMemoryCheckpointer[memoryState, flowy.NoEffect]()
	for i := 1; i <= 4; i++ {
		if _, err := cp.Save(context.Background(), uint64(i-1), flowy.Snapshot[memoryState, flowy.NoEffect]{
			ThreadID:         "thread-2",
			ExecutionPointer: "node",
			State:            memoryState{Value: i},
		}); err != nil {
			t.Fatalf("save %d: %v", i, err)
		}
	}

	if err := cp.Prune(context.Background(), "thread-2", 2); err != nil {
		t.Fatalf("prune: %v", err)
	}
	history, err := cp.GetHistory(context.Background(), "thread-2", 10)
	if err != nil {
		t.Fatalf("history: %v", err)
	}
	if len(history) != 2 || history[0].Revision != 4 || history[1].Revision != 3 {
		t.Fatalf("unexpected history after prune: %+v", history)
	}
}

func TestMemoryCheckpointerPruneNoopOnUnknownThread(t *testing.T) {
	t.Parallel()
	cp := NewMemoryCheckpointer[memoryState, flowy.NoEffect]()
	if err := cp.Prune(context.Background(), "missing", 2); err != nil {
		t.Fatalf("prune unknown: %v", err)
	}
}
