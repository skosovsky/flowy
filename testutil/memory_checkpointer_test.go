package testutil

import (
	"context"
	"sync"
	"testing"

	"github.com/skosovsky/flowy"
)

type memoryState struct {
	Value int
}

func TestMemoryCheckpointerConcurrentAccess(t *testing.T) {
	t.Parallel()
	cp := NewMemoryCheckpointer[memoryState, int]()
	const writes = 50

	var wg sync.WaitGroup
	for i := 1; i <= writes; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_ = cp.Save(context.Background(), flowy.Snapshot[memoryState, int]{
				ThreadID:         "thread-1",
				Revision:         i,
				ExecutionPointer: "node",
				State:            memoryState{Value: i},
				RunMeta:          flowy.RunMetadata{RetryCounts: map[string]int{"node": i}},
				Effects:          []int{i},
			})
		}(i)
	}
	wg.Wait()

	latest, err := cp.Load(context.Background(), "thread-1")
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
		_ = cp.Save(context.Background(), flowy.Snapshot[memoryState, flowy.NoEffect]{
			ThreadID:         "thread-2",
			Revision:         i,
			ExecutionPointer: "node",
			State:            memoryState{Value: i},
		})
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
