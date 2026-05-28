// Package testutil provides test helpers for flowy.
package testutil

import (
	"context"
	"maps"
	"sync"

	"github.com/skosovsky/flowy"
)

// MemoryCheckpointer is a thread-safe in-memory implementation of flowy.Checkpointer.
type MemoryCheckpointer[T, E any] struct {
	mu      sync.RWMutex
	history map[string][]flowy.Snapshot[T, E]
}

func NewMemoryCheckpointer[T, E any]() *MemoryCheckpointer[T, E] {
	return &MemoryCheckpointer[T, E]{
		history: make(map[string][]flowy.Snapshot[T, E]),
	}
}

func (m *MemoryCheckpointer[T, E]) Save(_ context.Context, snapshot flowy.Snapshot[T, E]) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	copied := copySnapshot(snapshot)
	m.history[snapshot.ThreadID] = append(m.history[snapshot.ThreadID], copied)
	return nil
}

func (m *MemoryCheckpointer[T, E]) Load(_ context.Context, threadID string) (flowy.Snapshot[T, E], error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	items := m.history[threadID]
	if len(items) == 0 {
		return flowy.Snapshot[T, E]{}, flowy.ErrThreadNotFound
	}
	return copySnapshot(items[len(items)-1]), nil
}

func (m *MemoryCheckpointer[T, E]) GetHistory(
	_ context.Context,
	threadID string,
	limit int,
) ([]flowy.Snapshot[T, E], error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	items := m.history[threadID]
	if len(items) == 0 {
		return []flowy.Snapshot[T, E]{}, nil
	}
	if limit <= 0 || limit > len(items) {
		limit = len(items)
	}
	out := make([]flowy.Snapshot[T, E], 0, limit)
	for i := len(items) - 1; i >= len(items)-limit; i-- {
		out = append(out, copySnapshot(items[i]))
	}
	return out, nil
}

func (m *MemoryCheckpointer[T, E]) Prune(_ context.Context, threadID string, retainCount int) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	items := m.history[threadID]
	if len(items) == 0 {
		return nil
	}
	if retainCount <= 0 {
		delete(m.history, threadID)
		return nil
	}
	if len(items) <= retainCount {
		return nil
	}
	m.history[threadID] = append([]flowy.Snapshot[T, E](nil), items[len(items)-retainCount:]...)
	return nil
}

func (m *MemoryCheckpointer[T, E]) Delete(_ context.Context, threadID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.history, threadID)
	return nil
}

func copySnapshot[T, E any](snapshot flowy.Snapshot[T, E]) flowy.Snapshot[T, E] {
	cloned := snapshot
	cloned.Effects = append([]E(nil), snapshot.Effects...)
	if snapshot.RunMeta.RetryCounts != nil {
		cloned.RunMeta.RetryCounts = make(map[string]int, len(snapshot.RunMeta.RetryCounts))
		maps.Copy(cloned.RunMeta.RetryCounts, snapshot.RunMeta.RetryCounts)
	}
	if snapshot.RunMeta.BudgetCounts != nil {
		cloned.RunMeta.BudgetCounts = make(map[string]int, len(snapshot.RunMeta.BudgetCounts))
		maps.Copy(cloned.RunMeta.BudgetCounts, snapshot.RunMeta.BudgetCounts)
	}
	return cloned
}
