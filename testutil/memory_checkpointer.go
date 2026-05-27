// Package testutil provides test helpers for flowy.
package testutil

import (
	"context"
	"maps"
	"sync"

	"github.com/skosovsky/flowy"
)

// MemoryCheckpointer is a thread-safe in-memory implementation of flowy.Checkpointer.
type MemoryCheckpointer[T any] struct {
	mu      sync.RWMutex
	history map[string][]flowy.Snapshot[T]
}

func NewMemoryCheckpointer[T any]() *MemoryCheckpointer[T] {
	return &MemoryCheckpointer[T]{
		history: make(map[string][]flowy.Snapshot[T]),
	}
}

func (m *MemoryCheckpointer[T]) Save(_ context.Context, snapshot flowy.Snapshot[T]) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	copied := copySnapshot(snapshot)
	m.history[snapshot.ThreadID] = append(m.history[snapshot.ThreadID], copied)
	return nil
}

func (m *MemoryCheckpointer[T]) Load(_ context.Context, threadID string) (flowy.Snapshot[T], error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	items := m.history[threadID]
	if len(items) == 0 {
		return flowy.Snapshot[T]{}, flowy.ErrThreadNotFound
	}
	return copySnapshot(items[len(items)-1]), nil
}

func (m *MemoryCheckpointer[T]) GetHistory(
	_ context.Context,
	threadID string,
	limit int,
) ([]flowy.Snapshot[T], error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	items := m.history[threadID]
	if len(items) == 0 {
		return []flowy.Snapshot[T]{}, nil
	}
	if limit <= 0 || limit > len(items) {
		limit = len(items)
	}
	out := make([]flowy.Snapshot[T], 0, limit)
	for i := len(items) - 1; i >= len(items)-limit; i-- {
		out = append(out, copySnapshot(items[i]))
	}
	return out, nil
}

func (m *MemoryCheckpointer[T]) Prune(_ context.Context, threadID string, retainCount int) error {
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
	m.history[threadID] = append([]flowy.Snapshot[T](nil), items[len(items)-retainCount:]...)
	return nil
}

func copySnapshot[T any](snapshot flowy.Snapshot[T]) flowy.Snapshot[T] {
	cloned := snapshot
	cloned.Effects = append([]any(nil), snapshot.Effects...)
	if snapshot.RunMeta.RetryCounts != nil {
		cloned.RunMeta.RetryCounts = make(map[string]int, len(snapshot.RunMeta.RetryCounts))
		maps.Copy(cloned.RunMeta.RetryCounts, snapshot.RunMeta.RetryCounts)
	}
	return cloned
}
