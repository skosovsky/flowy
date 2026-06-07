// Package testutil provides test helpers for flowy.
package testutil

import (
	"context"
	"maps"
	"sync"

	"github.com/skosovsky/flowy"
)

// MemoryCheckpointer is a thread-safe in-memory implementation of flowy.Checkpointer with OCC.
type MemoryCheckpointer[T, E any] struct {
	mu      sync.Mutex
	history map[string][]flowy.Snapshot[T, E]
}

func NewMemoryCheckpointer[T, E any]() *MemoryCheckpointer[T, E] {
	return &MemoryCheckpointer[T, E]{
		history: make(map[string][]flowy.Snapshot[T, E]),
	}
}

func (m *MemoryCheckpointer[T, E]) Save(
	_ context.Context,
	expectedRevision uint64,
	snapshot flowy.Snapshot[T, E],
) (uint64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	items := m.history[snapshot.ThreadID]
	var current uint64
	if len(items) > 0 {
		current = items[len(items)-1].Revision
	}
	if current != expectedRevision {
		return 0, flowy.ErrConcurrencyConflict
	}
	newRevision := expectedRevision + 1
	copied := copySnapshot(snapshot)
	copied.Revision = newRevision
	m.history[snapshot.ThreadID] = append(items, copied)
	return newRevision, nil
}

func (m *MemoryCheckpointer[T, E]) Load(_ context.Context, threadID string) (flowy.Snapshot[T, E], uint64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	items := m.history[threadID]
	if len(items) == 0 {
		return flowy.Snapshot[T, E]{}, 0, flowy.ErrThreadNotFound
	}
	latest := copySnapshot(items[len(items)-1])
	return latest, latest.Revision, nil
}

func (m *MemoryCheckpointer[T, E]) GetHistory(
	_ context.Context,
	threadID string,
	limit int,
) ([]flowy.Snapshot[T, E], error) {
	m.mu.Lock()
	defer m.mu.Unlock()
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

// DeleteIfIdle delegates to Delete (no lease store). Use NewLeaseGuardCheckpointer for in-process lease checks.
func (m *MemoryCheckpointer[T, E]) DeleteIfIdle(ctx context.Context, threadID string) error {
	return m.Delete(ctx, threadID)
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
