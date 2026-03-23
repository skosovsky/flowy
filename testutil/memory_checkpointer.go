package testutil

import (
	"context"
	"slices"
	"sync"

	"github.com/skosovsky/flowy/checkpoint"
)

// MemoryCheckpointer is a thread-safe in-memory checkpoint.Checkpointer for tests and examples.
// LoadLatest and GetHistory return checkpoints ordered by CreatedAt newest-first,
// breaking ties by Checkpoint.ID descending.
type MemoryCheckpointer struct {
	mu        sync.RWMutex
	byThread  map[string][]checkpoint.Checkpoint
	savedByID map[string]struct{}
}

// NewMemoryCheckpointer creates an in-memory append-only checkpointer.
func NewMemoryCheckpointer() *MemoryCheckpointer {
	return &MemoryCheckpointer{
		byThread:  make(map[string][]checkpoint.Checkpoint),
		savedByID: make(map[string]struct{}),
	}
}

// Save appends cp unless the same checkpoint ID has already been stored.
func (m *MemoryCheckpointer) Save(ctx context.Context, cp checkpoint.Checkpoint) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if m.byThread == nil {
		m.byThread = make(map[string][]checkpoint.Checkpoint)
	}
	if m.savedByID == nil {
		m.savedByID = make(map[string]struct{})
	}
	if _, exists := m.savedByID[cp.ID]; exists {
		return nil
	}

	m.byThread[cp.ThreadID] = append(m.byThread[cp.ThreadID], cloneCheckpoint(cp))
	m.savedByID[cp.ID] = struct{}{}
	return nil
}

// LoadLatest returns the newest checkpoint for threadID by CreatedAt.
func (m *MemoryCheckpointer) LoadLatest(ctx context.Context, threadID string) (checkpoint.Checkpoint, error) {
	if err := ctx.Err(); err != nil {
		return checkpoint.Checkpoint{}, err
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

	history := m.byThread[threadID]
	if len(history) == 0 {
		return checkpoint.Checkpoint{}, checkpoint.ErrNoCheckpoint
	}

	return sortedClonedHistory(history)[0], nil
}

// GetHistory returns newest-first history for threadID by CreatedAt.
func (m *MemoryCheckpointer) GetHistory(
	ctx context.Context,
	threadID string,
	limit int,
) ([]checkpoint.Checkpoint, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

	history := m.byThread[threadID]
	if len(history) == 0 {
		return nil, checkpoint.ErrNoCheckpoint
	}

	count := len(history)
	if limit > 0 && limit < count {
		count = limit
	}

	sorted := sortedClonedHistory(history)
	return sorted[:count], nil
}

func cloneCheckpoint(cp checkpoint.Checkpoint) checkpoint.Checkpoint {
	cp.StateData = slices.Clone(cp.StateData)
	return cp
}

func sortedClonedHistory(history []checkpoint.Checkpoint) []checkpoint.Checkpoint {
	out := make([]checkpoint.Checkpoint, 0, len(history))
	for _, cp := range history {
		out = append(out, cloneCheckpoint(cp))
	}

	slices.SortFunc(out, func(a, b checkpoint.Checkpoint) int {
		switch {
		case a.CreatedAt.After(b.CreatedAt):
			return -1
		case a.CreatedAt.Before(b.CreatedAt):
			return 1
		case a.ID > b.ID:
			return -1
		case a.ID < b.ID:
			return 1
		default:
			return 0
		}
	})
	return out
}
