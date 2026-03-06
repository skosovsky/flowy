// Package testutil provides test helpers for flowy (e.g. in-memory state+checkpoint store for v2).
package testutil

import (
	"context"
	"sync"

	"github.com/skosovsky/flowy"
)

// Store is a thread-safe in-memory store for (state, checkpoint) per key (e.g. thread ID).
// Caller persists state and checkpoint when a graph returns ErrSuspend and passes them to Resume.
type Store[T any] struct {
	mu   sync.RWMutex
	data map[string]struct {
		state T
		cp    *flowy.Checkpoint
	}
}

// NewStore creates a new in-memory store for state and checkpoint.
func NewStore[T any]() *Store[T] {
	return &Store[T]{
		data: make(map[string]struct {
			state T
			cp    *flowy.Checkpoint
		}),
	}
}

// Save stores state and checkpoint for the given key.
func (s *Store[T]) Save(ctx context.Context, key string, state T, cp *flowy.Checkpoint) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.data == nil {
		s.data = make(map[string]struct {
			state T
			cp    *flowy.Checkpoint
		})
	}
	var cpCopy *flowy.Checkpoint
	if cp != nil {
		cpCopy = &flowy.Checkpoint{NextNode: cp.NextNode}
	}
	s.data[key] = struct {
		state T
		cp    *flowy.Checkpoint
	}{state: state, cp: cpCopy}
	return nil
}

// Load returns state and checkpoint for the given key. ok is false if the key is missing.
func (s *Store[T]) Load(ctx context.Context, key string) (state T, cp *flowy.Checkpoint, ok bool) {
	if ctx.Err() != nil {
		var zero T
		return zero, nil, false
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	entry, ok := s.data[key]
	if !ok {
		var zero T
		return zero, nil, false
	}
	return entry.state, entry.cp, true
}
