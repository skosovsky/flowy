// Package testutil provides test helpers for flowy (e.g. in-memory state+resume-node store for v2).
package testutil

import (
	"context"
	"sync"
)

// Store is a thread-safe in-memory store for (state, startNode) per key (e.g. thread ID).
// Caller persists state and resume node when a graph returns ErrSuspend and passes them to Resume.
type Store[T any] struct {
	mu   sync.RWMutex
	data map[string]struct {
		state     T
		startNode string
	}
}

// NewStore creates a new in-memory store for state and resume node.
func NewStore[T any]() *Store[T] {
	return &Store[T]{
		data: make(map[string]struct {
			state     T
			startNode string
		}),
	}
}

// Save stores state and resume node for the given key.
func (s *Store[T]) Save(ctx context.Context, key string, state T, startNode string) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.data == nil {
		s.data = make(map[string]struct {
			state     T
			startNode string
		})
	}
	s.data[key] = struct {
		state     T
		startNode string
	}{state: state, startNode: startNode}
	return nil
}

// Load returns state and resume node for the given key. ok is false if the key is missing.
func (s *Store[T]) Load(ctx context.Context, key string) (state T, startNode string, ok bool) {
	if ctx.Err() != nil {
		var zero T
		return zero, "", false
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	entry, ok := s.data[key]
	if !ok {
		var zero T
		return zero, "", false
	}
	return entry.state, entry.startNode, true
}
