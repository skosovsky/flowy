// Package testutil provides test helpers for flowy (e.g. InMemoryCheckpointer).
package testutil

import (
	"context"
	"sync"

	"github.com/skosovsky/flowy"
)

// InMemoryCheckpointer is a thread-safe in-memory implementation of flowy.Checkpointer for tests.
type InMemoryCheckpointer[T any] struct {
	mu   sync.RWMutex
	data map[string]flowy.Checkpoint[T]
}

// NewInMemoryCheckpointer creates a new in-memory checkpointer.
func NewInMemoryCheckpointer[T any]() *InMemoryCheckpointer[T] {
	return &InMemoryCheckpointer[T]{
		data: make(map[string]flowy.Checkpoint[T]),
	}
}

// Save stores a checkpoint for the given threadID.
func (c *InMemoryCheckpointer[T]) Save(ctx context.Context, threadID string, cp flowy.Checkpoint[T]) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.data == nil {
		c.data = make(map[string]flowy.Checkpoint[T])
	}
	c.data[threadID] = cp
	return nil
}

// Load returns the checkpoint for the given threadID, or flowy.ErrThreadNotFound if missing.
func (c *InMemoryCheckpointer[T]) Load(ctx context.Context, threadID string) (flowy.Checkpoint[T], error) {
	if ctx.Err() != nil {
		var zero flowy.Checkpoint[T]
		return zero, ctx.Err()
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	cp, ok := c.data[threadID]
	if !ok {
		var zero flowy.Checkpoint[T]
		return zero, flowy.ErrThreadNotFound
	}
	return cp, nil
}
