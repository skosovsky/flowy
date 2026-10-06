package otel

import (
	"context"

	"github.com/skosovsky/flowy"
	"github.com/skosovsky/flowy/testutil"
)

// Corruption is injected at Load because normal storage now rejects empty pointers.
type invalidSnapshotCheckpointer[T, E any] struct {
	*testutil.MemoryCheckpointer[T, E]
}

func (c *invalidSnapshotCheckpointer[T, E]) Load(ctx context.Context, id string) (flowy.Snapshot[T, E], uint64, error) {
	snapshot, revision, err := c.MemoryCheckpointer.Load(ctx, id)
	snapshot.ExecutionPointer = ""
	return snapshot, revision, err
}
