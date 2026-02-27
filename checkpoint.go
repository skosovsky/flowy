package flowy

import "context"

// Checkpoint holds the state and the node name to resume from.
type Checkpoint[T any] struct {
	State    T
	NodeName string
}

// Checkpointer persists and loads checkpoints by threadID (e.g. for Redis, Postgres).
// Implementations must be safe for concurrent use.
type Checkpointer[T any] interface {
	Save(ctx context.Context, threadID string, cp Checkpoint[T]) error
	Load(ctx context.Context, threadID string) (Checkpoint[T], error)
}
