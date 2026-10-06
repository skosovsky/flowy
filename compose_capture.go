package flowy

import "context"

// AsStatelessNode composes a graph as a node.
// The inline runner uses an ephemeral checkpointer and does not receive parent RunOptions
// (WithHandoffOutbox, WithCheckpointErrorPolicy).
// Suspend/Handoff inside AsStatelessNode are not resumable: inner ResumeToken is not propagated.
// For suspend/handoff continuity use SubgraphNodeWithSlot.
func (g *Graph[T, E]) AsStatelessNode() Node[T, E] {
	return StatelessSubgraphNode(g, func(state T) T { return state }, func(_ T, updated T) T { return updated })
}

type captureCheckpointer[T, E any] struct {
	history []Snapshot[T, E]
}

func newCaptureCheckpointer[T, E any]() *captureCheckpointer[T, E] {
	return &captureCheckpointer[T, E]{}
}

func (n *captureCheckpointer[T, E]) Save(
	_ context.Context,
	expectedRevision uint64,
	s Snapshot[T, E],
) (uint64, error) {
	var current uint64
	if len(n.history) > 0 {
		current = n.history[len(n.history)-1].Revision
	}
	if current != expectedRevision {
		return 0, ErrConcurrencyConflict
	}
	newRev := expectedRevision + 1
	s.Revision = newRev
	n.history = append(n.history, s)
	return newRev, nil
}

func (n *captureCheckpointer[T, E]) Load(_ context.Context, _ string) (Snapshot[T, E], uint64, error) {
	if len(n.history) == 0 {
		var zero Snapshot[T, E]
		return zero, 0, ErrThreadNotFound
	}
	s := n.history[len(n.history)-1]
	return s, s.Revision, nil
}

func (n *captureCheckpointer[T, E]) GetHistory(
	_ context.Context,
	_ string,
	limit int,
) ([]Snapshot[T, E], error) {
	if len(n.history) == 0 {
		return []Snapshot[T, E]{}, nil
	}
	if limit <= 0 || limit > len(n.history) {
		limit = len(n.history)
	}
	out := make([]Snapshot[T, E], 0, limit)
	for i := len(n.history) - 1; i >= len(n.history)-limit; i-- {
		out = append(out, n.history[i])
	}
	return out, nil
}

func (n *captureCheckpointer[T, E]) Prune(_ context.Context, _ string, retainCount int) error {
	if retainCount <= 0 {
		n.history = nil
		return nil
	}
	if len(n.history) <= retainCount {
		return nil
	}
	n.history = append([]Snapshot[T, E](nil), n.history[len(n.history)-retainCount:]...)
	return nil
}

func (n *captureCheckpointer[T, E]) Delete(_ context.Context, _ string) error {
	n.history = nil
	return nil
}

// DeleteIfIdle clears ephemeral history (same as Delete for inline/subgraph runners).
func (n *captureCheckpointer[T, E]) DeleteIfIdle(_ context.Context, _ string) error {
	n.history = nil
	return nil
}
