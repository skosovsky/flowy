package flowy

// Checkpoint holds the node name to resume from (v2).
// When a graph returns ErrSuspend, Invoke returns (state, &Checkpoint{NextNode: nodeName}, ErrSuspend).
// The caller persists state and this checkpoint; Resume(ctx, state, cp) continues from cp.NextNode.
type Checkpoint struct {
	NextNode string // Name of the node to run next when resuming
}
