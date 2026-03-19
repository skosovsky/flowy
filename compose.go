package flowy

import "context"

// SubgraphNode returns a Node[Parent] that runs the subgraph with state mapped from Parent to Sub and back.
// Use it to embed a graph with a different (e.g. nested) state type into a parent graph.
// mapIn extracts the sub-state from the parent state; mapOut merges the final sub-state into the parent.
// If the subgraph returns ErrSuspend, it is propagated to the parent graph.
func SubgraphNode[Parent, Sub any](sub *Graph[Sub], mapIn func(Parent) Sub, mapOut func(Parent, Sub) Parent) Node[Parent] {
	return func(ctx context.Context, parentState Parent) (Parent, error) {
		subState := mapIn(parentState)
		finalSub, err := sub.Invoke(ctx, subState)
		if err != nil {
			return parentState, err
		}
		return mapOut(parentState, finalSub), nil
	}
}
