package flowy

import "context"

// SubgraphNode returns a Node[Parent] that runs the subgraph with state mapped from Parent to Sub and back.
// Use it to embed a graph with a different (e.g. nested) state type into a parent graph.
// mapIn extracts the sub-state from the parent state; mapOut merges the final sub-state into the parent.
// If the subgraph suspends, the suspend reason is propagated to the parent graph.
func SubgraphNode[Parent, Sub any](
	sub *Graph[Sub],
	mapIn func(Parent) Sub,
	mapOut func(Parent, Sub) Parent,
) Node[Parent] {
	return func(ctx context.Context, parentState Parent) (Parent, Directive, error) {
		subState := mapIn(parentState)
		result, err := sub.NewRunner(newCaptureCheckpointer[Sub]()).Start(ctx, "__subgraph__", subState)
		if err != nil {
			return parentState, Completed(), err
		}
		mapped := mapOut(parentState, result.State)
		switch result.Status {
		case RunStatusSuspended:
			return mapped, Suspend(result.Reason), nil
		case RunStatusCompleted:
			return mapped, Completed(), nil
		default:
			return mapped, Completed(), nil
		}
	}
}
