package flowy

import (
	"context"
	"fmt"
)

// SubgraphSlot embeds a subgraph execution cursor in parent persisted state.
type SubgraphSlot[Sub, E any] struct {
	NodeID   string
	Revision int
	State    Sub
	RunMeta  RunMetadata
	Effects  []E
}

// SubgraphNode runs a subgraph with state mapped from parent to sub and back.
// For suspend/handoff resume at the inner node, use SubgraphNodeWithSlot.
func SubgraphNode[Parent, Sub, E any](
	sub *Graph[Sub, E],
	mapIn func(Parent) Sub,
	mapOut func(Parent, Sub) Parent,
) Node[Parent, E] {
	return SubgraphNodeWithSlot(
		sub,
		mapIn,
		func(_ Parent) (SubgraphSlot[Sub, E], bool) { return SubgraphSlot[Sub, E]{}, false },
		func(parent Parent, _ SubgraphSlot[Sub, E]) Parent { return parent },
		mapOut,
	)
}

// SubgraphNodeWithSlot persists subgraph cursor in parent state for suspend/handoff continuity.
func SubgraphNodeWithSlot[Parent, Sub, E any](
	sub *Graph[Sub, E],
	mapIn func(Parent) Sub,
	loadSlot func(Parent) (SubgraphSlot[Sub, E], bool),
	storeSlot func(Parent, SubgraphSlot[Sub, E]) Parent,
	mapOut func(Parent, Sub) Parent,
) Node[Parent, E] {
	return func(ctx context.Context, parentState Parent) (Parent, Directive, error) {
		cp := newCaptureCheckpointer[Sub, E]()
		threadID := subgraphThreadID(ctx)

		var result *RunResult[Sub, E]
		var err error
		if slot, ok := loadSlot(parentState); ok && slot.NodeID != "" {
			_ = cp.Save(ctx, Snapshot[Sub, E]{
				ThreadID: threadID,
				NodeID:   slot.NodeID,
				Revision: slot.Revision,
				State:    slot.State,
				RunMeta:  slot.RunMeta,
				Effects:  append([]E(nil), slot.Effects...),
			})
			result, err = sub.NewRunner(cp).Resume(ctx, threadID)
		} else {
			subState := mapIn(parentState)
			result, err = sub.NewRunner(cp).Start(ctx, threadID, subState)
		}
		if err != nil {
			return parentState, Completed(), err
		}

		parentState = mapOut(parentState, result.State)
		if result.Status == RunStatusSuspended || result.Status == RunStatusHandoff {
			if snap, loadErr := cp.Load(ctx, threadID); loadErr == nil {
				parentState = storeSlot(parentState, SubgraphSlot[Sub, E]{
					NodeID:   snap.NodeID,
					Revision: snap.Revision,
					State:    snap.State,
					RunMeta:  snap.RunMeta,
					Effects:  append([]E(nil), snap.Effects...),
				})
			}
		}

		switch result.Status {
		case RunStatusSuspended:
			return parentState, Suspend(result.Reason), nil
		case RunStatusContextCanceled:
			return parentState, Completed(), context.Canceled
		case RunStatusHandoff:
			return parentState, Handoff(result.Reason), nil
		case RunStatusCompleted:
			return parentState, Completed(), nil
		case RunStatusFailed:
			reason := result.Reason
			if reason == "" {
				reason = "subgraph failed"
			}
			return parentState, Fail(reason), nil
		default:
			return parentState, Fail("subgraph failed"), nil
		}
	}
}

func subgraphThreadID(ctx context.Context) string {
	parentThread := RunThreadIDFromContext(ctx)
	nodeName := NodeNameFromContext(ctx)
	if parentThread == "" {
		parentThread = "__subgraph_parent__"
	}
	if nodeName == "" {
		nodeName = "subgraph"
	}
	return fmt.Sprintf("%s::%s", parentThread, nodeName)
}
