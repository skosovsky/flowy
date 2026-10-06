package flowy

import (
	"context"
	"fmt"
)

// InlineSlotContract identifies the persisted inline continuation format.
const InlineSlotContract = "flowy.inline-slot.v1"

// SubgraphSlot embeds a subgraph execution cursor in parent persisted state.
type SubgraphSlot[Sub, E any] struct {
	Contract         string
	ExecutionPointer ExecutionPointer
	// Revision is invocation-local capture progress, not a separate OCC authority.
	// Parent snapshot revision/fencing protects the slot. Each resume seeds a new
	// capture checkpointer with expected revision zero.
	Revision uint64
	State    Sub
	RunMeta  RunMetadata
	Effects  []E
	// ExportedEffects counts effects already published at a parent boundary.
	ExportedEffects int
}

// StatelessSubgraphNode starts the inner entry on each invocation with mapped parent state.
// Suspend/Handoff pause only the parent; a subsequent invocation starts the inner
// entry again. No inner cursor survives without the slot variant.
// For suspend/handoff resume at the inner node, use SubgraphNodeWithSlot.
// Nested subgraph runners do not inherit parent RunOptions (WithBindings, WithRunMetadata,
// WithRunLease, WithStateOverlay, WithHandoffOutbox,
// WithCheckpointErrorPolicy). Parent ctx values and BindingFromContext still apply in subgraph nodes.
func StatelessSubgraphNode[Parent, Sub, E any](
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
	return subgraphNodeWithCheckpointer(sub, mapIn, loadSlot, storeSlot, mapOut,
		func(context.Context) Checkpointer[Sub, E] { return newCaptureCheckpointer[Sub, E]() })
}

// The factory is an invocation-local seam; shipped constructors always use capture storage.
//
//nolint:gocognit // Explicit inline continuation and terminal boundary handling.
func subgraphNodeWithCheckpointer[Parent, Sub, E any](
	sub *Graph[Sub, E], mapIn func(Parent) Sub,
	loadSlot func(Parent) (SubgraphSlot[Sub, E], bool),
	storeSlot func(Parent, SubgraphSlot[Sub, E]) Parent,
	mapOut func(Parent, Sub) Parent,
	createCP func(context.Context) Checkpointer[Sub, E],
) Node[Parent, E] {
	return func(ctx context.Context, parentState Parent) (Parent, Directive, error) {
		if _, durable := ctx.Value(activityContextKey{}).(activityBackend); durable {
			return parentState, Fail("inline durable subgraph unsupported"), ErrExecutionCapability
		}
		ctx = context.WithValue(ctx, activityContextKey{}, struct{}{})
		ctx = context.WithValue(ctx, childGroupContextKey{}, struct{}{})
		ctx = context.WithValue(ctx, executionLeaseKey{}, struct{}{})
		exported := 0
		cp := createCP(ctx)
		threadID := subgraphThreadID(ctx)

		var result *RunResult[Sub, E]
		var err error
		if slot, ok := loadSlot(parentState); ok && slot.ExecutionPointer != "" {
			if slot.Contract != InlineSlotContract || slot.ExportedEffects < 0 ||
				slot.ExportedEffects > len(slot.Effects) {
				return parentState, Fail("invalid subgraph effect cursor"), ErrInvalidSnapshot
			}
			exported = slot.ExportedEffects
			// Slot seed uses direct Save (not persistSnapshot); parent RunOptions do not apply.
			// Ephemeral inner CP is always empty: first write expects revision 0.
			newRev, seedErr := cp.Save(ctx, 0, Snapshot[Sub, E]{
				ThreadID:         threadID,
				ExecutionPointer: slot.ExecutionPointer,
				Revision:         0,
				State:            slot.State,
				RunMeta:          slot.RunMeta,
				Effects:          append([]E(nil), slot.Effects...),
			})
			if seedErr != nil {
				return parentState, Fail("subgraph seed"), seedErr
			}
			result, err = sub.NewRunner(cp).Resume(ctx, ResumeToken{
				ThreadID:         threadID,
				SnapshotRevision: newRev,
			})
		} else {
			subState := mapIn(parentState)
			result, err = sub.NewRunner(cp).Start(ctx, threadID, subState)
		}
		if err != nil && (result == nil || result.Status != RunStatusContextCanceled) {
			return parentState, Fail("subgraph"), err
		}
		if result == nil || exported > len(result.Effects) {
			return parentState, Fail("subgraph result"), ErrInvalidSnapshot
		}
		newEffects := result.Effects[exported:]

		parentState = mapOut(parentState, result.State)
		switch result.Status {
		case RunStatusSuspended, RunStatusHandoff, RunStatusContextCanceled:
			snap, _, loadErr := cp.Load(ctx, threadID)
			if loadErr != nil {
				return parentState, Fail("subgraph slot"), loadErr
			}
			parentState = storeSlot(parentState, SubgraphSlot[Sub, E]{
				Contract:         InlineSlotContract,
				ExecutionPointer: snap.ExecutionPointer,
				Revision:         snap.Revision,
				State:            snap.State,
				RunMeta:          snap.RunMeta,
				Effects:          append([]E(nil), snap.Effects...),
				ExportedEffects:  len(snap.Effects),
			})
		case RunStatusCompleted:
			var empty SubgraphSlot[Sub, E]
			parentState = storeSlot(parentState, empty)
		case RunStatusFailed, RunStatusTransferred:
			// Retain the last confirmed slot for explicit recovery.
		}

		switch result.Status {
		case RunStatusSuspended:
			return parentState, WithEffects(Suspend(result.Reason), newEffects), nil
		case RunStatusContextCanceled:
			// The parent cancellation boundary persists the mapped inner continuation.
			return parentState, WithEffects(Completed(), newEffects), nil
		case RunStatusHandoff:
			return parentState, WithEffects(Handoff(result.Reason), newEffects), nil
		case RunStatusCompleted:
			return parentState, WithEffects(Completed(), newEffects), nil
		case RunStatusFailed:
			reason := result.Reason
			if reason == "" {
				reason = "subgraph failed"
			}
			return parentState, WithEffects(Fail(reason), newEffects), nil
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
