package flowy

import (
	"context"
	"fmt"
)

// SubgraphSlot embeds a subgraph execution cursor in parent persisted state.
type SubgraphSlot[Sub, E any] struct {
	ExecutionPointer ExecutionPointer
	Revision         int
	State            Sub
	RunMeta          RunMetadata
	Effects          []E
}

// SubgraphNode runs a subgraph with state mapped from parent to sub and back.
// For suspend/handoff resume at the inner node, use SubgraphNodeWithSlot.
// Nested subgraph runners do not inherit parent RunOptions (WithBindings, WithRunMetadata,
// WithRunLease, WithStateOverlay, WithSuspendPointerResolver, WithHandoffScheduler,
// WithCheckpointErrorPolicy). Parent ctx values and BindingFromContext still apply in subgraph nodes.
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
		cp := newSubgraphCheckpointer[Sub, E](ctx)
		threadID := subgraphThreadID(ctx)

		var result *RunResult[Sub, E]
		var err error
		if slot, ok := loadSlot(parentState); ok && slot.ExecutionPointer != "" {
			// Slot seed uses direct Save (not persistSnapshot); parent RunOptions do not apply.
			if seedErr := cp.Save(ctx, Snapshot[Sub, E]{
				ThreadID:         threadID,
				ExecutionPointer: slot.ExecutionPointer,
				Revision:         slot.Revision,
				State:            slot.State,
				RunMeta:          slot.RunMeta,
				Effects:          append([]E(nil), slot.Effects...),
			}); seedErr != nil {
				return parentState, Fail("subgraph seed"), seedErr
			}
			result, err = sub.NewRunner(cp).Resume(ctx, ResumeToken{
				ThreadID:         threadID,
				SnapshotRevision: slot.Revision,
			})
		} else {
			subState := mapIn(parentState)
			result, err = sub.NewRunner(cp).Start(ctx, threadID, subState)
		}
		if err != nil {
			return parentState, Fail("subgraph"), err
		}

		parentState = mapOut(parentState, result.State)
		if result.Status == RunStatusSuspended || result.Status == RunStatusHandoff {
			snap, loadErr := cp.Load(ctx, threadID)
			if loadErr != nil {
				return parentState, Fail("subgraph slot"), loadErr
			}
			parentState = storeSlot(parentState, SubgraphSlot[Sub, E]{
				ExecutionPointer: snap.ExecutionPointer,
				Revision:         snap.Revision,
				State:            snap.State,
				RunMeta:          snap.RunMeta,
				Effects:          append([]E(nil), snap.Effects...),
			})
		}

		switch result.Status {
		case RunStatusSuspended:
			return parentState, Suspend(result.Reason), nil
		case RunStatusContextCanceled:
			// Inner context cancel maps to Completed on the parent subgraph node;
			// the error return propagates context.Canceled to the parent runner.
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

type subgraphTestMode int

const (
	subgraphTestModeNone subgraphTestMode = iota
	subgraphTestModeFailSeedSave
	subgraphTestModeFailSlotLoad
	subgraphTestModeStaleInnerRevision
)

type subgraphTestModeKey struct{}

// withSubgraphTestMode configures ephemeral subgraph checkpointer behavior for tests.
func withSubgraphTestMode(ctx context.Context, mode subgraphTestMode) context.Context {
	return context.WithValue(ctx, subgraphTestModeKey{}, mode)
}

func newSubgraphCheckpointer[Sub, E any](ctx context.Context) Checkpointer[Sub, E] {
	mode, _ := ctx.Value(subgraphTestModeKey{}).(subgraphTestMode)
	switch mode {
	case subgraphTestModeFailSeedSave:
		base := newCaptureCheckpointer[Sub, E]()
		return &failingCaptureCheckpointer[Sub, E]{
			captureCheckpointer: *base,
			failSave:            true,
			failLoad:            false,
		}
	case subgraphTestModeFailSlotLoad:
		base := newCaptureCheckpointer[Sub, E]()
		return &failingCaptureCheckpointer[Sub, E]{
			captureCheckpointer: *base,
			failSave:            false,
			failLoad:            true,
		}
	case subgraphTestModeStaleInnerRevision:
		base := newCaptureCheckpointer[Sub, E]()
		return &bumpRevisionOnLoadCP[Sub, E]{captureCheckpointer: *base}
	default:
		return newCaptureCheckpointer[Sub, E]()
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
