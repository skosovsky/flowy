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
	Revision         uint64
	State            Sub
	RunMeta          RunMetadata
	Effects          []E
	// ExportedEffects counts effects already published at a parent boundary.
	ExportedEffects int
}

// SubgraphNode runs a subgraph with state mapped from parent to sub and back.
// For suspend/handoff resume at the inner node, use SubgraphNodeWithSlot.
// Nested subgraph runners do not inherit parent RunOptions (WithBindings, WithRunMetadata,
// WithRunLease, WithStateOverlay, WithHandoffOutbox,
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
//
//nolint:gocognit // explicit inline continuation and terminal boundary handling
func SubgraphNodeWithSlot[Parent, Sub, E any](
	sub *Graph[Sub, E],
	mapIn func(Parent) Sub,
	loadSlot func(Parent) (SubgraphSlot[Sub, E], bool),
	storeSlot func(Parent, SubgraphSlot[Sub, E]) Parent,
	mapOut func(Parent, Sub) Parent,
) Node[Parent, E] {
	return func(ctx context.Context, parentState Parent) (Parent, Directive, error) {
		if _, durable := ctx.Value(activityContextKey{}).(activityBackend); durable {
			return parentState, Fail("inline durable subgraph unsupported"), ErrExecutionCapability
		}
		ctx = context.WithValue(ctx, activityContextKey{}, struct{}{})
		ctx = context.WithValue(ctx, childGroupContextKey{}, struct{}{})
		ctx = context.WithValue(ctx, executionLeaseKey{}, struct{}{})
		exported := 0
		cp := newSubgraphCheckpointer[Sub, E](ctx)
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
