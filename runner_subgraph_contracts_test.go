package flowy

import (
	"context"
	"errors"
	"strings"
	"testing"
)

func TestAsNodeSuspendNotResumable(t *testing.T) {
	t.Parallel()

	type innerState struct{ Step int }
	type outerState struct{ Step int }

	innerBuilder := NewGraph[innerState, NoEffect](func(_ innerState, u innerState) innerState { return u })
	innerBuilder.AddNode("work", func(_ context.Context, s innerState) (innerState, Directive, error) {
		s.Step++
		if s.Step < 2 {
			return s, Suspend("inner-wait"), nil
		}
		return s, End(), nil
	})
	innerBuilder.SetEntryPoint("work")
	innerBuilder.AllowNoOutgoingRoute("work")
	inner, err := innerBuilder.Compile()
	if err != nil {
		t.Fatalf("compile inner: %v", err)
	}

	outerBuilder := NewGraph[outerState, NoEffect](func(_ outerState, u outerState) outerState { return u })
	outerBuilder.AddNode("inline", func(ctx context.Context, s outerState) (outerState, Directive, error) {
		_, d, runErr := inner.AsNode()(ctx, innerState{})
		return s, d, runErr
	})
	outerBuilder.SetEntryPoint("inline")
	outerBuilder.AllowNoOutgoingRoute("inline")
	outer, err := outerBuilder.Compile()
	if err != nil {
		t.Fatalf("compile outer: %v", err)
	}

	cp := newMemoryCP[outerState, NoEffect]()
	runner := outer.NewRunner(cp)

	first, err := runner.Start(context.Background(), "asnode-th", outerState{})
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	if first.Status != RunStatusSuspended {
		t.Fatalf("expected suspended, got %s", first.Status)
	}
	if first.ResumeToken.ThreadID == "" {
		t.Fatal("expected parent ResumeToken on suspend")
	}

	second, err := runner.Resume(context.Background(), first.ResumeToken)
	if err != nil {
		t.Fatalf("parent resume: %v", err)
	}
	if second.Status != RunStatusSuspended {
		t.Fatalf("AsNode inner suspend is not resumable; inline graph restarts, got %s", second.Status)
	}
}
func TestSubgraphDoesNotInheritHandoffOutbox(t *testing.T) {
	t.Parallel()

	type childState struct{}
	type parentState struct {
		Slot SubgraphSlot[childState, NoEffect]
	}

	outbox := &stubHandoffOutbox{}
	subBuilder := NewGraph[childState, NoEffect](func(_ childState, u childState) childState { return u })
	subBuilder.AddNode("work", func(_ context.Context, s childState) (childState, Directive, error) {
		return s, Handoff("inner-bg"), nil
	})
	subBuilder.AllowNoOutgoingRoute("work")
	subBuilder.SetEntryPoint("work")
	sub, err := subBuilder.Compile()
	if err != nil {
		t.Fatalf("compile subgraph: %v", err)
	}

	parentBuilder := NewGraph[parentState, NoEffect](func(_ parentState, u parentState) parentState { return u })
	loadSlot := func(p parentState) (SubgraphSlot[childState, NoEffect], bool) {
		return p.Slot, p.Slot.ExecutionPointer != ""
	}
	parentBuilder.AddNode("sub", SubgraphNodeWithSlot(
		sub,
		func(_ parentState) childState { return childState{} },
		loadSlot,
		func(p parentState, slot SubgraphSlot[childState, NoEffect]) parentState {
			p.Slot = slot
			return p
		},
		func(p parentState, _ childState) parentState { return p },
	))
	parentBuilder.SetEntryPoint("sub")
	parentBuilder.AllowNoOutgoingRoute("sub")
	parentGraph, err := parentBuilder.Compile()
	if err != nil {
		t.Fatalf("compile parent: %v", err)
	}

	cp := newMemoryCP[parentState, NoEffect]()
	res, err := parentGraph.NewRunner(cp).
		Start(context.Background(), "sub-outbox-th", parentState{},
			WithHandoffOutbox[parentState, NoEffect](outbox),
		)
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	if res.Status != RunStatusHandoff {
		t.Fatalf("expected parent handoff from inner handoff, got %s", res.Status)
	}
	if len(outbox.calls) != 1 {
		t.Fatalf("outbox must run once at parent handoff, not on inner runner, calls=%d", len(outbox.calls))
	}
	if outbox.calls[0].ThreadID != "sub-outbox-th" {
		t.Fatalf("expected parent thread in outbox token, got %+v", outbox.calls[0])
	}
	snap, _, loadErr := cp.Load(context.Background(), "sub-outbox-th")
	if loadErr != nil {
		t.Fatalf("load parent snapshot: %v", loadErr)
	}
	if snap.RunMeta.HandoffStatus != HandoffStatusEnqueued {
		t.Fatalf("expected enqueued handoff status on parent snap, got %q", snap.RunMeta.HandoffStatus)
	}
	assertHandoffTokenRevisionContract(t, outbox, res, cp, "sub-outbox-th")
}

func TestSubgraphHandoffEnqueueFailureOrphansParentSnapshot(t *testing.T) {
	t.Parallel()

	type childState struct{}
	type parentState struct {
		Slot SubgraphSlot[childState, NoEffect]
	}

	outbox := &stubHandoffOutbox{err: errors.New("broker down")}
	subBuilder := NewGraph[childState, NoEffect](func(_ childState, u childState) childState { return u })
	subBuilder.AddNode("work", func(_ context.Context, s childState) (childState, Directive, error) {
		return s, Handoff("inner-bg"), nil
	})
	subBuilder.AllowNoOutgoingRoute("work")
	subBuilder.SetEntryPoint("work")
	sub, err := subBuilder.Compile()
	if err != nil {
		t.Fatalf("compile subgraph: %v", err)
	}

	parentBuilder := NewGraph[parentState, NoEffect](func(_ parentState, u parentState) parentState { return u })
	loadSlot := func(p parentState) (SubgraphSlot[childState, NoEffect], bool) {
		return p.Slot, p.Slot.ExecutionPointer != ""
	}
	parentBuilder.AddNode("sub", SubgraphNodeWithSlot(
		sub,
		func(_ parentState) childState { return childState{} },
		loadSlot,
		func(p parentState, slot SubgraphSlot[childState, NoEffect]) parentState {
			p.Slot = slot
			return p
		},
		func(p parentState, _ childState) parentState { return p },
	))
	parentBuilder.SetEntryPoint("sub")
	parentBuilder.AllowNoOutgoingRoute("sub")
	parentGraph, err := parentBuilder.Compile()
	if err != nil {
		t.Fatalf("compile parent: %v", err)
	}

	cp := newMemoryCP[parentState, NoEffect]()
	res, err := parentGraph.NewRunner(cp).
		Start(context.Background(), "sub-enqueue-fail-th", parentState{},
			WithHandoffOutbox[parentState, NoEffect](outbox),
		)
	if !errors.Is(err, ErrHandoffEnqueueFailed) {
		t.Fatalf("expected ErrHandoffEnqueueFailed, got %v", err)
	}
	if res == nil || res.Status != RunStatusHandoff {
		t.Fatalf("expected RunStatusHandoff, got %+v", res)
	}
	assertOrphanedHandoffSnapshot(t, cp, "sub-enqueue-fail-th", res, "inner-bg")
	assertRunMetaHandoffStatusMatchesSnapshot(t, res, cp, "sub-enqueue-fail-th")
	assertHandoffReasonMatchesStatus(t, res, cp, "sub-enqueue-fail-th", "inner-bg")
}

func TestSubgraphHandoffEnqueueFailPatchOrphanFails(t *testing.T) {
	t.Parallel()

	type childState struct{}
	type parentState struct {
		Slot SubgraphSlot[childState, NoEffect]
	}

	outbox := &stubHandoffOutbox{err: errors.New("broker down")}
	cp := &handoffPatchFailCP[parentState, NoEffect]{failOnStatus: HandoffStatusOrphaned}
	subBuilder := NewGraph[childState, NoEffect](func(_ childState, u childState) childState { return u })
	subBuilder.AddNode("work", func(_ context.Context, s childState) (childState, Directive, error) {
		return s, Handoff("inner-bg"), nil
	})
	subBuilder.AllowNoOutgoingRoute("work")
	subBuilder.SetEntryPoint("work")
	sub, err := subBuilder.Compile()
	if err != nil {
		t.Fatalf("compile subgraph: %v", err)
	}

	parentBuilder := NewGraph[parentState, NoEffect](func(_ parentState, u parentState) parentState { return u })
	loadSlot := func(p parentState) (SubgraphSlot[childState, NoEffect], bool) {
		return p.Slot, p.Slot.ExecutionPointer != ""
	}
	parentBuilder.AddNode("sub", SubgraphNodeWithSlot(
		sub,
		func(_ parentState) childState { return childState{} },
		loadSlot,
		func(p parentState, slot SubgraphSlot[childState, NoEffect]) parentState {
			p.Slot = slot
			return p
		},
		func(p parentState, _ childState) parentState { return p },
	))
	parentBuilder.SetEntryPoint("sub")
	parentBuilder.AllowNoOutgoingRoute("sub")
	parentGraph, err := parentBuilder.Compile()
	if err != nil {
		t.Fatalf("compile parent: %v", err)
	}

	res, err := parentGraph.NewRunner(cp).
		Start(context.Background(), "sub-patch-orphan-fail-th", parentState{},
			WithHandoffOutbox[parentState, NoEffect](outbox),
		)
	if !errors.Is(err, ErrHandoffEnqueueFailed) {
		t.Fatalf("expected ErrHandoffEnqueueFailed, got %v", err)
	}
	snap, _, loadErr := cp.Load(context.Background(), "sub-patch-orphan-fail-th")
	if loadErr != nil {
		t.Fatalf("load: %v", loadErr)
	}
	if snap.RunMeta.HandoffStatus != HandoffStatusPending {
		t.Fatalf("expected pending when orphan patch fails, got %q", snap.RunMeta.HandoffStatus)
	}
	assertRunMetaHandoffStatusMatchesSnapshot(t, res, cp, "sub-patch-orphan-fail-th")
	assertHandoffReasonMatchesStatus(t, res, cp, "sub-patch-orphan-fail-th", "inner-bg")
}

func TestSubgraphHandoffEnqueueOkPatchEnqueuedFails(t *testing.T) {
	t.Parallel()

	type childState struct{}
	type parentState struct {
		Slot SubgraphSlot[childState, NoEffect]
	}

	outbox := &stubHandoffOutbox{}
	cp := &handoffPatchFailCP[parentState, NoEffect]{failOnStatus: HandoffStatusEnqueued}
	subBuilder := NewGraph[childState, NoEffect](func(_ childState, u childState) childState { return u })
	subBuilder.AddNode("work", func(_ context.Context, s childState) (childState, Directive, error) {
		return s, Handoff("inner-bg"), nil
	})
	subBuilder.AllowNoOutgoingRoute("work")
	subBuilder.SetEntryPoint("work")
	sub, err := subBuilder.Compile()
	if err != nil {
		t.Fatalf("compile subgraph: %v", err)
	}

	parentBuilder := NewGraph[parentState, NoEffect](func(_ parentState, u parentState) parentState { return u })
	loadSlot := func(p parentState) (SubgraphSlot[childState, NoEffect], bool) {
		return p.Slot, p.Slot.ExecutionPointer != ""
	}
	parentBuilder.AddNode("sub", SubgraphNodeWithSlot(
		sub,
		func(_ parentState) childState { return childState{} },
		loadSlot,
		func(p parentState, slot SubgraphSlot[childState, NoEffect]) parentState {
			p.Slot = slot
			return p
		},
		func(p parentState, _ childState) parentState { return p },
	))
	parentBuilder.SetEntryPoint("sub")
	parentBuilder.AllowNoOutgoingRoute("sub")
	parentGraph, err := parentBuilder.Compile()
	if err != nil {
		t.Fatalf("compile parent: %v", err)
	}

	res, err := parentGraph.NewRunner(cp).
		Start(context.Background(), "sub-patch-enqueued-fail-th", parentState{},
			WithHandoffOutbox[parentState, NoEffect](outbox),
		)
	if err == nil {
		t.Fatalf("expected patch failure, got %+v", res)
	}
	if !errors.Is(err, ErrHandoffPatchFailed) {
		t.Fatalf("expected ErrHandoffPatchFailed, got %v", err)
	}
	assertOrphanedHandoffSnapshot(t, cp, "sub-patch-enqueued-fail-th", res, "inner-bg")
	assertRunMetaHandoffStatusMatchesSnapshot(t, res, cp, "sub-patch-enqueued-fail-th")
	assertHandoffReasonMatchesStatus(t, res, cp, "sub-patch-enqueued-fail-th", "inner-bg")
}

func TestSubgraphHandoffEnqueueOkBothPatchesFail(t *testing.T) {
	t.Parallel()

	type childState struct{}
	type parentState struct {
		Slot SubgraphSlot[childState, NoEffect]
	}

	outbox := &stubHandoffOutbox{}
	cp := &handoffPatchFailCP[parentState, NoEffect]{
		failOnStatuses: map[HandoffStatus]struct{}{ //nolint:exhaustive // patch statuses only
			HandoffStatusEnqueued: {},
			HandoffStatusOrphaned: {},
		},
	}
	subBuilder := NewGraph[childState, NoEffect](func(_ childState, u childState) childState { return u })
	subBuilder.AddNode("work", func(_ context.Context, s childState) (childState, Directive, error) {
		return s, Handoff("inner-bg"), nil
	})
	subBuilder.AllowNoOutgoingRoute("work")
	subBuilder.SetEntryPoint("work")
	sub, err := subBuilder.Compile()
	if err != nil {
		t.Fatalf("compile subgraph: %v", err)
	}

	parentBuilder := NewGraph[parentState, NoEffect](func(_ parentState, u parentState) parentState { return u })
	loadSlot := func(p parentState) (SubgraphSlot[childState, NoEffect], bool) {
		return p.Slot, p.Slot.ExecutionPointer != ""
	}
	parentBuilder.AddNode("sub", SubgraphNodeWithSlot(
		sub,
		func(_ parentState) childState { return childState{} },
		loadSlot,
		func(p parentState, slot SubgraphSlot[childState, NoEffect]) parentState {
			p.Slot = slot
			return p
		},
		func(p parentState, _ childState) parentState { return p },
	))
	parentBuilder.SetEntryPoint("sub")
	parentBuilder.AllowNoOutgoingRoute("sub")
	parentGraph, err := parentBuilder.Compile()
	if err != nil {
		t.Fatalf("compile parent: %v", err)
	}

	res, err := parentGraph.NewRunner(cp).
		Start(context.Background(), "sub-both-patch-fail-th", parentState{},
			WithHandoffOutbox[parentState, NoEffect](outbox),
		)
	if err == nil {
		t.Fatalf("expected patch failure, got %+v", res)
	}
	if !errors.Is(err, ErrHandoffPatchFailed) {
		t.Fatalf("expected ErrHandoffPatchFailed, got %v", err)
	}
	snap, _, loadErr := cp.Load(context.Background(), "sub-both-patch-fail-th")
	if loadErr != nil {
		t.Fatalf("load: %v", loadErr)
	}
	if snap.RunMeta.HandoffStatus != HandoffStatusPending {
		t.Fatalf("expected pending when both patches fail, got %q", snap.RunMeta.HandoffStatus)
	}
	assertRunMetaHandoffStatusMatchesSnapshot(t, res, cp, "sub-both-patch-fail-th")
	assertHandoffReasonMatchesStatus(t, res, cp, "sub-both-patch-fail-th", "inner-bg")
}

type subHandoffChildState struct{}
type subHandoffParentState struct {
	Slot SubgraphSlot[subHandoffChildState, NoEffect]
}

func subHandoffParentGraph(
	t *testing.T,
	cp Checkpointer[subHandoffParentState, NoEffect],
	outbox *stubHandoffOutbox,
) *Graph[subHandoffParentState, NoEffect] {
	t.Helper()
	subBuilder := NewGraph[subHandoffChildState, NoEffect](
		func(_ subHandoffChildState, u subHandoffChildState) subHandoffChildState { return u },
	)
	subBuilder.AddNode("work", func(_ context.Context, s subHandoffChildState) (
		subHandoffChildState, Directive, error,
	) {
		return s, Handoff("inner-bg"), nil
	})
	subBuilder.AllowNoOutgoingRoute("work")
	subBuilder.SetEntryPoint("work")
	sub, err := subBuilder.Compile()
	if err != nil {
		t.Fatalf("compile subgraph: %v", err)
	}
	parentBuilder := NewGraph[subHandoffParentState, NoEffect](
		func(_ subHandoffParentState, u subHandoffParentState) subHandoffParentState { return u },
	)
	loadSlot := func(p subHandoffParentState) (SubgraphSlot[subHandoffChildState, NoEffect], bool) {
		return p.Slot, p.Slot.ExecutionPointer != ""
	}
	parentBuilder.AddNode("sub", SubgraphNodeWithSlot(
		sub,
		func(_ subHandoffParentState) subHandoffChildState { return subHandoffChildState{} },
		loadSlot,
		func(p subHandoffParentState, slot SubgraphSlot[subHandoffChildState, NoEffect]) subHandoffParentState {
			p.Slot = slot
			return p
		},
		func(p subHandoffParentState, _ subHandoffChildState) subHandoffParentState { return p },
	))
	parentBuilder.SetEntryPoint("sub")
	parentBuilder.AllowNoOutgoingRoute("sub")
	parentGraph, err := parentBuilder.Compile()
	if err != nil {
		t.Fatalf("compile parent: %v", err)
	}
	_ = cp
	_ = outbox
	return parentGraph
}

func TestSubgraphStreamHandoffEnqueueFail(t *testing.T) {
	t.Parallel()

	outbox := &stubHandoffOutbox{err: errors.New("broker down")}
	cp := newMemoryCP[subHandoffParentState, NoEffect]()
	parentGraph := subHandoffParentGraph(t, cp, outbox)

	handle, err := parentGraph.NewRunner(cp).
		Stream(context.Background(), "sub-stream-enqueue-fail-th", subHandoffParentState{},
			WithHandoffOutbox[subHandoffParentState, NoEffect](outbox),
		)
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
	events, waitErr := CollectEventsAndWait(context.Background(), handle)
	if !errors.Is(waitErr, ErrHandoffEnqueueFailed) {
		t.Fatalf("expected ErrHandoffEnqueueFailed, got %v", waitErr)
	}
	syncRes, syncErr := parentGraph.NewRunner(cp).
		Start(context.Background(), "sub-stream-enqueue-sync-th", subHandoffParentState{},
			WithHandoffOutbox[subHandoffParentState, NoEffect](outbox),
		)
	if !errors.Is(syncErr, ErrHandoffEnqueueFailed) {
		t.Fatalf("expected sync ErrHandoffEnqueueFailed, got %v", syncErr)
	}
	assertTerminalEventReasonMatchesSync(t, events, EventHandoff, syncRes.Reason)
	assertOrphanedHandoffSnapshot(t, cp, "sub-stream-enqueue-fail-th", syncRes, "inner-bg")
	assertRunMetaHandoffStatusMatchesSnapshot(t, syncRes, cp, "sub-stream-enqueue-sync-th")
}

func TestSubgraphStreamHandoffEnqueueFailPatchOrphanFails(t *testing.T) {
	t.Parallel()

	outbox := &stubHandoffOutbox{err: errors.New("broker down")}
	cp := &handoffPatchFailCP[subHandoffParentState, NoEffect]{failOnStatus: HandoffStatusOrphaned}
	parentGraph := subHandoffParentGraph(t, cp, outbox)

	handle, err := parentGraph.NewRunner(cp).
		Stream(context.Background(), "sub-stream-patch-orphan-fail-th", subHandoffParentState{},
			WithHandoffOutbox[subHandoffParentState, NoEffect](outbox),
		)
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
	events, waitErr := CollectEventsAndWait(context.Background(), handle)
	if !errors.Is(waitErr, ErrHandoffEnqueueFailed) {
		t.Fatalf("expected ErrHandoffEnqueueFailed, got %v", waitErr)
	}
	syncRes, syncErr := parentGraph.NewRunner(cp).
		Start(context.Background(), "sub-stream-patch-orphan-sync-th", subHandoffParentState{},
			WithHandoffOutbox[subHandoffParentState, NoEffect](outbox),
		)
	if !errors.Is(syncErr, ErrHandoffEnqueueFailed) {
		t.Fatalf("expected sync ErrHandoffEnqueueFailed, got %v", syncErr)
	}
	assertRunMetaHandoffStatusMatchesSnapshot(t, syncRes, cp, "sub-stream-patch-orphan-sync-th")
	assertHandoffReasonMatchesStatus(t, syncRes, cp, "sub-stream-patch-orphan-sync-th", "inner-bg")
	assertTerminalEventReasonMatchesSync(t, events, EventHandoff, syncRes.Reason)
}

func TestSubgraphStreamHandoffEnqueueOkPatchEnqueuedFails(t *testing.T) {
	t.Parallel()

	outbox := &stubHandoffOutbox{}
	cp := &handoffPatchFailCP[subHandoffParentState, NoEffect]{failOnStatus: HandoffStatusEnqueued}
	parentGraph := subHandoffParentGraph(t, cp, outbox)

	handle, err := parentGraph.NewRunner(cp).
		Stream(context.Background(), "sub-stream-patch-enqueued-fail-th", subHandoffParentState{},
			WithHandoffOutbox[subHandoffParentState, NoEffect](outbox),
		)
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
	events, waitErr := CollectEventsAndWait(context.Background(), handle)
	if waitErr == nil {
		t.Fatal("expected patch failure from Wait")
	}
	if !errors.Is(waitErr, ErrHandoffPatchFailed) {
		t.Fatalf("expected ErrHandoffPatchFailed from Wait, got %v", waitErr)
	}
	syncRes, syncErr := parentGraph.NewRunner(cp).
		Start(context.Background(), "sub-stream-patch-enqueued-sync-th", subHandoffParentState{},
			WithHandoffOutbox[subHandoffParentState, NoEffect](outbox),
		)
	if syncErr == nil {
		t.Fatalf("expected sync patch failure, got %+v", syncRes)
	}
	assertOrphanedHandoffSnapshot(t, cp, "sub-stream-patch-enqueued-fail-th", syncRes, "inner-bg")
	assertRunMetaHandoffStatusMatchesSnapshot(t, syncRes, cp, "sub-stream-patch-enqueued-sync-th")
	assertHandoffReasonMatchesStatus(t, syncRes, cp, "sub-stream-patch-enqueued-sync-th", "inner-bg")
	assertTerminalEventReasonMatchesSync(t, events, EventHandoff, syncRes.Reason)
}

func TestSubgraphStreamHandoffEnqueueOkBothPatchesFail(t *testing.T) {
	t.Parallel()

	outbox := &stubHandoffOutbox{}
	cp := &handoffPatchFailCP[subHandoffParentState, NoEffect]{
		failOnStatuses: map[HandoffStatus]struct{}{ //nolint:exhaustive // patch statuses only
			HandoffStatusEnqueued: {},
			HandoffStatusOrphaned: {},
		},
	}
	parentGraph := subHandoffParentGraph(t, cp, outbox)

	handle, err := parentGraph.NewRunner(cp).
		Stream(context.Background(), "sub-stream-both-patch-fail-th", subHandoffParentState{},
			WithHandoffOutbox[subHandoffParentState, NoEffect](outbox),
		)
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
	events, waitErr := CollectEventsAndWait(context.Background(), handle)
	if waitErr == nil {
		t.Fatal("expected patch failure from Wait")
	}
	if !errors.Is(waitErr, ErrHandoffPatchFailed) {
		t.Fatalf("expected ErrHandoffPatchFailed from Wait, got %v", waitErr)
	}
	syncRes, syncErr := parentGraph.NewRunner(cp).
		Start(context.Background(), "sub-stream-both-patch-sync-th", subHandoffParentState{},
			WithHandoffOutbox[subHandoffParentState, NoEffect](outbox),
		)
	if syncErr == nil {
		t.Fatalf("expected sync patch failure, got %+v", syncRes)
	}
	assertRunMetaHandoffStatusMatchesSnapshot(t, syncRes, cp, "sub-stream-both-patch-sync-th")
	assertHandoffReasonMatchesStatus(t, syncRes, cp, "sub-stream-both-patch-sync-th", "inner-bg")
	assertTerminalEventReasonMatchesSync(t, events, EventHandoff, syncRes.Reason)
}

func TestSubgraphTransactionalHandoffSuccess(t *testing.T) {
	t.Parallel()

	outbox := &stubHandoffOutbox{}
	cp := &transactionalMemoryCP[subHandoffParentState, NoEffect]{
		memoryCP: newMemoryCP[subHandoffParentState, NoEffect](),
	}
	parentGraph := subHandoffParentGraph(t, cp, outbox)

	res, err := parentGraph.NewRunner(cp).Start(context.Background(), "sub-tx-handoff-ok-th", subHandoffParentState{},
		WithHandoffOutbox[subHandoffParentState, NoEffect](outbox),
	)
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	if res.Status != RunStatusHandoff {
		t.Fatalf("expected handoff, got %s", res.Status)
	}
	snap, rev, loadErr := cp.Load(context.Background(), "sub-tx-handoff-ok-th")
	if loadErr != nil {
		t.Fatalf("load: %v", loadErr)
	}
	if snap.RunMeta.HandoffStatus != HandoffStatusEnqueued {
		t.Fatalf("expected enqueued, got %q", snap.RunMeta.HandoffStatus)
	}
	if len(outbox.calls) != 1 || outbox.calls[0].SnapshotRevision != rev {
		t.Fatalf("unexpected outbox calls: %+v rev=%d", outbox.calls, rev)
	}
}

func TestSubgraphInnerHandoffParentOutboxRunsAtParentLevel(t *testing.T) {
	t.Parallel()

	type childState struct{ Done bool }
	type parentState struct {
		Child childState
		Slot  SubgraphSlot[childState, NoEffect]
	}

	outbox := &stubHandoffOutbox{}
	subBuilder := NewGraph[childState, NoEffect](func(_ childState, u childState) childState { return u })
	subBuilder.AddNode("work", func(_ context.Context, s childState) (childState, Directive, error) {
		s.Done = true
		return s, End(), nil
	})
	subBuilder.AllowNoOutgoingRoute("work")
	subBuilder.SetEntryPoint("work")
	sub, err := subBuilder.Compile()
	if err != nil {
		t.Fatalf("compile subgraph: %v", err)
	}

	parentBuilder := NewGraph[parentState, NoEffect](func(_ parentState, u parentState) parentState { return u })
	loadSlot := func(p parentState) (SubgraphSlot[childState, NoEffect], bool) {
		return p.Slot, p.Slot.ExecutionPointer != ""
	}
	parentBuilder.AddNode("sub", SubgraphNodeWithSlot(
		sub,
		func(p parentState) childState { return p.Child },
		loadSlot,
		func(p parentState, slot SubgraphSlot[childState, NoEffect]) parentState {
			p.Slot = slot
			return p
		},
		func(p parentState, child childState) parentState {
			p.Child = child
			return p
		},
	))
	parentBuilder.AddNode("dispatch", func(_ context.Context, s parentState) (parentState, Directive, error) {
		return s, Handoff("parent-bg"), nil
	})
	parentBuilder.AddEdge("sub", "dispatch")
	parentBuilder.AllowNoOutgoingRoute("dispatch")
	parentBuilder.SetEntryPoint("sub")
	parentGraph, err := parentBuilder.Compile()
	if err != nil {
		t.Fatalf("compile parent: %v", err)
	}

	cp := newMemoryCP[parentState, NoEffect]()
	res, err := parentGraph.NewRunner(cp).
		Start(context.Background(), "parent-outbox-th", parentState{},
			WithHandoffOutbox[parentState, NoEffect](outbox),
		)
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	if res.Status != RunStatusHandoff {
		t.Fatalf("expected parent handoff, got %s", res.Status)
	}
	if res.Reason != "parent-bg" {
		t.Fatalf("expected parent handoff reason, got %q", res.Reason)
	}
	if len(outbox.calls) != 1 {
		t.Fatalf("expected single parent-level outbox call, got %d", len(outbox.calls))
	}
	token := outbox.calls[0]
	if token.ThreadID != "parent-outbox-th" {
		t.Fatalf("expected parent thread token, got %+v", token)
	}
	snap, _, err := cp.Load(context.Background(), "parent-outbox-th")
	if err != nil {
		t.Fatalf("load parent snapshot: %v", err)
	}
	if res.ResumeToken.SnapshotRevision != snap.Revision {
		t.Fatalf("result token revision %d != snapshot revision %d", res.ResumeToken.SnapshotRevision, snap.Revision)
	}
	if token.SnapshotRevision != snap.Revision-1 {
		t.Fatalf("outbox snapshot revision %d != pending revision %d", token.SnapshotRevision, snap.Revision-1)
	}
	if token.SnapshotRevision == res.ResumeToken.SnapshotRevision {
		t.Fatalf("outbox token must differ from result token when enqueued: %+v", token)
	}
	if snap.RunMeta.HandoffStatus != HandoffStatusEnqueued {
		t.Fatalf("expected enqueued handoff status on parent snap, got %q", snap.RunMeta.HandoffStatus)
	}
}
func TestAsNodeStartErrorReturnsFailDirective(t *testing.T) {
	t.Parallel()

	type state struct{}

	inner := NewGraph[state, NoEffect](func(_ state, u state) state { return u })
	inner.AddNode("boom", func(_ context.Context, s state) (state, Directive, error) {
		return s, Completed(), errors.New("inline boom")
	})
	inner.AllowNoOutgoingRoute("boom")
	inner.SetEntryPoint("boom")
	innerGraph, err := inner.Compile()
	if err != nil {
		t.Fatalf("compile inner: %v", err)
	}

	outer := NewGraph[state, NoEffect](func(_ state, u state) state { return u })
	outer.AddNode("inline", innerGraph.AsNode())
	outer.AllowNoOutgoingRoute("inline")
	outer.SetEntryPoint("inline")
	outerGraph, err := outer.Compile()
	if err != nil {
		t.Fatalf("compile outer: %v", err)
	}

	res, err := outerGraph.NewRunner(newMemoryCP[state, NoEffect]()).
		Start(context.Background(), "asnode-fail-th", state{})
	if err == nil {
		t.Fatal("expected inline graph error")
	}
	if res == nil || res.Status != RunStatusFailed {
		t.Fatalf("expected RunStatusFailed, got res=%+v", res)
	}
	if !strings.Contains(err.Error(), "inline boom") {
		t.Fatalf("expected inline boom in error, got %v", err)
	}
}
func TestSubgraphInnerResumeErrorPropagates(t *testing.T) {
	t.Parallel()

	type childState struct{ Bad bool }
	type parentState struct {
		Slot SubgraphSlot[childState, NoEffect]
	}

	sub := NewGraph[childState, NoEffect](func(_ childState, u childState) childState { return u })
	sub.AddNode("work", func(_ context.Context, s childState) (childState, Directive, error) {
		return s, Suspend("inner"), nil
	})
	sub.AllowNoOutgoingRoute("work")
	sub.SetEntryPoint("work")
	subGraph, err := sub.Compile()
	if err != nil {
		t.Fatalf("compile sub: %v", err)
	}

	parent := NewGraph[parentState, NoEffect](func(_ parentState, u parentState) parentState { return u })
	loadParentSlot := func(p parentState) (SubgraphSlot[childState, NoEffect], bool) {
		return p.Slot, p.Slot.ExecutionPointer != ""
	}
	parent.AddNode("sub", SubgraphNodeWithSlot(
		subGraph,
		func(_ parentState) childState { return childState{} },
		loadParentSlot,
		func(p parentState, slot SubgraphSlot[childState, NoEffect]) parentState {
			p.Slot = slot
			return p
		},
		func(p parentState, _ childState) parentState { return p },
	))
	parent.SetEntryPoint("sub")
	parent.AllowNoOutgoingRoute("sub")
	parentGraph, err := parent.Compile()
	if err != nil {
		t.Fatalf("compile parent: %v", err)
	}

	cp := newMemoryCP[parentState, NoEffect]()
	runner := parentGraph.NewRunner(cp)
	first, err := runner.Start(context.Background(), "sub-resume-err-th", parentState{})
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	if first.Status != RunStatusSuspended {
		t.Fatalf("expected suspended, got %s", first.Status)
	}

	badParent := first.State
	badParent.Slot.ExecutionPointer = "__missing__"
	overlayMerger := func(_, overlay parentState) parentState { return overlay }
	_, err = runner.Resume(
		context.Background(),
		first.ResumeToken,
		WithStateOverlay[parentState, NoEffect](badParent, overlayMerger),
	)
	if err == nil {
		t.Fatal("expected subgraph resume error")
	}
	if !strings.Contains(err.Error(), "__missing__") && !strings.Contains(err.Error(), "not found") {
		t.Fatalf("expected inner resume pointer error, got %v", err)
	}
}
func TestSubgraphDoesNotInheritCheckpointErrorPolicy(t *testing.T) {
	t.Parallel()

	type childState struct{}
	type parentState struct {
		Slot SubgraphSlot[childState, NoEffect]
	}

	subBuilder := NewGraph[childState, NoEffect](func(_ childState, u childState) childState { return u })
	subBuilder.AddNode("work", func(_ context.Context, s childState) (childState, Directive, error) {
		return s, Handoff("inner-bg"), nil
	})
	subBuilder.AllowNoOutgoingRoute("work")
	subBuilder.SetEntryPoint("work")
	sub, err := subBuilder.Compile()
	if err != nil {
		t.Fatalf("compile subgraph: %v", err)
	}

	parentBuilder := NewGraph[parentState, NoEffect](func(_ parentState, u parentState) parentState { return u })
	loadSlot := func(p parentState) (SubgraphSlot[childState, NoEffect], bool) {
		return p.Slot, p.Slot.ExecutionPointer != ""
	}
	parentBuilder.AddNode("sub", SubgraphNodeWithSlot(
		sub,
		func(_ parentState) childState { return childState{} },
		loadSlot,
		func(p parentState, slot SubgraphSlot[childState, NoEffect]) parentState {
			p.Slot = slot
			return p
		},
		func(p parentState, _ childState) parentState { return p },
	))
	parentBuilder.SetEntryPoint("sub")
	parentBuilder.AllowNoOutgoingRoute("sub")
	parentGraph, err := parentBuilder.Compile()
	if err != nil {
		t.Fatalf("compile parent: %v", err)
	}

	ctx := withSubgraphTestMode(context.Background(), subgraphTestModeFailSeedSave)
	res, err := parentGraph.NewRunner(newMemoryCP[parentState, NoEffect]()).
		Start(ctx, "sub-policy-th", parentState{},
			WithCheckpointErrorPolicy[parentState, NoEffect](CheckpointPolicySkipOnSaveError),
		)
	if err == nil {
		t.Fatalf("expected inner handoff save hard fail, got res=%+v", res)
	}
	if res != nil && res.Reason == ReasonHandoffCheckpointSkipped {
		t.Fatalf("parent SkipOnSaveError must not apply to inner subgraph save, reason=%q", res.Reason)
	}
}
func TestSubgraphInnerHandoffWithParentSkipOnSaveError(t *testing.T) {
	t.Parallel()

	type childState struct{}
	type parentState struct {
		Slot SubgraphSlot[childState, NoEffect]
	}

	subBuilder := NewGraph[childState, NoEffect](func(_ childState, u childState) childState { return u })
	subBuilder.AddNode("work", func(_ context.Context, s childState) (childState, Directive, error) {
		return s, Handoff("inner-bg"), nil
	})
	subBuilder.AllowNoOutgoingRoute("work")
	subBuilder.SetEntryPoint("work")
	sub, err := subBuilder.Compile()
	if err != nil {
		t.Fatalf("compile subgraph: %v", err)
	}

	parentBuilder := NewGraph[parentState, NoEffect](func(_ parentState, u parentState) parentState { return u })
	loadSlot := func(p parentState) (SubgraphSlot[childState, NoEffect], bool) {
		return p.Slot, p.Slot.ExecutionPointer != ""
	}
	parentBuilder.AddNode("sub", SubgraphNodeWithSlot(
		sub,
		func(_ parentState) childState { return childState{} },
		loadSlot,
		func(p parentState, slot SubgraphSlot[childState, NoEffect]) parentState {
			p.Slot = slot
			return p
		},
		func(p parentState, _ childState) parentState { return p },
	))
	parentBuilder.SetEntryPoint("sub")
	parentBuilder.AllowNoOutgoingRoute("sub")
	parentGraph, err := parentBuilder.Compile()
	if err != nil {
		t.Fatalf("compile parent: %v", err)
	}

	cp := &failingMemoryCP[parentState, NoEffect]{failSave: true}
	res, err := parentGraph.NewRunner(cp).
		Start(context.Background(), "sub-parent-skip-on-save-th", parentState{},
			WithCheckpointErrorPolicy[parentState, NoEffect](CheckpointPolicySkipOnSaveError),
		)
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	if res.Status != RunStatusHandoff {
		t.Fatalf("expected parent handoff, got %s", res.Status)
	}
	if res.Reason != ReasonHandoffCheckpointSkipped {
		t.Fatalf("expected parent soft warn reason, got %q", res.Reason)
	}
	if res.ResumeToken.ThreadID != "" {
		t.Fatalf("expected zero ResumeToken on parent soft warn, got %+v", res.ResumeToken)
	}
}
func TestSubgraphDoesNotInheritSuspendPointerResolverOnInnerHandoff(t *testing.T) {
	t.Parallel()

	type childState struct{}
	type parentState struct {
		Slot SubgraphSlot[childState, NoEffect]
	}

	sub := NewGraph[childState, NoEffect](func(_ childState, u childState) childState { return u })
	sub.AddNode("wait", func(_ context.Context, s childState) (childState, Directive, error) {
		return s, Handoff("inner-bg"), nil
	})
	sub.AddNode("router", func(_ context.Context, s childState) (childState, Directive, error) {
		return s, Handoff("inner-bg"), nil
	})
	sub.AllowNoOutgoingRoute("wait")
	sub.AllowNoOutgoingRoute("router")
	sub.SetEntryPoint("wait")
	subGraph, err := sub.Compile()
	if err != nil {
		t.Fatalf("compile sub: %v", err)
	}

	parent := NewGraph[parentState, NoEffect](func(_ parentState, u parentState) parentState { return u })
	loadResolverSlot := func(p parentState) (SubgraphSlot[childState, NoEffect], bool) {
		return p.Slot, p.Slot.ExecutionPointer != ""
	}
	parent.AddNode("sub", SubgraphNodeWithSlot(
		subGraph,
		func(_ parentState) childState { return childState{} },
		loadResolverSlot,
		func(p parentState, slot SubgraphSlot[childState, NoEffect]) parentState {
			p.Slot = slot
			return p
		},
		func(p parentState, _ childState) parentState { return p },
	))
	parent.SetEntryPoint("sub")
	parent.AllowNoOutgoingRoute("sub")
	parentGraph, err := parent.Compile()
	if err != nil {
		t.Fatalf("compile parent: %v", err)
	}

	var resolverCalls []string
	trackResolver := func(_ parentState, ptr ExecutionPointer) (ExecutionPointer, error) {
		resolverCalls = append(resolverCalls, string(ptr))
		return ptr, nil
	}
	cp := newMemoryCP[parentState, NoEffect]()
	res, err := parentGraph.NewRunner(cp).Start(
		context.Background(),
		"sub-handoff-resolver-th",
		parentState{},
		WithSuspendPointerResolver[parentState, NoEffect](trackResolver),
	)
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	if res.Status != RunStatusHandoff {
		t.Fatalf("expected handoff, got %s", res.Status)
	}
	for _, ptr := range resolverCalls {
		if ptr == "wait" {
			t.Fatalf("parent resolver must not run on inner subgraph handoff node %q", ptr)
		}
	}
	if len(resolverCalls) != 1 {
		t.Fatalf("expected single parent resolver call, got %d calls %v", len(resolverCalls), resolverCalls)
	}
	snap, _, err := cp.Load(context.Background(), "sub-handoff-resolver-th")
	if err != nil {
		t.Fatalf("load parent snapshot: %v", err)
	}
	if snap.State.Slot.ExecutionPointer != "wait" {
		t.Fatalf("expected inner pointer wait in slot, got %q", snap.State.Slot.ExecutionPointer)
	}
}
func TestSubgraphDoesNotInheritSuspendPointerResolver(t *testing.T) {
	t.Parallel()

	type childState struct{}
	type parentState struct {
		Slot SubgraphSlot[childState, NoEffect]
	}

	sub := NewGraph[childState, NoEffect](func(_ childState, u childState) childState { return u })
	sub.AddNode("wait", func(_ context.Context, s childState) (childState, Directive, error) {
		return s, Suspend("inner"), nil
	})
	sub.AddNode("router", func(_ context.Context, s childState) (childState, Directive, error) {
		return s, Suspend("inner"), nil
	})
	sub.AllowNoOutgoingRoute("wait")
	sub.AllowNoOutgoingRoute("router")
	sub.SetEntryPoint("wait")
	subGraph, err := sub.Compile()
	if err != nil {
		t.Fatalf("compile sub: %v", err)
	}

	parent := NewGraph[parentState, NoEffect](func(_ parentState, u parentState) parentState { return u })
	loadResolverSlot := func(p parentState) (SubgraphSlot[childState, NoEffect], bool) {
		return p.Slot, p.Slot.ExecutionPointer != ""
	}
	parent.AddNode("sub", SubgraphNodeWithSlot(
		subGraph,
		func(_ parentState) childState { return childState{} },
		loadResolverSlot,
		func(p parentState, slot SubgraphSlot[childState, NoEffect]) parentState {
			p.Slot = slot
			return p
		},
		func(p parentState, _ childState) parentState { return p },
	))
	parent.SetEntryPoint("sub")
	parent.AllowNoOutgoingRoute("sub")
	parentGraph, err := parent.Compile()
	if err != nil {
		t.Fatalf("compile parent: %v", err)
	}

	var resolverCalls []string
	trackResolver := func(_ parentState, ptr ExecutionPointer) (ExecutionPointer, error) {
		resolverCalls = append(resolverCalls, string(ptr))
		return ptr, nil
	}
	cp := newMemoryCP[parentState, NoEffect]()
	_, err = parentGraph.NewRunner(cp).Start(
		context.Background(),
		"sub-resolver-th",
		parentState{},
		WithSuspendPointerResolver[parentState, NoEffect](trackResolver),
	)
	if err != nil {
		t.Fatalf("start: %v", err)
	}

	for _, ptr := range resolverCalls {
		if ptr == "wait" {
			t.Fatalf("parent resolver must not run on inner subgraph suspend pointer %q", ptr)
		}
	}
	if len(resolverCalls) == 0 {
		t.Fatal("expected parent resolver call on parent suspend save path")
	}

	snap, _, err := cp.Load(context.Background(), "sub-resolver-th")
	if err != nil {
		t.Fatalf("load parent snapshot: %v", err)
	}
	if snap.State.Slot.ExecutionPointer != "wait" {
		t.Fatalf("expected inner pointer wait without parent resolver, got %q", snap.State.Slot.ExecutionPointer)
	}
}
func TestSubgraphSeedSaveBypassesParentCheckpointPolicy(t *testing.T) {
	t.Parallel()

	type childState struct{ Step int }
	type parentState struct {
		Child childState
		Slot  SubgraphSlot[childState, NoEffect]
	}

	subBuilder := NewGraph[childState, NoEffect](func(_ childState, u childState) childState { return u })
	subBuilder.AddNode("work", func(_ context.Context, s childState) (childState, Directive, error) {
		return s, End(), nil
	})
	subBuilder.SetEntryPoint("work")
	subBuilder.AllowNoOutgoingRoute("work")
	sub, err := subBuilder.Compile()
	if err != nil {
		t.Fatalf("compile subgraph: %v", err)
	}

	parentBuilder := NewGraph[parentState, NoEffect](func(_ parentState, u parentState) parentState { return u })
	parentBuilder.AddNode("sub", SubgraphNodeWithSlot(
		sub,
		func(s parentState) childState { return s.Child },
		func(s parentState) (SubgraphSlot[childState, NoEffect], bool) {
			return SubgraphSlot[childState, NoEffect]{
				ExecutionPointer: "work",
				Revision:         1,
				State:            s.Child,
			}, true
		},
		func(s parentState, slot SubgraphSlot[childState, NoEffect]) parentState {
			s.Slot = slot
			return s
		},
		func(s parentState, child childState) parentState {
			s.Child = child
			return s
		},
	))
	parentBuilder.SetEntryPoint("sub")
	parentBuilder.AllowNoOutgoingRoute("sub")
	parentGraph, err := parentBuilder.Compile()
	if err != nil {
		t.Fatalf("compile parent: %v", err)
	}

	ctx := withSubgraphTestMode(context.Background(), subgraphTestModeFailSeedSave)
	_, err = parentGraph.NewRunner(newMemoryCP[parentState, NoEffect]()).
		Start(ctx, "seed-policy-th", parentState{},
			WithCheckpointErrorPolicy[parentState, NoEffect](CheckpointPolicySkipOnSaveError),
		)
	if err == nil {
		t.Fatal("expected subgraph seed save hard fail")
	}
	if strings.Contains(err.Error(), "checkpoint_skipped") {
		t.Fatalf("seed save must not use parent soft warn skip, got %v", err)
	}
	if !strings.Contains(err.Error(), "subgraph seed") {
		t.Fatalf("expected subgraph seed error, got %v", err)
	}
}
func TestSubgraphStaleInnerSlotRevisionRejected(t *testing.T) {
	t.Parallel()

	type childState struct{ Step int }
	type parentState struct {
		Child childState
		Slot  SubgraphSlot[childState, NoEffect]
	}

	subBuilder := NewGraph[childState, NoEffect](func(_ childState, u childState) childState { return u })
	subBuilder.AddNode("work", func(_ context.Context, s childState) (childState, Directive, error) {
		s.Step++
		return s, Suspend("child-wait"), nil
	})
	subBuilder.SetEntryPoint("work")
	subBuilder.AllowNoOutgoingRoute("work")
	sub, err := subBuilder.Compile()
	if err != nil {
		t.Fatalf("compile subgraph: %v", err)
	}

	parentBuilder := NewGraph[parentState, NoEffect](func(_ parentState, u parentState) parentState { return u })
	loadSlot := func(s parentState) (SubgraphSlot[childState, NoEffect], bool) {
		return s.Slot, s.Slot.ExecutionPointer != ""
	}
	parentBuilder.AddNode("sub", SubgraphNodeWithSlot(
		sub,
		func(s parentState) childState { return s.Child },
		loadSlot,
		func(s parentState, slot SubgraphSlot[childState, NoEffect]) parentState {
			s.Slot = slot
			return s
		},
		func(s parentState, child childState) parentState {
			s.Child = child
			return s
		},
	))
	parentBuilder.SetEntryPoint("sub")
	parentBuilder.AllowNoOutgoingRoute("sub")
	parentGraph, err := parentBuilder.Compile()
	if err != nil {
		t.Fatalf("compile parent: %v", err)
	}

	cp := newMemoryCP[parentState, NoEffect]()
	ctx := withSubgraphTestMode(context.Background(), subgraphTestModeStaleInnerRevision)
	first, err := parentGraph.NewRunner(cp).Start(ctx, "stale-inner-th", parentState{
		Slot: SubgraphSlot[childState, NoEffect]{
			ExecutionPointer: "work",
			Revision:         1,
			State:            childState{},
		},
	})
	if err == nil {
		t.Fatalf("expected stale inner resume token, got res=%+v", first)
	}
	if !errors.Is(err, ErrConcurrencyConflict) {
		t.Fatalf("expected ErrConcurrencyConflict on stale inner revision, got %v", err)
	}
}
func TestSubgraphInnerContextCanceledMapsToCompleted(t *testing.T) {
	t.Parallel()

	type childState struct{ Ticks int }
	type parentState struct {
		Child childState
		Slot  SubgraphSlot[childState, NoEffect]
	}

	subBuilder := NewGraph[childState, NoEffect](func(_ childState, u childState) childState { return u })
	subBuilder.AddNode("loop", func(_ context.Context, s childState) (childState, Directive, error) {
		s.Ticks++
		return s, Completed(), nil
	})
	subBuilder.AddEdge("loop", "loop")
	subBuilder.SetEntryPoint("loop")
	sub, err := subBuilder.Compile(WithMaxSteps(3))
	if err != nil {
		t.Fatalf("compile subgraph: %v", err)
	}

	parentBuilder := NewGraph[parentState, NoEffect](func(_ parentState, u parentState) parentState { return u })
	parentBuilder.AddNode("sub", SubgraphNodeWithSlot(
		sub,
		func(s parentState) childState { return s.Child },
		func(s parentState) (SubgraphSlot[childState, NoEffect], bool) {
			return s.Slot, s.Slot.ExecutionPointer != ""
		},
		func(s parentState, slot SubgraphSlot[childState, NoEffect]) parentState {
			s.Slot = slot
			return s
		},
		func(s parentState, child childState) parentState {
			s.Child = child
			return s
		},
	))
	parentBuilder.AllowNoOutgoingRoute("sub")
	parentBuilder.SetEntryPoint("sub")
	parentGraph, err := parentBuilder.Compile()
	if err != nil {
		t.Fatalf("compile parent: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = parentGraph.NewRunner(newMemoryCP[parentState, NoEffect]()).Start(ctx, "inner-cancel-th", parentState{})
	if err == nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled from subgraph inner cancel mapping, got %v", err)
	}
}
func TestAsNodeInnerContextCanceledMapsToCompleted(t *testing.T) {
	t.Parallel()

	type state struct{ N int }

	innerBuilder := NewGraph[state, NoEffect](func(_ state, u state) state { return u })
	innerBuilder.AddNode("loop", func(_ context.Context, s state) (state, Directive, error) {
		s.N++
		return s, Completed(), nil
	})
	innerBuilder.AddEdge("loop", "loop")
	innerBuilder.SetEntryPoint("loop")
	inner, err := innerBuilder.Compile(WithMaxSteps(2))
	if err != nil {
		t.Fatalf("compile inner: %v", err)
	}

	outerBuilder := NewGraph[state, NoEffect](func(_ state, u state) state { return u })
	outerBuilder.AddNode("inline", inner.AsNode())
	outerBuilder.AllowNoOutgoingRoute("inline")
	outerBuilder.SetEntryPoint("inline")
	outer, err := outerBuilder.Compile()
	if err != nil {
		t.Fatalf("compile outer: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = outer.NewRunner(newMemoryCP[state, NoEffect]()).Start(ctx, "asnode-cancel-th", state{})
	if err == nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled from AsNode inner cancel, got %v", err)
	}
}
