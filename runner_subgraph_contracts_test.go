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
func TestSubgraphDoesNotInheritHandoffScheduler(t *testing.T) {
	t.Parallel()

	type childState struct{}
	type parentState struct {
		Slot SubgraphSlot[childState, NoEffect]
	}

	scheduler := &stubHandoffScheduler{}
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

	res, err := parentGraph.NewRunner(newMemoryCP[parentState, NoEffect]()).
		Start(context.Background(), "sub-scheduler-th", parentState{},
			WithHandoffScheduler[parentState, NoEffect](scheduler),
		)
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	if res.Status != RunStatusHandoff {
		t.Fatalf("expected parent handoff from inner handoff, got %s", res.Status)
	}
	if len(scheduler.calls) != 1 {
		t.Fatalf("scheduler must run once at parent handoff, not on inner runner, calls=%d", len(scheduler.calls))
	}
	if scheduler.calls[0].ThreadID != "sub-scheduler-th" {
		t.Fatalf("expected parent thread in scheduler token, got %+v", scheduler.calls[0])
	}
}
func TestSubgraphInnerHandoffParentSchedulerRunsAtParentLevel(t *testing.T) {
	t.Parallel()

	type childState struct{ Done bool }
	type parentState struct {
		Child childState
		Slot  SubgraphSlot[childState, NoEffect]
	}

	scheduler := &stubHandoffScheduler{}
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
		Start(context.Background(), "parent-sched-th", parentState{},
			WithHandoffScheduler[parentState, NoEffect](scheduler),
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
	if len(scheduler.calls) != 1 {
		t.Fatalf("expected single parent-level scheduler call, got %d", len(scheduler.calls))
	}
	token := scheduler.calls[0]
	if token.ThreadID != "parent-sched-th" {
		t.Fatalf("expected parent thread token, got %+v", token)
	}
	if token.SnapshotRevision != res.ResumeToken.SnapshotRevision {
		t.Fatalf(
			"scheduler snapshot revision %d != result token %d",
			token.SnapshotRevision,
			res.ResumeToken.SnapshotRevision,
		)
	}
	snap, err := cp.Load(context.Background(), "parent-sched-th")
	if err != nil {
		t.Fatalf("load parent snapshot: %v", err)
	}
	if token.SnapshotRevision != snap.Revision {
		t.Fatalf("scheduler snapshot revision %d != snapshot revision %d", token.SnapshotRevision, snap.Revision)
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
	snap, err := cp.Load(context.Background(), "sub-handoff-resolver-th")
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

	snap, err := cp.Load(context.Background(), "sub-resolver-th")
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
	if !strings.Contains(err.Error(), "stale resume token") {
		t.Fatalf("expected ErrStaleResumeToken, got %v", err)
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
