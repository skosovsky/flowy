package flowy

import (
	"context"
	"errors"
	"strings"
	"testing"
)

func TestSubgraphHandoffPropagates(t *testing.T) {
	t.Parallel()

	type childState struct{}
	type parentState struct {
		HandedOff bool
	}

	subBuilder := NewGraph[childState, NoEffect](func(_ childState, u childState) childState { return u })
	subBuilder.AddNode("work", func(_ context.Context, s childState) (childState, Directive, error) {
		return s, Handoff("child_to_background"), nil
	})
	subBuilder.AllowNoOutgoingRoute("work")
	subBuilder.SetEntryPoint("work")
	sub, err := subBuilder.Compile()
	if err != nil {
		t.Fatalf("compile subgraph: %v", err)
	}

	parentBuilder := NewGraph[parentState, NoEffect](func(_ parentState, u parentState) parentState { return u })
	parentBuilder.AddNode("sub", SubgraphNode(
		sub,
		func(_ parentState) childState { return childState{} },
		func(s parentState, _ childState) parentState {
			s.HandedOff = true
			return s
		},
	))
	parentBuilder.SetEntryPoint("sub")
	parentBuilder.AllowNoOutgoingRoute("sub")
	parentGraph, err := parentBuilder.Compile()
	if err != nil {
		t.Fatalf("compile parent: %v", err)
	}

	scheduler := &stubHandoffScheduler{}
	cp := newMemoryCP[parentState, NoEffect]()
	res, err := parentGraph.NewRunner(cp).
		Start(context.Background(), "sub-handoff", parentState{},
			WithHandoffScheduler[parentState, NoEffect](scheduler),
		)
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	if res.Status != RunStatusHandoff {
		t.Fatalf("expected handoff status, got %s", res.Status)
	}
	if res.Reason != "child_to_background" {
		t.Fatalf("expected child handoff reason, got %q", res.Reason)
	}
	if len(scheduler.calls) != 1 {
		t.Fatalf("expected single parent-level scheduler call after inner handoff, got %d", len(scheduler.calls))
	}
	if scheduler.calls[0].ThreadID != "sub-handoff" {
		t.Fatalf("expected parent thread in scheduler token, got %+v", scheduler.calls[0])
	}
	snap, loadErr := cp.Load(context.Background(), "sub-handoff")
	if loadErr != nil {
		t.Fatalf("load: %v", loadErr)
	}
	if res.ResumeToken.SnapshotRevision != snap.Revision {
		t.Fatalf("snapshot revision %d != revision %d", res.ResumeToken.SnapshotRevision, snap.Revision)
	}
	if scheduler.calls[0].SnapshotRevision != snap.Revision {
		t.Fatalf("scheduler snapshot revision %d != revision %d", scheduler.calls[0].SnapshotRevision, snap.Revision)
	}
}

func TestSubgraphContextCancelPropagates(t *testing.T) {
	t.Parallel()

	type childState struct {
		Ticks int
	}

	type parentState struct {
		Child childState
	}

	subBuilder := NewGraph[childState, NoEffect](func(_ childState, u childState) childState { return u })
	subBuilder.AddNode("loop", func(_ context.Context, s childState) (childState, Directive, error) {
		s.Ticks++
		return s, Completed(), nil
	})
	subBuilder.AddEdge("loop", "loop")
	subBuilder.SetEntryPoint("loop")
	sub, err := subBuilder.Compile(WithMaxSteps(100))
	if err != nil {
		t.Fatalf("compile subgraph: %v", err)
	}

	parentBuilder := NewGraph[parentState, NoEffect](func(_ parentState, u parentState) parentState { return u })
	parentBuilder.AddNode("sub", SubgraphNode(
		sub,
		func(s parentState) childState { return s.Child },
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

	res, err := parentGraph.NewRunner(newMemoryCP[parentState, NoEffect]()).Start(ctx, "cancel-subgraph", parentState{})
	if err == nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got res=%+v err=%v", res, err)
	}
	if res != nil && res.Status != RunStatusContextCanceled {
		t.Fatalf("expected context_canceled status, got %s", res.Status)
	}
}

func TestSubgraphNodeWithSlotContextCancelPropagates(t *testing.T) {
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
	sub, err := subBuilder.Compile(WithMaxSteps(100))
	if err != nil {
		t.Fatalf("compile subgraph: %v", err)
	}

	parentBuilder := NewGraph[parentState, NoEffect](func(_ parentState, u parentState) parentState { return u })
	parentBuilder.AddNode("sub", SubgraphNodeWithSlot(
		sub,
		func(s parentState) childState { return s.Child },
		func(s parentState) (SubgraphSlot[childState, NoEffect], bool) {
			if s.Slot.ExecutionPointer == "" {
				return SubgraphSlot[childState, NoEffect]{}, false
			}
			return s.Slot, true
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

	res, err := parentGraph.NewRunner(newMemoryCP[parentState, NoEffect]()).
		Start(ctx, "cancel-slot-subgraph", parentState{})
	if err == nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got res=%+v err=%v", res, err)
	}
	if res != nil && res.Status != RunStatusContextCanceled {
		t.Fatalf("expected context_canceled status, got %s", res.Status)
	}
}

func TestSubgraphHandoffResumeContinuity(t *testing.T) {
	t.Parallel()

	type childState struct {
		Step int
	}

	type parentState struct {
		Child childState
		Slot  SubgraphSlot[childState, NoEffect]
	}

	subBuilder := NewGraph[childState, NoEffect](func(_ childState, u childState) childState { return u })
	subBuilder.AddNode("step1", func(_ context.Context, s childState) (childState, Directive, error) {
		if s.Step == 0 {
			s.Step = 1
			return s, Handoff("child_pause"), nil
		}
		return s, Completed(), nil
	})
	subBuilder.AddNode("step2", func(_ context.Context, s childState) (childState, Directive, error) {
		s.Step = 2
		return s, End(), nil
	})
	subBuilder.AddEdge("step1", "step2")
	subBuilder.AllowNoOutgoingRoute("step2")
	subBuilder.SetEntryPoint("step1")
	sub, err := subBuilder.Compile()
	if err != nil {
		t.Fatalf("compile subgraph: %v", err)
	}

	parentBuilder := NewGraph[parentState, NoEffect](func(_ parentState, u parentState) parentState { return u })
	parentBuilder.AddNode("sub", SubgraphNodeWithSlot(
		sub,
		func(s parentState) childState { return s.Child },
		func(s parentState) (SubgraphSlot[childState, NoEffect], bool) {
			if s.Slot.ExecutionPointer == "" {
				return SubgraphSlot[childState, NoEffect]{}, false
			}
			return s.Slot, true
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
	parentBuilder.AddNode("finalize", func(_ context.Context, s parentState) (parentState, Directive, error) {
		return s, End(), nil
	})
	parentBuilder.AddEdge("sub", "finalize")
	parentBuilder.AllowNoOutgoingRoute("finalize")
	parentBuilder.SetEntryPoint("sub")
	parentGraph, err := parentBuilder.Compile()
	if err != nil {
		t.Fatalf("compile parent: %v", err)
	}

	cp := newMemoryCP[parentState, NoEffect]()
	runner := parentGraph.NewRunner(cp)

	first, err := runner.Start(context.Background(), "sub-resume-th", parentState{})
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	if first.Status != RunStatusHandoff {
		t.Fatalf("expected handoff, got %s", first.Status)
	}
	if first.ResumeToken.SnapshotRevision <= 0 {
		t.Fatalf("expected parent ResumeToken on handoff, got %+v", first.ResumeToken)
	}
	snap, err := cp.Load(context.Background(), "sub-resume-th")
	if err != nil {
		t.Fatalf("load parent snapshot: %v", err)
	}
	if first.ResumeToken.SnapshotRevision != snap.Revision {
		t.Fatalf("snapshot revision %d != snapshot revision %d", first.ResumeToken.SnapshotRevision, snap.Revision)
	}
	if first.State.Slot.ExecutionPointer != "step1" || first.State.Child.Step != 1 {
		t.Fatalf("expected subgraph slot at step1, got slot=%+v child=%+v", first.State.Slot, first.State.Child)
	}

	second, err := runner.Resume(context.Background(), first.ResumeToken)
	if err != nil {
		t.Fatalf("resume: %v", err)
	}
	if second.Status != RunStatusCompleted {
		t.Fatalf("expected completed, got %s", second.Status)
	}
	if second.State.Child.Step != 2 {
		t.Fatalf("expected subgraph to continue to step2, child=%+v", second.State.Child)
	}
}

func TestSubgraphSuspendSlotResumeContinuity(t *testing.T) {
	t.Parallel()

	type childState struct {
		Step int
	}

	type parentState struct {
		Child childState
		Slot  SubgraphSlot[childState, NoEffect]
	}

	subBuilder := NewGraph[childState, NoEffect](func(_ childState, u childState) childState { return u })
	subBuilder.AddNode("work", func(_ context.Context, s childState) (childState, Directive, error) {
		s.Step++
		if s.Step < 2 {
			return s, Suspend("child-wait"), nil
		}
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
			if s.Slot.ExecutionPointer == "" {
				return SubgraphSlot[childState, NoEffect]{}, false
			}
			return s.Slot, true
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
	parentBuilder.AddNode("finalize", func(_ context.Context, s parentState) (parentState, Directive, error) {
		return s, End(), nil
	})
	parentBuilder.AddEdge("sub", "finalize")
	parentBuilder.AllowNoOutgoingRoute("finalize")
	parentBuilder.SetEntryPoint("sub")
	parentGraph, err := parentBuilder.Compile()
	if err != nil {
		t.Fatalf("compile parent: %v", err)
	}

	cp := newMemoryCP[parentState, NoEffect]()
	runner := parentGraph.NewRunner(cp)

	first, err := runner.Start(context.Background(), "sub-suspend-th", parentState{})
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	if first.Status != RunStatusSuspended {
		t.Fatalf("expected suspended, got %s", first.Status)
	}
	if first.ResumeToken.SnapshotRevision <= 0 {
		t.Fatalf("expected parent ResumeToken on suspend, got %+v", first.ResumeToken)
	}
	if first.State.Slot.ExecutionPointer != "work" || first.State.Child.Step != 1 {
		t.Fatalf("unexpected slot after suspend: slot=%+v child=%+v", first.State.Slot, first.State.Child)
	}

	second, err := runner.Resume(context.Background(), first.ResumeToken)
	if err != nil {
		t.Fatalf("resume: %v", err)
	}
	if second.Status != RunStatusCompleted {
		t.Fatalf("expected completed, got %s", second.Status)
	}
	if second.State.Child.Step != 2 {
		t.Fatalf("expected child step 2 after slot resume, got %d", second.State.Child.Step)
	}
}

func TestComposeSubgraphResumeToken(t *testing.T) {
	t.Parallel()

	type childState struct {
		Step int
	}
	type parentState struct {
		Child childState
		Slot  SubgraphSlot[childState, NoEffect]
	}

	subBuilder := NewGraph[childState, NoEffect](func(_ childState, u childState) childState { return u })
	subBuilder.AddNode("work", func(_ context.Context, s childState) (childState, Directive, error) {
		s.Step++
		if s.Step < 3 {
			return s, Suspend("child-wait"), nil
		}
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
			if s.Slot.ExecutionPointer == "" {
				return SubgraphSlot[childState, NoEffect]{}, false
			}
			return s.Slot, true
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
	parentBuilder.AddNode("finalize", func(_ context.Context, s parentState) (parentState, Directive, error) {
		return s, End(), nil
	})
	parentBuilder.AddEdge("sub", "finalize")
	parentBuilder.AllowNoOutgoingRoute("finalize")
	parentBuilder.SetEntryPoint("sub")
	parentGraph, err := parentBuilder.Compile()
	if err != nil {
		t.Fatalf("compile parent: %v", err)
	}

	cp := newMemoryCP[parentState, NoEffect]()
	runner := parentGraph.NewRunner(cp)

	first, err := runner.Start(context.Background(), "compose-token-th", parentState{})
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	if first.ResumeToken.SnapshotRevision <= 0 {
		t.Fatalf("expected parent OCC ResumeToken, got %+v", first.ResumeToken)
	}
	if first.State.Slot.Revision <= 0 {
		t.Fatalf("expected inner slot revision, got %+v", first.State.Slot)
	}
	if first.State.Slot.ExecutionPointer != "work" {
		t.Fatalf("expected inner slot pointer work, got %q", first.State.Slot.ExecutionPointer)
	}

	second, err := runner.Resume(context.Background(), first.ResumeToken)
	if err != nil {
		t.Fatalf("resume: %v", err)
	}
	if second.Status != RunStatusSuspended {
		t.Fatalf("expected second suspend, got %s", second.Status)
	}
	if second.State.Child.Step != 2 {
		t.Fatalf("expected inner step 2, got %d", second.State.Child.Step)
	}

	third, err := runner.Resume(context.Background(), second.ResumeToken)
	if err != nil {
		t.Fatalf("third resume: %v", err)
	}
	if third.Status != RunStatusCompleted {
		t.Fatalf("expected completed, got %s", third.Status)
	}
	if third.State.Child.Step != 3 {
		t.Fatalf("expected inner step 3, got %d", third.State.Child.Step)
	}

	_, err = runner.Resume(context.Background(), first.ResumeToken)
	if !errors.Is(err, ErrStaleResumeToken) {
		t.Fatalf("expected stale token, got %v", err)
	}
}

func TestSubgraphSlotStoreFailure(t *testing.T) {
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
	parentBuilder.AddNode("sub", SubgraphNodeWithSlot(
		sub,
		func(s parentState) childState { return s.Child },
		func(s parentState) (SubgraphSlot[childState, NoEffect], bool) {
			if s.Slot.ExecutionPointer == "" {
				return SubgraphSlot[childState, NoEffect]{}, false
			}
			return s.Slot, true
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

	ctx := withSubgraphTestMode(context.Background(), subgraphTestModeFailSlotLoad)
	_, err = parentGraph.NewRunner(newMemoryCP[parentState, NoEffect]()).Start(ctx, "slot-load-fail-th", parentState{})
	if err == nil {
		t.Fatal("expected subgraph slot load failure")
	}
	if !strings.Contains(err.Error(), "subgraph slot") {
		t.Fatalf("expected subgraph slot error, got %v", err)
	}
}

func TestSubgraphSeedSaveFailure(t *testing.T) {
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
	_, err = parentGraph.NewRunner(newMemoryCP[parentState, NoEffect]()).Start(ctx, "seed-fail-th", parentState{})
	if err == nil {
		t.Fatal("expected subgraph seed save failure")
	}
	if !strings.Contains(err.Error(), "subgraph seed") {
		t.Fatalf("expected subgraph seed error, got %v", err)
	}
}

func TestSubgraphDoesNotInheritParentRunMetadata(t *testing.T) {
	t.Parallel()

	type childState struct {
		ParentTokens int
	}
	type parentState struct{}

	subBuilder := NewGraph[childState, NoEffect](func(_ childState, u childState) childState { return u })
	subBuilder.AddNode("read", func(ctx context.Context, s childState) (childState, Directive, error) {
		s.ParentTokens = BudgetUsed(ctx, "tokens")
		return s, End(), nil
	})
	subBuilder.AllowNoOutgoingRoute("read")
	subBuilder.SetEntryPoint("read")
	sub, err := subBuilder.Compile()
	if err != nil {
		t.Fatalf("compile subgraph: %v", err)
	}

	parentBuilder := NewGraph[parentState, NoEffect](func(_ parentState, u parentState) parentState { return u })
	parentBuilder.AddNode("sub", SubgraphNode(
		sub,
		func(_ parentState) childState { return childState{} },
		func(_ parentState, child childState) parentState {
			if child.ParentTokens != 0 {
				t.Fatalf("subgraph must not inherit parent WithRunMetadata, got tokens=%d", child.ParentTokens)
			}
			return parentState{}
		},
	))
	parentBuilder.AddNode("finalize", func(_ context.Context, s parentState) (parentState, Directive, error) {
		return s, End(), nil
	})
	parentBuilder.AddEdge("sub", "finalize")
	parentBuilder.AllowNoOutgoingRoute("finalize")
	parentBuilder.SetEntryPoint("sub")
	parentGraph, err := parentBuilder.Compile()
	if err != nil {
		t.Fatalf("compile parent: %v", err)
	}

	_, err = parentGraph.NewRunner(newMemoryCP[parentState, NoEffect]()).
		Start(context.Background(), "sub-meta-th", parentState{},
			WithRunMetadata[parentState, NoEffect](RunMetadataInput{
				BudgetCounts: map[string]int{"tokens": 99},
			}),
		)
	if err != nil {
		t.Fatalf("start: %v", err)
	}
}
