package flowy

import (
	"context"
	"errors"
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

	res, err := parentGraph.NewRunner(newMemoryCP[parentState, NoEffect]()).
		Start(context.Background(), "sub-handoff", parentState{})
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	if res.Status != RunStatusHandoff {
		t.Fatalf("expected handoff status, got %s", res.Status)
	}
	if res.Reason != "child_to_background" {
		t.Fatalf("expected child handoff reason, got %q", res.Reason)
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
			if s.Slot.NodeID == "" {
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
			if s.Slot.NodeID == "" {
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
	if first.State.Slot.NodeID != "step1" || first.State.Child.Step != 1 {
		t.Fatalf("expected subgraph slot at step1, got slot=%+v child=%+v", first.State.Slot, first.State.Child)
	}

	second, err := runner.Resume(context.Background(), "sub-resume-th")
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
