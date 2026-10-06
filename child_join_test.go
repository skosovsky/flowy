package flowy_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/skosovsky/flowy"
	"github.com/skosovsky/flowy/checkpoint"
	"github.com/skosovsky/flowy/testutil"
)

func TestChildJoinFailureRetainsOutcomesAndCommittedReplay(t *testing.T) {
	// Arrange: committed children survive a pure merge failure.
	ctx := context.Background()
	store := testutil.NewMemoryExecutionStore(nil)
	plan := persistedChildPlan()
	plan.Children = []flowy.ChildSpec{{ID: "b"}, {ID: "a"}}
	var dispatches, merges atomic.Int32
	builder := flowy.NewGraph[durableTestState, flowy.NoEffect](
		func(_, update durableTestState) durableTestState { return update },
	)
	builder.AddNode("node", func(ctx context.Context, state durableTestState) (durableTestState, flowy.Directive, error) {
		group, err := flowy.RunChildren(
			ctx,
			plan,
			nil,
			func(_ context.Context, invocation flowy.ChildInvocation) (flowy.ChildResult, error) {
				dispatches.Add(1)
				return flowy.ChildResult{State: flowy.ChildCompleted, Payload: []byte(invocation.ChildID)}, nil
			},
		)
		if err != nil {
			return state, flowy.End(), err
		}
		result, err := flowy.JoinChildren(
			ctx,
			group,
			func(_ context.Context, children []flowy.ChildRecord) ([]byte, error) {
				if merges.Add(1) == 1 {
					return nil, errors.New("merge unavailable")
				}
				return append(children[0].Result, children[1].Result...), nil
			},
		)
		if err != nil {
			return state, flowy.End(), err
		}
		group, err = flowy.PrepareChildren(ctx, plan, nil)
		if err == nil {
			result, err = flowy.JoinChildren(ctx, group, func(context.Context, []flowy.ChildRecord) ([]byte, error) {
				merges.Add(100)
				return nil, errors.New("committed merge replayed")
			})
		}
		if string(result) != "ab" {
			return state, flowy.End(), errors.New("unordered merged result")
		}
		state.Value = len(result)
		return state, flowy.End(), err
	}).
		AllowNoOutgoingRoute("node").
		SetEntryPoint("node")
	graph, err := builder.Compile()
	if err != nil {
		t.Fatal(err)
	}
	runner, err := flowy.NewDurableRunner(graph, store, durableDescriptor("current"),
		checkpoint.JSONSerializer[durableTestState]{}, checkpoint.JSONSerializer[[]flowy.NoEffect]{},
		flowy.DurableOptions{Owner: "worker", LeaseTTL: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	// Act: recovery retries only the pure merge, then replays its committed result.
	first, firstErr := runner.Start(ctx, "join", durableTestState{})
	if !errors.Is(firstErr, flowy.ErrChildMergeConflict) || first == nil {
		t.Fatalf("merge failure: %v", firstErr)
	}
	second, resumeErr := runner.Resume(ctx, first.ResumeToken)
	// Assert: deterministic ID order, no duplicate dispatch or committed merge.
	if resumeErr != nil || second == nil || dispatches.Load() != 2 || merges.Load() != 2 {
		t.Fatalf("join recovery: %v dispatches=%d merges=%d", resumeErr, dispatches.Load(), merges.Load())
	}
}
