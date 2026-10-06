package main

import (
	"context"
	"fmt"
	"sync/atomic"

	"github.com/skosovsky/flowy"
	"github.com/skosovsky/flowy/checkpoint"
	"github.com/skosovsky/flowy/testutil"
)

func childrenDemo(ctx context.Context) error {
	var dispatches atomic.Int32
	dispatch, err := flowy.TypedChildDispatcher(checkpoint.JSONSerializer[int]{}, checkpoint.JSONSerializer[int]{},
		func(_ context.Context, invocation flowy.TypedChildInvocation[int]) (flowy.TypedChildResult[int], error) {
			dispatches.Add(1)
			return flowy.TypedChildResult[int]{State: flowy.ChildCompleted, Result: invocation.Input * correction}, nil
		})
	if err != nil {
		return err
	}
	runner, err := bind(
		testutil.NewMemoryExecutionStore(nil),
		"children",
		func(ctx context.Context, s state) (state, flowy.Directive, error) {
			updated, groupErr := runChildGroup(ctx, s, dispatch)
			return updated, flowy.End(), groupErr
		},
		options(),
	)
	if err != nil {
		return err
	}
	result, err := runner.Start(ctx, "children-demo", state{})
	if err != nil || result == nil || result.State.Value != 30 || dispatches.Load() != 2 {
		return fmt.Errorf("child merge/replay failed: result=%+v dispatches=%d err=%w", result, dispatches.Load(), err)
	}
	_, err = runner.Resume(ctx, result.ResumeToken)
	if err != nil || dispatches.Load() != 2 {
		return fmt.Errorf("children redispatched: %w", err)
	}
	return nil
}

func runChildGroup(ctx context.Context, s state, dispatch flowy.ChildDispatcher) (state, error) {
	specs, err := flowy.ProjectChildSpecs(
		ctx,
		s,
		[]flowy.ChildProjectionSpec{
			{ID: "b", Allocation: map[string]int{workCounter: 1}},
			{ID: "a", Allocation: map[string]int{workCounter: 1}},
		},
		checkpoint.JSONSerializer[state]{},
		checkpoint.JSONSerializer[int]{},
		func(_ context.Context, parent state, id string) (int, error) {
			return parent.Value + map[string]int{"a": 1, "b": 2}[id], nil
		},
	)
	if err != nil {
		return s, err
	}
	plan := flowy.ChildGroupPlan{Key: "team", Label: "host-projection-json", MergeLabel: "id-order",
		BudgetLabel: "fixed-work", CancelLabel: "host-confirmed", MaxConcurrency: 2,
		FailurePolicy: flowy.ChildCollectErrors, Children: specs}
	group, err := flowy.RunChildren(ctx, plan, map[string]int{workCounter: 2}, dispatch)
	if err != nil {
		return s, err
	}
	for _, child := range group.Children {
		group, err = flowy.ReturnChildBudget(ctx, group, flowy.ChildBudgetReturn{
			ChildID: child.Spec.ID, ChildRevision: child.Revision, DecisionID: "usage-" + child.Spec.ID,
			Reason: "host measured usage", Evidence: "host receipt", Used: map[string]int{workCounter: 1}})
		if err != nil {
			return s, err
		}
	}
	values, err := flowy.JoinTypedChildren(
		ctx,
		group,
		checkpoint.JSONSerializer[int]{},
		checkpoint.JSONSerializer[[]int]{},
		func(_ context.Context, outcomes []flowy.TypedChildOutcome[int]) ([]int, error) {
			if len(outcomes) != 2 || outcomes[0].ID != "a" || outcomes[1].ID != "b" {
				return nil, fmt.Errorf("unordered child outcomes: %+v", outcomes)
			}
			return []int{outcomes[0].Result, outcomes[1].Result}, nil
		},
	)
	if err != nil {
		return s, err
	}
	s.Value = values[0] + values[1]
	return s, nil
}
