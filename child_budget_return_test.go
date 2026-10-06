package flowy_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/skosovsky/flowy"
	"github.com/skosovsky/flowy/testutil"
)

func budgetChildPlan(key string, units int) flowy.ChildGroupPlan {
	plan := persistedChildPlan()
	plan.Key = key
	plan.Children[0].Allocation = map[string]int{"units": units}
	return plan
}

func TestChildBudgetLedgerPartialReturnReplayAndCommitFault(t *testing.T) {
	for _, failAt := range []int32{0, 6} {
		t.Run(map[int32]string{0: "normal", 6: "return commit"}[failAt], func(t *testing.T) {
			// Arrange: total capacity ten, first allocation eight, consumption three.
			ctx := context.Background()
			base := testutil.NewMemoryExecutionStore(nil)
			store := &faultExecutionStore{ExecutionStore: base, failAt: failAt}
			var dispatches atomic.Int32
			runner := childJoinRunner(t, store, budgetLedgerNode(base, &dispatches))
			// Act: a failed return commit is recoverable without another child dispatch.
			first, err := runner.Start(ctx, "budget", durableTestState{})
			if failAt != 0 {
				if first == nil || !errors.Is(err, errInjectedCommit) {
					t.Fatalf("return commit fault missing: %v", err)
				}
				_, err = runner.Resume(ctx, first.ResumeToken)
			}
			// Assert: one dispatch for each stable child, no false budget replenishment.
			latest, loadErr := base.LoadExecution(ctx, "budget")
			if err != nil || loadErr != nil || latest.Terminal == nil || dispatches.Load() != 2 {
				t.Fatalf("budget recovery: %v load=%v dispatch=%d", err, loadErr, dispatches.Load())
			}
		})
	}
}

func budgetLedgerNode(
	store flowy.ExecutionStore,
	dispatches *atomic.Int32,
) flowy.Node[durableTestState, flowy.NoEffect] {
	return func(ctx context.Context, state durableTestState) (durableTestState, flowy.Directive, error) {
		capacity := map[string]int{"units": 10}
		dispatch := func(context.Context, flowy.ChildInvocation) (flowy.ChildResult, error) {
			dispatches.Add(1)
			return flowy.ChildResult{State: flowy.ChildCompleted}, nil
		}
		first, err := flowy.RunChildren(ctx, budgetChildPlan("first", 8), capacity, dispatch)
		if err != nil {
			return state, flowy.End(), err
		}
		if len(first.BudgetReturns) == 0 {
			_, budgetErr := flowy.PrepareChildren(ctx, budgetChildPlan("second", 7), capacity)
			if !errors.Is(budgetErr, flowy.ErrBudgetExceeded) {
				return state, flowy.End(), errors.New("groups oversubscribed parent capacity")
			}
		}
		claim := flowy.ChildBudgetReturn{ChildID: first.Children[0].Spec.ID, ChildRevision: first.Children[0].Revision,
			DecisionID: "usage", Reason: "completed usage", Evidence: "host meter", Used: map[string]int{"units": 3}}
		first, err = flowy.ReturnChildBudget(ctx, first, claim)
		if err != nil {
			return state, flowy.End(), err
		}
		before, loadErr := store.LoadExecution(ctx, "budget")
		if loadErr != nil {
			return state, flowy.End(), loadErr
		}
		first, err = flowy.ReturnChildBudget(ctx, first, claim)
		after, loadErr := store.LoadExecution(ctx, "budget")
		if err != nil || loadErr != nil || before.Digest != after.Digest ||
			first.BudgetReturns[claim.ChildID].Returned["units"] != 5 {
			return state, flowy.End(), errors.New("return replay changed ledger")
		}
		second, err := flowy.RunChildren(ctx, budgetChildPlan("second", 7), capacity, dispatch)
		if err != nil {
			return state, flowy.End(), err
		}
		_, budgetErr := flowy.PrepareChildren(ctx, budgetChildPlan("third", 1), capacity)
		if !errors.Is(budgetErr, flowy.ErrBudgetExceeded) {
			return state, flowy.End(), errors.New("return replenished capacity twice")
		}
		merge := func(context.Context, []flowy.ChildRecord) ([]byte, error) { return nil, nil }
		_, err = flowy.JoinChildren(ctx, second, merge)
		if err == nil {
			_, err = flowy.JoinChildren(ctx, first, merge)
		}
		return state, flowy.End(), err
	}
}
