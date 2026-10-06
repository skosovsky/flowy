package flowy_test

import (
	"context"
	"errors"
	"testing"

	"github.com/skosovsky/flowy"
	"github.com/skosovsky/flowy/testutil"
)

func TestChildBudgetReturnUnknownRetainsAllocation(t *testing.T) {
	// Arrange: unknown external work occupies the entire activation capacity.
	ctx := context.Background()
	store := testutil.NewMemoryExecutionStore(nil)
	runner := childJoinRunner(
		t,
		store,
		func(ctx context.Context, state durableTestState) (durableTestState, flowy.Directive, error) {
			capacity := map[string]int{"units": 8}
			group, runErr := flowy.RunChildren(
				ctx,
				budgetChildPlan("unknown", 8),
				capacity,
				func(context.Context, flowy.ChildInvocation) (flowy.ChildResult, error) {
					return flowy.ChildResult{State: flowy.ChildUnknown}, nil
				},
			)
			if !errors.Is(runErr, flowy.ErrChildrenUnresolved) {
				return state, flowy.End(), errors.New("missing unknown child")
			}
			_, returnErr := flowy.ReturnChildBudget(ctx, group, flowy.ChildBudgetReturn{
				ChildID:       group.Children[0].Spec.ID,
				ChildRevision: group.Children[0].Revision,
				DecisionID:    "usage",
				Reason:        "claimed unused",
				Evidence:      "host claim",
				Used:          map[string]int{"units": 0},
			})
			_, allocateErr := flowy.PrepareChildren(ctx, budgetChildPlan("second", 1), capacity)
			if !errors.Is(returnErr, flowy.ErrChildrenUnresolved) || !errors.Is(allocateErr, flowy.ErrBudgetExceeded) {
				return state, flowy.End(), errors.New("unknown child released its allocation")
			}
			return state, flowy.End(), runErr
		},
	)
	// Act.
	_, err := runner.Start(ctx, "unknown-budget", durableTestState{})
	group := storedChildGroup(t, store, "unknown-budget")
	// Assert: no return decision or duplicate group was committed.
	if !errors.Is(err, flowy.ErrChildrenUnresolved) || len(group.BudgetReturns) != 0 ||
		group.Children[0].State != flowy.ChildUnknown {
		t.Fatalf("unknown allocation escaped: %v group=%+v", err, group)
	}
}

func TestChildBudgetReturnRejectsStaleAndChangedClaims(t *testing.T) {
	// Arrange: settle a child and record one usage claim.
	ctx := context.Background()
	store := testutil.NewMemoryExecutionStore(nil)
	runner := childJoinRunner(
		t,
		store,
		func(ctx context.Context, state durableTestState) (durableTestState, flowy.Directive, error) {
			capacity := map[string]int{"units": 8}
			plan := budgetChildPlan("first", 8)
			group, err := flowy.RunChildren(
				ctx,
				plan,
				capacity,
				func(context.Context, flowy.ChildInvocation) (flowy.ChildResult, error) {
					return flowy.ChildResult{State: flowy.ChildCompleted}, nil
				},
			)
			if err != nil {
				return state, flowy.End(), err
			}
			claim := flowy.ChildBudgetReturn{
				ChildID:       group.Children[0].Spec.ID,
				ChildRevision: group.Children[0].Revision,
				DecisionID:    "usage",
				Reason:        "consumed",
				Evidence:      "host meter",
				Used:          map[string]int{"units": 3},
			}
			returned, err := flowy.ReturnChildBudget(ctx, group, claim)
			if err != nil {
				return state, flowy.End(), err
			}
			before, err := store.LoadExecution(ctx, "claims")
			if err != nil {
				return state, flowy.End(), err
			}
			// Act: neither a stale group nor a changed claim/capacity may replenish the ledger.
			_, staleErr := flowy.ReturnChildBudget(ctx, group, claim)
			claim.Used["units"] = 2
			_, changedErr := flowy.ReturnChildBudget(ctx, returned, claim)
			_, capacityErr := flowy.PrepareChildren(ctx, plan, map[string]int{"units": 9})
			after, loadErr := store.LoadExecution(ctx, "claims")
			if !errors.Is(staleErr, flowy.ErrChildRevision) || !errors.Is(changedErr, flowy.ErrChildRevision) ||
				!errors.Is(capacityErr, flowy.ErrBudgetExceeded) || loadErr != nil || before.Digest != after.Digest {
				return state, flowy.End(), errors.New("invalid budget mutation changed aggregate")
			}
			_, err = flowy.JoinChildren(
				ctx,
				returned,
				func(context.Context, []flowy.ChildRecord) ([]byte, error) { return nil, nil },
			)
			return state, flowy.End(), err
		},
	)
	// Assert: the valid claim remains usable for join after all rejected mutations.
	if _, err := runner.Start(ctx, "claims", durableTestState{}); err != nil {
		t.Fatal(err)
	}
}
