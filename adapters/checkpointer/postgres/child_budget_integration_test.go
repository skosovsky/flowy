//go:build integration

package postgres

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/skosovsky/flowy"
)

func postgresBudgetPlan(key string, units int) flowy.ChildGroupPlan {
	plan := nonCooperativeChildPlan()
	plan.Key = key
	plan.Children[0].Allocation = map[string]int{"units": units}
	return plan
}

func postgresChildBudgetRunner(t *testing.T, store flowy.ExecutionStore, complete bool,
	dispatches *atomic.Int32) *flowy.DurableRunner[intState, flowy.NoEffect] {
	t.Helper()
	return postgresChildNodeRunner(
		t,
		store,
		func(ctx context.Context, state intState) (intState, flowy.Directive, error) {
			capacity := map[string]int{"units": 10}
			first, err := flowy.RunChildren(
				ctx,
				postgresBudgetPlan("first", 8),
				capacity,
				func(context.Context, flowy.ChildInvocation) (flowy.ChildResult, error) {
					dispatches.Add(1)
					return flowy.ChildResult{State: flowy.ChildCompleted}, nil
				},
			)
			if err != nil {
				return state, flowy.End(), err
			}
			first, err = flowy.ReturnChildBudget(ctx, first, flowy.ChildBudgetReturn{
				ChildID:       first.Children[0].Spec.ID,
				ChildRevision: first.Children[0].Revision,
				DecisionID:    "usage",
				Reason:        "consumed",
				Evidence:      "host meter",
				Used:          map[string]int{"units": 3},
			})
			if err != nil || !complete {
				return state, flowy.End(), err
			}
			second, err := flowy.PrepareChildren(ctx, postgresBudgetPlan("second", 7), capacity)
			if err == nil {
				second, err = flowy.CancelChildren(
					ctx,
					second,
					flowy.ChildCancelRequest{ID: "not needed", Reason: "host requested"},
					func(context.Context, flowy.ChildCancelNotice) error {
						return errors.New("planned child should not notify remote work")
					},
				)
			}
			if err != nil {
				return state, flowy.End(), err
			}
			_, excessErr := flowy.PrepareChildren(ctx, postgresBudgetPlan("third", 1), capacity)
			if !errors.Is(excessErr, flowy.ErrBudgetExceeded) {
				return state, flowy.End(), errors.New("persistent return was applied twice")
			}
			merge := func(context.Context, []flowy.ChildRecord) ([]byte, error) { return nil, nil }
			_, err = flowy.JoinChildren(ctx, second, merge)
			if err == nil {
				_, err = flowy.JoinChildren(ctx, first, merge)
			}
			return state, flowy.End(), err
		},
	)
}

func TestIntegrationChildBudgetPersistentPartialReturnReplayAcrossPoolRestart(t *testing.T) {
	// Arrange: return commits before an intentionally unjoined parent loses its pool.
	ctx, pool := racePool(t)
	if _, err := pool.Exec(ctx, ExecutionSchemaSQL()); err != nil {
		t.Fatal(err)
	}
	id := testThreadID(t)
	var dispatches atomic.Int32
	store := NewExecutionStore(pool)
	first, err := postgresChildBudgetRunner(t, store, false, &dispatches).Start(ctx, id, intState{})
	if first == nil || !errors.Is(err, flowy.ErrChildrenUnresolved) {
		t.Fatalf("parent bypassed pending join: %v", err)
	}
	group := postgresStoredChildGroup(ctx, t, store, id)
	claim := group.BudgetReturns[group.Children[0].Spec.ID]
	if claim.Used["units"] != 3 || claim.Returned["units"] != 5 {
		t.Fatalf("partial return missing: %+v", claim)
	}
	pool.Close()
	restartCtx, restartPool := reopenPool(t, pool)
	restartStore := NewExecutionStore(restartPool)
	// Act: replay the first return and allocate seven remaining units on a new pool.
	_, err = postgresChildBudgetRunner(t, restartStore, true, &dispatches).Resume(restartCtx, first.ResumeToken)
	latest, loadErr := restartStore.LoadExecution(restartCtx, id)
	// Assert: no child relaunch, second return credit or lost terminal.
	if err != nil || loadErr != nil || latest.Terminal == nil || dispatches.Load() != 1 {
		t.Fatalf("budget restart: %v load=%v dispatch=%d", err, loadErr, dispatches.Load())
	}
}
