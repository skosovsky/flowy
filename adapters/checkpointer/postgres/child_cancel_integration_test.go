//go:build integration

package postgres

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/skosovsky/flowy"
	"github.com/skosovsky/flowy/checkpoint"
)

func postgresChildCancelRunner(
	t *testing.T,
	store flowy.ExecutionStore,
	dispatches, notices *atomic.Int32,
) *flowy.DurableRunner[intState, flowy.NoEffect] {
	t.Helper()
	plan := flowy.ChildGroupPlan{Key: "cancel", Label: "isolated", MergeLabel: "ordered", BudgetLabel: "fixed",
		CancelLabel: "confirmed", MaxConcurrency: 1, FailurePolicy: flowy.ChildCollectErrors,
		Children: []flowy.ChildSpec{{ID: "child"}}}
	node := func(ctx context.Context, state intState) (intState, flowy.Directive, error) {
		group, err := flowy.RunChildren(
			ctx,
			plan,
			nil,
			func(context.Context, flowy.ChildInvocation) (flowy.ChildResult, error) {
				dispatches.Add(1)
				return flowy.ChildResult{State: flowy.ChildWaiting, WaitID: "external"}, nil
			},
		)
		if errors.Is(err, flowy.ErrChildrenUnresolved) {
			_, err = flowy.CancelChildren(
				ctx,
				group,
				flowy.ChildCancelRequest{ID: "stop", Reason: "host requested"},
				func(context.Context, flowy.ChildCancelNotice) error {
					notices.Add(1)
					return nil
				},
			)
			return state, flowy.End(), err
		}
		if err == nil {
			_, err = flowy.JoinChildren(
				ctx,
				group,
				func(context.Context, []flowy.ChildRecord) ([]byte, error) { return nil, nil },
			)
		}
		return state, flowy.End(), err
	}
	return postgresChildNodeRunner(t, store, node)
}

func postgresChildNodeRunner(
	t *testing.T,
	store flowy.ExecutionStore,
	node flowy.Node[intState, flowy.NoEffect],
) *flowy.DurableRunner[intState, flowy.NoEffect] {
	t.Helper()
	builder := flowy.NewGraph[intState, flowy.NoEffect](func(_, update intState) intState { return update })
	builder.AddNode("node", node).AllowNoOutgoingRoute("node").SetEntryPoint("node")
	graph, err := builder.Compile()
	if err != nil {
		t.Fatal(err)
	}
	runner, err := flowy.NewDurableRunner(graph, store, referenceDescriptor("current"),
		checkpoint.JSONSerializer[intState]{}, checkpoint.JSONSerializer[[]flowy.NoEffect]{},
		flowy.DurableOptions{Owner: "worker", LeaseTTL: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	return runner
}

func TestIntegrationChildCancellationRequestAndConfirmationPersistentRestart(t *testing.T) {
	// Arrange: stop acknowledgement leaves a durably requested external wait.
	ctx, pool := racePool(t)
	if _, err := pool.Exec(ctx, ExecutionSchemaSQL()); err != nil {
		t.Fatal(err)
	}
	id := testThreadID(t)
	var dispatches, notices atomic.Int32
	store := NewExecutionStore(pool)
	first, err := postgresChildCancelRunner(t, store, &dispatches, &notices).Start(ctx, id, intState{})
	if first == nil || !errors.Is(err, flowy.ErrChildrenUnresolved) {
		t.Fatalf("requested wait missing: %v", err)
	}
	source, err := store.LoadExecution(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	pool.Close()
	restartCtx, restartPool := reopenPool(t, pool)
	restartStore := NewExecutionStore(restartPool)
	runner := postgresChildCancelRunner(t, restartStore, &dispatches, &notices)
	// Act: repeat notification after restart, then persist explicit host confirmation.
	second, resumeErr := runner.Resume(restartCtx, first.ResumeToken)
	latest, loadErr := restartStore.LoadExecution(restartCtx, id)
	group := postgresStoredChildGroup(restartCtx, t, restartStore, id)
	if second == nil || !errors.Is(resumeErr, flowy.ErrChildrenUnresolved) || loadErr != nil ||
		latest.Digest != source.Digest ||
		dispatches.Load() != 1 ||
		notices.Load() != 2 ||
		group.Children[0].CancelConfirmed ||
		group.Children[0].State != flowy.ChildWaiting {
		t.Fatalf("restart fabricated cancellation: %v load=%v group=%+v", resumeErr, loadErr, group)
	}
	child := group.Children[0]
	decision := flowy.ChildCancelConfirmation{
		Node:          group.Node,
		Activation:    group.Activation,
		GroupKey:      group.Plan.Key,
		ChildID:       child.Spec.ID,
		ExecutionID:   child.ExecutionID,
		ChildRevision: child.Revision,
		RequestID:     group.CancelRequest.ID,
		DecisionID:    "confirmed",
		Reason:        "remote worker stopped",
		Evidence:      "host verified termination",
	}
	token, err := runner.ConfirmChildCancellation(restartCtx, second.ResumeToken, decision)
	if err != nil {
		t.Fatal(err)
	}
	restartPool.Close()
	finalCtx, finalPool := reopenPool(t, pool)
	finalStore := NewExecutionStore(finalPool)
	_, err = postgresChildCancelRunner(t, finalStore, &dispatches, &notices).Resume(finalCtx, token)
	group = postgresStoredChildGroup(finalCtx, t, finalStore, id)
	// Assert: confirmed state/provenance survive another pool, with no remote relaunch.
	if err != nil || dispatches.Load() != 1 || notices.Load() != 2 || group.Children[0].State != flowy.ChildCanceled ||
		group.Children[0].CancelConfirmation == nil || group.Children[0].CancelConfirmation.DecisionID != "confirmed" {
		t.Fatalf("confirmation lost after restart: %v group=%+v", err, group)
	}
}
