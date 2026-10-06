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

type childTerminalFaultStore struct {
	flowy.ExecutionStore
}

func (s childTerminalFaultStore) CommitExecution(ctx context.Context, revision uint64, lease flowy.ExecutionLease,
	envelope flowy.ExecutionEnvelope) (flowy.ExecutionEnvelope, error) {
	if envelope.Terminal != nil {
		return flowy.ExecutionEnvelope{}, errors.New("terminal publication unavailable")
	}
	return s.ExecutionStore.CommitExecution(ctx, revision, lease, envelope)
}

func postgresChildJoinRunner(
	t *testing.T,
	store flowy.ExecutionStore,
	dispatches, merges *atomic.Int32,
) *flowy.DurableRunner[intState, flowy.NoEffect] {
	t.Helper()
	plan := flowy.ChildGroupPlan{Key: "join", Label: "isolated", MergeLabel: "ordered", BudgetLabel: "fixed",
		CancelLabel: "confirmed", MaxConcurrency: 2, FailurePolicy: flowy.ChildCollectErrors,
		Children: []flowy.ChildSpec{{ID: "b"}, {ID: "a"}}}
	builder := flowy.NewGraph[intState, flowy.NoEffect](func(_, update intState) intState { return update })
	builder.AddNode("node", func(ctx context.Context, state intState) (intState, flowy.Directive, error) {
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
				merges.Add(1)
				return append(children[0].Result, children[1].Result...), nil
			},
		)
		if err == nil && string(result) != "ab" {
			err = errors.New("unordered joined result")
		}
		return state, flowy.End(), err
	}).AllowNoOutgoingRoute("node").SetEntryPoint("node")
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

func TestChildJoinCommittedResultSurvivesPersistentRestart(t *testing.T) {
	// Arrange: join commits, but the following terminal publication fails.
	ctx, pool := racePool(t)
	if _, err := pool.Exec(ctx, ExecutionSchemaSQL()); err != nil {
		t.Fatal(err)
	}
	id := testThreadID(t)
	var dispatches, merges atomic.Int32
	store := mustExecutionStore(t, pool)
	first, err := postgresChildJoinRunner(
		t,
		childTerminalFaultStore{ExecutionStore: store},
		&dispatches,
		&merges,
	).Start(ctx, id, intState{})
	if err == nil || first == nil {
		t.Fatalf("terminal failure missing: %v", err)
	}
	pool.Close()
	restartCtx, restartPool := racePool(t)
	// Act: a fresh worker/pool must replay the committed join, not run its merge.
	_, resumeErr := postgresChildJoinRunner(
		t,
		mustExecutionStore(t, restartPool),
		&dispatches,
		&merges,
	).Resume(restartCtx, first.ResumeToken)
	// Assert.
	if resumeErr != nil || dispatches.Load() != 2 || merges.Load() != 1 {
		t.Fatalf("persistent join replay: %v dispatch=%d merge=%d", resumeErr, dispatches.Load(), merges.Load())
	}
}
