//go:build integration

package postgres

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/skosovsky/flowy"
	"github.com/skosovsky/flowy/checkpoint"
)

func postgresTypedChildRunner(
	t *testing.T,
	store flowy.ExecutionStore,
	calls, merges *atomic.Int32,
) *flowy.DurableRunner[intState, flowy.NoEffect] {
	t.Helper()
	plan := nonCooperativeChildPlan()
	plan.MaxConcurrency = 2
	plan.Children = []flowy.ChildSpec{{ID: "b", Input: []byte(`"b"`)}, {ID: "a", Input: []byte(`"a"`)}}
	codec := checkpoint.JSONSerializer[string]{}
	dispatch, err := flowy.TypedChildDispatcher(
		codec,
		codec,
		func(_ context.Context, invocation flowy.TypedChildInvocation[string]) (flowy.TypedChildResult[string], error) {
			calls.Add(1)
			return flowy.TypedChildResult[string]{State: flowy.ChildCompleted, Result: invocation.Input}, nil
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	return postgresChildNodeRunner(
		t,
		store,
		func(ctx context.Context, state intState) (intState, flowy.Directive, error) {
			group, runErr := flowy.RunChildren(ctx, plan, nil, dispatch)
			if runErr != nil {
				return state, flowy.End(), runErr
			}
			result, joinErr := flowy.JoinTypedChildren(
				ctx,
				group,
				codec,
				codec,
				func(_ context.Context, outcomes []flowy.TypedChildOutcome[string]) (string, error) {
					merges.Add(1)
					return outcomes[0].Result + outcomes[1].Result, nil
				},
			)
			if joinErr == nil && result != "ab" {
				joinErr = errors.New("typed merge order or codec changed")
			}
			return state, flowy.End(), joinErr
		},
	)
}

func TestTypedChildCommittedMergePersistentRestart(t *testing.T) {
	// Arrange: typed join succeeds, then terminal publication fails on the old pool.
	ctx, pool := racePool(t)
	if _, err := pool.Exec(ctx, ExecutionSchemaSQL()); err != nil {
		t.Fatal(err)
	}
	id := testThreadID(t)
	var calls, merges atomic.Int32
	first, err := postgresTypedChildRunner(
		t,
		childTerminalFaultStore{ExecutionStore: NewExecutionStore(pool)},
		&calls,
		&merges,
	).Start(ctx, id, intState{})
	if first == nil || err == nil {
		t.Fatalf("terminal fault missing: %v", err)
	}
	pool.Close()
	restartCtx, restartPool := racePool(t)
	// Act: a new pool/worker decodes cached typed merge bytes rather than rerunning it.
	_, err = postgresTypedChildRunner(
		t,
		NewExecutionStore(restartPool),
		&calls,
		&merges,
	).Resume(restartCtx, first.ResumeToken)
	// Assert.
	if err != nil || calls.Load() != 2 || merges.Load() != 1 {
		t.Fatalf("typed persistent replay: %v calls=%d merges=%d", err, calls.Load(), merges.Load())
	}
}
