//go:build integration

package postgres

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/skosovsky/flowy"
	"github.com/skosovsky/flowy/checkpoint"
)

func TestChildIntentPersistentRestartWithoutDuplicateGroup(t *testing.T) {
	// Arrange: intent-only node cannot complete until its children are joined.
	ctx, pool := racePool(t)
	if _, err := pool.Exec(ctx, ExecutionSchemaSQL()); err != nil {
		t.Fatal(err)
	}
	id := testThreadID(t)
	plan := flowy.ChildGroupPlan{
		Key:            "group",
		Label:          "isolated",
		MergeLabel:     "ordered",
		BudgetLabel:    "fixed",
		CancelLabel:    "confirmed",
		MaxConcurrency: 2,
		FailurePolicy:  flowy.ChildCollectErrors,
		Children:       []flowy.ChildSpec{{ID: "child", Input: []byte("input")}},
	}
	store := NewExecutionStore(pool)
	first, err := postgresChildIntentRunner(t, store, plan).Start(ctx, id, intState{})
	if !errors.Is(err, flowy.ErrChildrenUnresolved) || first == nil {
		t.Fatalf("unjoined parent completed: %v", err)
	}
	source, err := store.LoadExecution(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	pool.Close()
	restartCtx, restartPool := racePool(t)
	restartedStore := NewExecutionStore(restartPool)
	// Act.
	_, resumeErr := postgresChildIntentRunner(t, restartedStore, plan).Resume(restartCtx, first.ResumeToken)
	latest, err := restartedStore.LoadExecution(restartCtx, id)
	// Assert: replay through a fresh connection commits no replacement group or terminal.
	if err != nil || !errors.Is(resumeErr, flowy.ErrChildrenUnresolved) || latest.Revision != source.Revision ||
		latest.Digest != source.Digest || string(latest.ChildrenPayload) != string(source.ChildrenPayload) || latest.Terminal != nil {
		t.Fatalf("child intent reset after restart: resume=%v latest=%+v load=%v", resumeErr, latest, err)
	}
}

func postgresChildIntentRunner(
	t *testing.T,
	store flowy.ExecutionStore,
	plan flowy.ChildGroupPlan,
) *flowy.DurableRunner[intState, flowy.NoEffect] {
	return postgresChildLaunchRunner(t, store, plan, nil)
}

func postgresChildLaunchRunner(t *testing.T, store flowy.ExecutionStore, plan flowy.ChildGroupPlan,
	dispatch flowy.ChildDispatcher) *flowy.DurableRunner[intState, flowy.NoEffect] {
	t.Helper()
	builder := flowy.NewGraph[intState, flowy.NoEffect](func(_, update intState) intState { return update })
	builder.AddNode("node", func(ctx context.Context, state intState) (intState, flowy.Directive, error) {
		var err error
		if dispatch == nil {
			_, err = flowy.PrepareChildren(ctx, plan, nil)
		} else {
			_, err = flowy.RunChildren(ctx, plan, nil, dispatch)
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
