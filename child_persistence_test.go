package flowy_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/skosovsky/flowy"
	"github.com/skosovsky/flowy/checkpoint"
	"github.com/skosovsky/flowy/testutil"
)

func persistedChildPlan() flowy.ChildGroupPlan {
	return flowy.ChildGroupPlan{
		Key:            "group",
		Label:          "isolated",
		MergeLabel:     "ordered",
		BudgetLabel:    "fixed",
		CancelLabel:    "confirmed",
		MaxConcurrency: 2,
		FailurePolicy:  flowy.ChildCollectErrors,
		Children:       []flowy.ChildSpec{{ID: "child", Input: []byte("input")}},
	}
}

func childIntentRunner(
	t *testing.T,
	store flowy.ExecutionStore,
	plan *flowy.ChildGroupPlan,
) *flowy.DurableRunner[durableTestState, flowy.NoEffect] {
	t.Helper()
	builder := flowy.NewGraph[durableTestState, flowy.NoEffect](
		func(_, update durableTestState) durableTestState { return update },
	)
	builder.AddNode("node", func(ctx context.Context, state durableTestState) (durableTestState, flowy.Directive, error) {
		group, err := flowy.PrepareChildren(ctx, *plan, nil)
		if err == nil {
			group.Children[0].Spec.Input[0] = 'X'
		}
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
	return runner
}

func TestChildIntentReplayCannotAdvanceOrResetGroup(t *testing.T) {
	t.Parallel()
	// Arrange.
	ctx := context.Background()
	store := testutil.NewMemoryExecutionStore(nil)
	plan := persistedChildPlan()
	runner := childIntentRunner(t, store, &plan)
	// Act: the node ignores its unjoined group and attempts End.
	first, err := runner.Start(ctx, "run", durableTestState{})
	if !errors.Is(err, flowy.ErrChildrenUnresolved) || first == nil {
		t.Fatalf("unjoined parent completed: %v", err)
	}
	_, replayErr := runner.Resume(ctx, first.ResumeToken)
	latest, err := store.LoadExecution(ctx, "run")
	if err != nil {
		t.Fatal(err)
	}
	// Assert: replay adds no group/revision, and the caller's copy did not mutate persistence.
	var groups map[string]flowy.ChildGroupRecord
	if err := json.Unmarshal(latest.ChildrenPayload, &groups); err != nil {
		t.Fatal(err)
	}
	if !errors.Is(replayErr, flowy.ErrChildrenUnresolved) || latest.Revision != 2 || latest.Terminal != nil ||
		len(groups) != 1 {
		t.Fatalf("intent replay changed execution: %+v %v", latest, replayErr)
	}
	for _, group := range groups {
		if string(group.Children[0].Spec.Input) != "input" {
			t.Fatal("returned copy aliases persisted input")
		}
	}
	plan.MergeLabel = "changed"
	_, mismatchErr := runner.Resume(ctx, first.ResumeToken)
	if !errors.Is(mismatchErr, flowy.ErrChildInvalid) {
		t.Fatalf("incompatible plan reset: %v", mismatchErr)
	}
}

func TestChildIntentCommitFailureLeavesNoGroup(t *testing.T) {
	t.Parallel()
	// Arrange.
	ctx := context.Background()
	base := testutil.NewMemoryExecutionStore(nil)
	store := &faultExecutionStore{ExecutionStore: base, failAt: 2}
	plan := persistedChildPlan()
	// Act.
	_, err := childIntentRunner(t, store, &plan).Start(ctx, "run", durableTestState{})
	latest, loadErr := base.LoadExecution(ctx, "run")
	// Assert.
	if !errors.Is(err, errInjectedCommit) || loadErr != nil || latest.Revision != 1 ||
		len(latest.ChildrenPayload) != 0 ||
		latest.Terminal != nil {
		t.Fatalf("partial intent exposed: %v %+v load=%v", err, latest, loadErr)
	}
}
