package flowy_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/skosovsky/flowy"
	"github.com/skosovsky/flowy/testutil"
)

func TestDurableStepSaveFailureStopsNextNode(t *testing.T) {
	// Arrange: the first step commit fails after the initial envelope is durable.
	ctx := context.Background()
	store := &faultExecutionStore{ExecutionStore: testutil.NewMemoryExecutionStore(nil), failAt: 2}
	var firstCalls, nextCalls atomic.Int32
	builder := flowy.NewGraph[durableTestState, flowy.NoEffect](
		func(_, update durableTestState) durableTestState { return update },
	)
	builder.AddNode("first", func(_ context.Context, state durableTestState) (durableTestState, flowy.Directive, error) {
		firstCalls.Add(1)
		state.Value++
		return state, flowy.Completed(), nil
	}).
		AddNode("next", func(_ context.Context, state durableTestState) (durableTestState, flowy.Directive, error) {
			nextCalls.Add(1)
			state.Value++
			return state, flowy.End(), nil
		}).
		AddEdge("first", "next").
		AllowNoOutgoingRoute("next").
		SetEntryPoint("first")
	runner := compileActivityTestRunner(t, builder, store)
	// Act.
	failed, err := runner.Start(ctx, "run", durableTestState{})
	// Assert: only the pure first step ran; its uncommitted cursor is not exposed.
	if !errors.Is(err, errInjectedCommit) || failed == nil || failed.State.Value != 0 ||
		failed.RunMeta.StepCount != 0 || firstCalls.Load() != 1 || nextCalls.Load() != 0 {
		t.Fatalf(
			"next node crossed failed commit: %+v %v first=%d next=%d",
			failed,
			err,
			firstCalls.Load(),
			nextCalls.Load(),
		)
	}
	stored, loadErr := store.LoadExecution(ctx, "run")
	if loadErr != nil || stored.Progress.ExecutionPointer != "first" || stored.Activation != 1 ||
		stored.Terminal != nil {
		t.Fatalf("uncommitted step visible: %+v %v", stored, loadErr)
	}
	resumed, resumeErr := runner.Resume(ctx, failed.ResumeToken)
	if resumeErr != nil || resumed.State.Value != 2 || firstCalls.Load() != 2 || nextCalls.Load() != 1 {
		t.Fatalf("step recovery failed: %+v %v", resumed, resumeErr)
	}
}
