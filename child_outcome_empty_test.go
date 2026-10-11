package flowy_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/skosovsky/flowy"
	"github.com/skosovsky/flowy/testutil"
)

func TestChildOutcomeEmptyPayloadReplaySurvivesPersistence(t *testing.T) {
	// Arrange.
	ctx := context.Background()
	store := testutil.NewMemoryExecutionStore(nil)
	var calls atomic.Int32
	dispatch := func(context.Context, flowy.ChildInvocation) (flowy.ChildResult, error) {
		calls.Add(1)
		return flowy.ChildResult{State: flowy.ChildUnknown}, nil
	}
	runner := task24ChildJoinRunner(t, store, dispatch)
	_, err := runner.Start(ctx, "run", durableTestState{})
	if !errors.Is(err, flowy.ErrChildrenUnresolved) {
		t.Fatal(err)
	}
	token, decision, _ := task24ChildDecision(t, store)
	decision.Result.Payload = []byte{}
	// Act.
	resolved, err := runner.ResolveChildOutcome(ctx, token, decision)
	if err != nil {
		t.Fatal(err)
	}
	replay, replayErr := runner.ResolveChildOutcome(ctx, resolved, decision)
	decision.Result.Payload = nil
	canonical, canonicalErr := runner.ResolveChildOutcome(ctx, resolved, decision)
	result, resumeErr := runner.Resume(ctx, resolved)
	// Assert.
	if replayErr != nil || canonicalErr != nil || replay != resolved || canonical != resolved || resumeErr != nil ||
		result.State.Value != 0 ||
		calls.Load() != 1 {
		t.Fatalf("replay=%v canonical=%v resume=%v calls=%d", replayErr, canonicalErr, resumeErr, calls.Load())
	}
}
