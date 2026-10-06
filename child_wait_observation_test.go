package flowy_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/skosovsky/flowy"
	"github.com/skosovsky/flowy/checkpoint"
	"github.com/skosovsky/flowy/testutil"
)

type childCarrierObserver struct {
	activityObserver

	carrier atomic.Value
}

func (o *childCarrierObserver) ObserveLifecycle(ctx context.Context, event flowy.LifecycleObservation) {
	if event.Operation == flowy.LifecycleChildResolve && event.DecisionID != "" {
		carrier, _ := ctx.Value(durableTraceKey{}).(string)
		o.carrier.Store(carrier)
	}
	o.activityObserver.ObserveLifecycle(ctx, event)
}

func requireRuntimeObservation(t *testing.T, events []flowy.LifecycleObservation,
	operation flowy.LifecycleOperation, stage flowy.LifecycleStage, code string,
) flowy.LifecycleObservation {
	t.Helper()
	for _, event := range events {
		if event.Operation == operation && event.Stage == stage && (code == "" || event.Code == code) {
			if stage == flowy.LifecycleCommitted && event.Revision <= event.SourceRevision ||
				stage == flowy.LifecycleFailed && event.Revision != 0 {
				t.Fatalf("invalid publication boundary: %+v", event)
			}
			return event
		}
	}
	t.Fatalf("missing observation %s/%s/%s: %+v", operation, stage, code, events)
	return flowy.LifecycleObservation{}
}

func TestChildOutcomeObservationLostAcknowledgementAndHostResolution(t *testing.T) {
	// Arrange: dispatch succeeds, while the outcome write loses acknowledgement.
	observer := &childCarrierObserver{}
	flowy.SetLifecycleObserver(observer)
	t.Cleanup(func() { flowy.SetLifecycleObserver(nil) })
	var injections atomic.Int32
	flowy.SetTelemetryBridge(durableTraceBridge{injections: &injections})
	t.Cleanup(func() { flowy.SetTelemetryBridge(nil) })
	base := testutil.NewMemoryExecutionStore(nil)
	faulty := &faultExecutionStore{ExecutionStore: base, failAt: 5}
	var calls atomic.Int32
	dispatch := func(context.Context, flowy.ChildInvocation) (flowy.ChildResult, error) {
		calls.Add(1)
		return flowy.ChildResult{State: flowy.ChildCompleted, Payload: []byte("done")}, nil
	}
	// Act: recover through an addressed operator decision, then complete the join.
	sourceContext := context.WithValue(context.Background(), durableTraceKey{}, "initial-child-trace")
	_, err := task24ChildJoinRunner(t, faulty, dispatch).Start(sourceContext, "run", durableTestState{})
	if !errors.Is(err, errInjectedCommit) {
		t.Fatal(err)
	}
	before := observer.snapshot()
	token, decision, _ := task24ChildDecision(t, base)
	restarted := task24ChildJoinRunner(t, base, dispatch)
	resolved, err := restarted.ResolveChildOutcome(context.Background(), token, decision)
	if err != nil {
		t.Fatal(err)
	}
	finished, err := restarted.Resume(context.Background(), resolved)
	// Assert: failed acknowledgement never exports committed outcome or repeats dispatch.
	if err != nil || finished.State.Value != 4 || calls.Load() != 1 {
		t.Fatalf("recovery: %+v err=%v calls=%d", finished, err, calls.Load())
	}
	running := requireRuntimeObservation(
		t,
		before,
		flowy.LifecycleChildLaunch,
		flowy.LifecycleCommitted,
		"child_running",
	)
	failed := requireRuntimeObservation(t, before, flowy.LifecycleChildResolve, flowy.LifecycleFailed, "")
	if running.ChildExecutionID != decision.ExecutionID || failed.ChildExecutionID != decision.ExecutionID {
		t.Fatalf("child identity lost: running=%+v failed=%+v", running, failed)
	}
	for _, event := range before {
		if event.Operation == flowy.LifecycleChildResolve && event.Stage == flowy.LifecycleCommitted {
			t.Fatalf("failed outcome acknowledged by telemetry: %+v", event)
		}
	}
	committed := requireRuntimeObservation(
		t,
		observer.snapshot(),
		flowy.LifecycleChildResolve,
		flowy.LifecycleCommitted,
		"child_completed",
	)
	if committed.DecisionID != decision.DecisionID || committed.SourceRevision != token.SnapshotRevision ||
		committed.Revision != resolved.SnapshotRevision || observer.carrier.Load() != "initial-child-trace" {
		t.Fatalf("operator publication address: %+v", committed)
	}
	requireRuntimeObservation(t, observer.snapshot(), flowy.LifecycleChildJoin, flowy.LifecycleCommitted, "")
}

func TestWaitObservationsSeparateFailedWinnerReplayAndLoser(t *testing.T) {
	// Arrange: an armed wait and an injected winner write failure.
	observer := &activityObserver{}
	flowy.SetLifecycleObserver(observer)
	t.Cleanup(func() { flowy.SetLifecycleObserver(nil) })
	store := newWaitRegistrationStore()
	var calls, matches, applies atomic.Int32
	runner := deliveryRunner(t, store, &calls, checkpoint.JSONSerializer[durableTestState]{})
	if _, err := runner.Start(context.Background(), "run", durableTestState{}); err != nil {
		t.Fatal(err)
	}
	delivery := deliveryForArmedWait(t, store)
	contract := deliveryContract(&matches, &applies)
	store.failAt = store.commits.Load() + 1
	// Act: failed publication is followed by one accepted decision, its replay and a loser.
	_, err := runner.DeliverWait(context.Background(), "run", delivery, contract)
	if !errors.Is(err, errInjectedCommit) {
		t.Fatal(err)
	}
	before := observer.snapshot()
	accepted, err := runner.DeliverWait(context.Background(), "run", delivery, contract)
	if err != nil {
		t.Fatal(err)
	}
	duplicate, duplicateErr := runner.DeliverWait(context.Background(), "run", delivery, contract)
	loser := delivery
	loser.ID, loser.Kind, loser.Payload = "late-timer", flowy.WaitTimer, nil
	lost, lostErr := runner.DeliverWait(context.Background(), "run", loser, contract)
	// Assert: a recorded loser is not an accepted winner; replay publishes no new revision.
	if duplicateErr != nil || lostErr != nil || !duplicate.Replay ||
		duplicate.ResumeToken != accepted.ResumeToken || lost.Decision.Status != flowy.WaitLost || calls.Load() != 1 {
		t.Fatalf(
			"decisions: accepted=%+v duplicate=%+v/%v loser=%+v/%v",
			accepted,
			duplicate,
			duplicateErr,
			lost,
			lostErr,
		)
	}
	requireRuntimeObservation(t, before, flowy.LifecycleWaitArm, flowy.LifecycleCommitted, "")
	requireRuntimeObservation(t, before, flowy.LifecycleWaitDelivery, flowy.LifecycleFailed, "wait_accepted")
	for _, event := range before {
		if event.Operation == flowy.LifecycleWaitDelivery && event.Stage == flowy.LifecycleCommitted {
			t.Fatalf("unacknowledged winner: %+v", event)
		}
	}
	requireRuntimeObservation(
		t,
		observer.snapshot(),
		flowy.LifecycleWaitDelivery,
		flowy.LifecycleCommitted,
		"wait_accepted",
	)
	requireRuntimeObservation(
		t,
		observer.snapshot(),
		flowy.LifecycleWaitDelivery,
		flowy.LifecycleReplayed,
		"wait_accepted",
	)
	lostEvent := requireRuntimeObservation(
		t,
		observer.snapshot(),
		flowy.LifecycleWaitDelivery,
		flowy.LifecycleCommitted,
		"wait_lost",
	)
	if lostEvent.WorkID != delivery.Generation || lostEvent.DecisionID != loser.ID {
		t.Fatalf("lost decision address: %+v", lostEvent)
	}
}
