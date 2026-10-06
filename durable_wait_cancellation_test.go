package flowy_test

import (
	"context"
	"errors"
	"reflect"
	"sync/atomic"
	"testing"

	"github.com/skosovsky/flowy"
	"github.com/skosovsky/flowy/checkpoint"
)

func cancellationForDelivery(delivery flowy.WaitDelivery) flowy.WaitCancellation {
	return flowy.WaitCancellation{Generation: delivery.Generation, ID: "cancel", Reason: "operator requested",
		Evidence: "host-evidence"}
}

func TestWaitCancellationReplayLateEventAndTerminalCannotRevive(t *testing.T) {
	t.Parallel()
	// Arrange.
	ctx := context.Background()
	store := newWaitRegistrationStore()
	var calls, matches, applies, decodes atomic.Int32
	runner := deliveryRunner(t, store, &calls, checkpoint.JSONSerializer[durableTestState]{})
	armed, err := runner.Start(ctx, "run", durableTestState{})
	if err != nil {
		t.Fatal(err)
	}
	delivery := deliveryForArmedWait(t, store)
	request := cancellationForDelivery(delivery)
	rawRunner := deliveryRunner(t, store, &calls, failingDecode{calls: &decodes})
	// Act: cancellation and its replay have no domain decoding or node calls.
	canceled, cancelErr := rawRunner.CancelWait(ctx, armed.ResumeToken, request)
	if cancelErr != nil {
		t.Fatal(cancelErr)
	}
	before, err := store.LoadExecution(ctx, "run")
	if err != nil {
		t.Fatal(err)
	}
	replayed, replayErr := rawRunner.CancelWait(ctx, armed.ResumeToken, request)
	contract := deliveryContract(&matches, &applies)
	contract.Match, contract.Apply = nil, nil
	late, lateErr := rawRunner.DeliverWait(ctx, "run", delivery, contract)
	after, afterErr := store.LoadExecution(ctx, "run")
	// Assert: a late rejection preserves canceled state, pointer, activation and terminal.
	if replayErr != nil || replayed != canceled || lateErr != nil ||
		late.Decision.Status != flowy.WaitRejectedCanceled ||
		afterErr != nil ||
		after.Terminal == nil ||
		before.Terminal == nil ||
		!reflect.DeepEqual(after.Terminal, before.Terminal) ||
		after.Progress.ExecutionPointer != before.Progress.ExecutionPointer ||
		after.Activation != before.Activation ||
		decodes.Load() != 0 ||
		matches.Load() != 0 ||
		applies.Load() != 0 ||
		calls.Load() != 1 {
		t.Fatalf(
			"cancellation revived: replay=%+v/%v late=%+v/%v after=%+v load=%v",
			replayed,
			replayErr,
			late,
			lateErr,
			after,
			afterErr,
		)
	}
	result, resumeErr := runner.Resume(ctx, late.ResumeToken)
	if !errors.Is(resumeErr, flowy.ErrExecutionFailed) || result == nil || result.Status != flowy.RunStatusFailed ||
		result.Reason != "durable_wait_canceled" || calls.Load() != 1 || store.registrations.Load() != 1 {
		t.Fatalf("canceled Resume re-entered: %+v err=%v calls=%d", result, resumeErr, calls.Load())
	}
}

func TestWaitCancellationCommitFaultAndChangedEvidenceReject(t *testing.T) {
	t.Parallel()
	// Arrange.
	ctx := context.Background()
	store := newWaitRegistrationStore()
	store.failAt = 3
	var calls atomic.Int32
	runner := deliveryRunner(t, store, &calls, checkpoint.JSONSerializer[durableTestState]{})
	armed, err := runner.Start(ctx, "run", durableTestState{})
	if err != nil {
		t.Fatal(err)
	}
	request := cancellationForDelivery(deliveryForArmedWait(t, store))
	before, err := store.LoadExecution(ctx, "run")
	if err != nil {
		t.Fatal(err)
	}
	// Act.
	uncommitted, failedErr := runner.CancelWait(ctx, armed.ResumeToken, request)
	after, afterErr := store.LoadExecution(ctx, "run")
	// Assert: no partial cancellation or terminal, and no successful token on write failure.
	if !errors.Is(failedErr, errInjectedCommit) || uncommitted.SnapshotRevision != 0 || afterErr != nil ||
		after.Digest != before.Digest || after.Terminal != nil || after.Revision != before.Revision {
		t.Fatalf(
			"cancel fault partially committed: token=%+v err=%v after=%+v load=%v",
			uncommitted,
			failedErr,
			after,
			afterErr,
		)
	}
	canceled, cancelErr := runner.CancelWait(ctx, armed.ResumeToken, request)
	if cancelErr != nil {
		t.Fatal(cancelErr)
	}
	request.Evidence = "changed"
	_, changedErr := runner.CancelWait(ctx, canceled, request)
	request.Evidence, request.ID = "host-evidence", "another-cancellation"
	_, duplicateErr := runner.CancelWait(ctx, canceled, request)
	if !errors.Is(changedErr, flowy.ErrWaitConflict) || !errors.Is(duplicateErr, flowy.ErrWaitCanceled) ||
		calls.Load() != 1 {
		t.Fatalf("changed cancellation accepted: changed=%v different=%v", changedErr, duplicateErr)
	}
}

func TestWaitCancellationStaleTokenAndResolvedWinnerReject(t *testing.T) {
	t.Parallel()
	// Arrange: unmatched delivery advances the aggregate but does not resolve the wait.
	ctx := context.Background()
	store := newWaitRegistrationStore()
	var calls, matches, applies atomic.Int32
	runner := deliveryRunner(t, store, &calls, checkpoint.JSONSerializer[durableTestState]{})
	armed, err := runner.Start(ctx, "run", durableTestState{})
	if err != nil {
		t.Fatal(err)
	}
	delivery := deliveryForArmedWait(t, store)
	request := cancellationForDelivery(delivery)
	delivery.Payload = []byte("unmatched")
	unmatched, err := runner.DeliverWait(ctx, "run", delivery, deliveryContract(&matches, &applies))
	if err != nil {
		t.Fatal(err)
	}
	// Act.
	_, staleErr := runner.CancelWait(ctx, armed.ResumeToken, request)
	delivery.ID, delivery.Payload, delivery.ExpectedRevision = "accepted", []byte(
		"approved",
	), unmatched.ResumeToken.SnapshotRevision
	accepted, err := runner.DeliverWait(ctx, "run", delivery, deliveryContract(&matches, &applies))
	if err != nil {
		t.Fatal(err)
	}
	_, resolvedErr := runner.CancelWait(ctx, accepted.ResumeToken, request)
	// Assert: no cancellation replaces the winner/selected continuation.
	if !errors.Is(staleErr, flowy.ErrWaitStale) || !errors.Is(resolvedErr, flowy.ErrWaitConflict) ||
		applies.Load() != 1 {
		t.Fatalf("stale/winner cancellation accepted: stale=%v resolved=%v", staleErr, resolvedErr)
	}
}
