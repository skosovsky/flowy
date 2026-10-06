//go:build integration

package postgres

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/skosovsky/flowy"
)

func TestWaitConcurrentDuplicateAndTimerPersistentSingleContinuation(t *testing.T) {
	// Arrange: discard the arming pool before three concurrent deliveries.
	ctx, pool := racePool(t)
	if _, err := pool.Exec(ctx, ExecutionSchemaSQL()); err != nil {
		t.Fatal(err)
	}
	store, err := NewWaitExecutionStore(pool, postgresWaitProfile())
	if err != nil {
		t.Fatal(err)
	}
	id := testThreadID(t)
	spec := postgresWaitSpec(time.Date(2026, 10, 5, 0, 0, 0, 0, time.UTC))
	clock := waitAcceptanceClock{now: spec.Deadline}
	var nodes, matches, applies atomic.Int32
	if _, err = postgresWaitRunner(t, store, spec, &nodes, clock).Start(ctx, id, intState{}); err != nil {
		t.Fatal(err)
	}
	event := pgWaitDelivery(ctx, t, store, id)
	timer := event
	timer.ID, timer.Kind, timer.Payload = "timer", flowy.WaitTimer, nil
	pool.Close()
	restartCtx, restartPool := racePool(t)
	restarted, err := NewWaitExecutionStore(restartPool, postgresWaitProfile())
	if err != nil {
		t.Fatal(err)
	}
	runner := postgresWaitRunner(t, restarted, spec, &nodes, clock)
	contract := pgWaitContract(spec, &matches, &applies)
	// Act: event, its transport duplicate and the due timer race on the same head.
	assertWaitConcurrentAttempts(restartCtx, t, runner, id, []flowy.WaitDelivery{event, event, timer}, contract)
	eventResult, eventErr := runner.DeliverWait(restartCtx, id, event, contract)
	timerResult, timerErr := runner.DeliverWait(restartCtx, id, timer, contract)
	if eventErr != nil || timerErr != nil {
		t.Fatalf("delivery retry failed: event=%v timer=%v", eventErr, timerErr)
	}
	head, err := restarted.LoadExecution(restartCtx, id)
	if err != nil {
		t.Fatal(err)
	}
	assertConcurrentWaitWinner(t, head, eventResult, timerResult, &nodes, &matches, &applies)
	result, resumeErr := runner.Resume(restartCtx, flowy.ResumeToken{ThreadID: id, SnapshotRevision: head.Revision})
	// Assert: only the durable selected branch runs, once.
	if resumeErr != nil || result.Status != flowy.RunStatusCompleted || result.State.Value != 11 || nodes.Load() != 2 {
		t.Fatalf("concurrent deliveries duplicated continuation: %+v err=%v nodes=%d", result, resumeErr, nodes.Load())
	}
}

func assertWaitConcurrentAttempts(ctx context.Context, t *testing.T,
	runner *flowy.DurableRunner[intState, flowy.NoEffect], id string, deliveries []flowy.WaitDelivery,
	contract flowy.WaitDeliveryContract[intState],
) {
	t.Helper()
	ready := make(chan struct{}, len(deliveries))
	start := make(chan struct{})
	done := make(chan error, len(deliveries))
	for _, delivery := range deliveries {
		go func() {
			ready <- struct{}{}
			<-start
			_, err := runner.DeliverWait(ctx, id, delivery, contract)
			done <- err
		}()
	}
	for range deliveries {
		select {
		case <-ready:
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	close(start)
	for range deliveries {
		select {
		case err := <-done:
			if err != nil && !errors.Is(err, flowy.ErrLeaseHeld) && !errors.Is(err, flowy.ErrThreadLeaseBusy) {
				t.Fatalf("unexpected concurrent delivery error: %v", err)
			}
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
}

func assertConcurrentWaitWinner(t *testing.T, head flowy.ExecutionEnvelope,
	event, timer flowy.WaitDeliveryResult, nodes, matches, applies *atomic.Int32,
) {
	t.Helper()
	waits, err := flowy.InspectExecutionWaits(head)
	if err != nil || len(waits) != 1 {
		t.Fatalf("waits missing: %+v err=%v", waits, err)
	}
	wait := waits[0]
	expectedPointer := flowy.ExecutionPointer("accepted")
	expectedMatches := int32(1)
	if wait.WinnerID == "timer" {
		expectedPointer, expectedMatches = "timed-out", 0
	}
	accepted := 0
	for _, result := range []flowy.WaitDeliveryResult{event, timer} {
		if result.Decision.Status == flowy.WaitAccepted {
			accepted++
		} else if result.Decision.Status != flowy.WaitLost {
			t.Fatalf("delivery neither winner nor loser: %+v", result)
		}
	}
	if accepted != 1 || len(wait.Decisions) != 2 || head.Progress.ExecutionPointer != expectedPointer ||
		head.Activation != 2 || head.Terminal != nil || nodes.Load() != 1 || matches.Load() != expectedMatches || applies.Load() != 1 {
		t.Fatalf(
			"arbitration lost uniqueness: head=%+v waits=%+v callbacks=%d/%d",
			head,
			waits,
			matches.Load(),
			applies.Load(),
		)
	}
}
