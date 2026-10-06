package flowy_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/skosovsky/flowy"
	"github.com/skosovsky/flowy/testutil"
)

func TestRolloverObservationsAddressBothHeadsAndReplay(t *testing.T) {
	// Arrange: a persisted boundary with settled journal records.
	observer := &activityObserver{}
	flowy.SetLifecycleObserver(observer)
	t.Cleanup(func() { flowy.SetLifecycleObserver(nil) })
	store := testutil.NewMemoryExecutionStore(nil)
	var calls, projections atomic.Int32
	runner := lifecycleRunner(t, store, 1, &calls)
	paused, err := runner.Start(context.Background(), "source", 0)
	if err != nil {
		t.Fatal(err)
	}
	request := lifecycleRolloverRequest("target", 1, &projections)
	// Act: publish the pair, then replay the same addressed request.
	rolled, err := runner.Rollover(context.Background(), paused.ResumeToken, request)
	if err != nil {
		t.Fatal(err)
	}
	replayed, replayErr := runner.Rollover(context.Background(), paused.ResumeToken, request)
	// Assert: target starts at revision one; source advances independently; projection is not repeated.
	if replayErr != nil || rolled != replayed || rolled.SnapshotRevision != 1 || projections.Load() != 1 ||
		calls.Load() != 1 {
		t.Fatalf("rollover=%+v replay=%+v err=%v projections=%d", rolled, replayed, replayErr, projections.Load())
	}
	committed := requireRuntimeObservation(
		t,
		observer.snapshot(),
		flowy.LifecycleRollover,
		flowy.LifecycleCommitted,
		"",
	)
	if committed.SourceExecutionID != "source" || committed.TargetExecutionID != "target" ||
		committed.SourceRevision != paused.ResumeToken.SnapshotRevision ||
		committed.Revision != paused.ResumeToken.SnapshotRevision+1 || committed.TargetRevision != 1 {
		t.Fatalf("pair address: %+v", committed)
	}
	requireRuntimeObservation(t, observer.snapshot(), flowy.LifecycleRollover, flowy.LifecycleReplayed, "")
}

func TestRetentionObservationsDoNotAdvanceHeadOrAcknowledgeFailure(t *testing.T) {
	// Arrange: a settled execution and an invalid maintenance policy.
	observer := &activityObserver{}
	flowy.SetLifecycleObserver(observer)
	t.Cleanup(func() { flowy.SetLifecycleObserver(nil) })
	store := testutil.NewMemoryExecutionStore(nil)
	var calls atomic.Int32
	result, err := activityTestRunner(
		t,
		store,
		&calls,
		false,
	).Start(context.Background(), "retained", durableTestState{})
	if err != nil {
		t.Fatal(err)
	}
	request := flowy.ExecutionRetentionRequest{ExecutionID: "retained", Revision: result.ResumeToken.SnapshotRevision,
		Policy: flowy.ExecutionRetentionPolicy{Label: "archive", KeepLast: 1}}
	// Act: prune history, then attempt a stale maintenance request.
	receipt, err := flowy.RetainExecution(context.Background(), store, request)
	if err != nil {
		t.Fatal(err)
	}
	request.Revision--
	_, staleErr := flowy.RetainExecution(context.Background(), store, request)
	// Assert: maintenance receipt addresses unchanged head; stale write is not acknowledged.
	if staleErr == nil || receipt.Revision != result.ResumeToken.SnapshotRevision || receipt.DeletedRevisions == 0 {
		t.Fatalf("maintenance=%+v staleErr=%v", receipt, staleErr)
	}
	var committed, failed int
	for _, event := range observer.snapshot() {
		if event.Operation != flowy.LifecycleRetention {
			continue
		}
		if event.Stage == flowy.LifecycleCommitted {
			committed++
			if event.Revision != event.SourceRevision || event.Revision != receipt.Revision {
				t.Fatalf("maintenance invented head advance: %+v", event)
			}
		}
		if event.Stage == flowy.LifecycleFailed {
			failed++
			if event.Revision != 0 {
				t.Fatalf("stale maintenance acknowledged: %+v", event)
			}
		}
	}
	if committed != 1 || failed != 1 || errors.Is(staleErr, flowy.ErrExecutionCorrupt) {
		t.Fatalf("maintenance counts committed=%d failed=%d err=%v", committed, failed, staleErr)
	}
}
