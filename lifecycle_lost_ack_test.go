package flowy_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/skosovsky/flowy"
	"github.com/skosovsky/flowy/testutil"
)

type lostAckExecutionStore struct {
	flowy.ExecutionStore

	commits atomic.Int32
	failAt  int32
}

func (s *lostAckExecutionStore) CommitExecution(ctx context.Context, revision uint64,
	lease flowy.ExecutionLease, target flowy.ExecutionEnvelope,
) (flowy.ExecutionEnvelope, error) {
	committed, err := s.ExecutionStore.CommitExecution(ctx, revision, lease, target)
	if err == nil && s.commits.Add(1) == s.failAt {
		// The backend has already acknowledged/published to this adapter. The
		// runtime caller loses that acknowledgement, not the external effect.
		return flowy.ExecutionEnvelope{}, errInjectedCommit
	}
	return committed, err
}

func TestActivityObservationAfterActualCommitAcknowledgementLoss(t *testing.T) {
	// Arrange: the completed activity is stored before returning an error.
	observer := &activityObserver{}
	flowy.SetLifecycleObserver(observer)
	t.Cleanup(func() { flowy.SetLifecycleObserver(nil) })
	base := testutil.NewMemoryExecutionStore(nil)
	store := &lostAckExecutionStore{ExecutionStore: base, failAt: 4}
	var calls atomic.Int32
	// Act: read the authoritative head after lost acknowledgement, then recover.
	_, err := activityTestRunner(t, store, &calls, false).Start(context.Background(), "run", durableTestState{})
	if !errors.Is(err, errInjectedCommit) {
		t.Fatal(err)
	}
	before := observer.snapshot()
	head, err := base.LoadExecution(context.Background(), "run")
	if err != nil {
		t.Fatal(err)
	}
	result, err := activityTestRunner(t, base, &calls, false).Resume(context.Background(),
		flowy.ResumeToken{ThreadID: head.ExecutionID, SnapshotRevision: head.Revision})
	// Assert: authoritative replay is distinct from a dispatch or the lost commit ack.
	if err != nil || result.State.Value != 1 || calls.Load() != 1 || head.Revision != 4 {
		t.Fatalf("recovery=%+v err=%v calls=%d head=%d", result, err, calls.Load(), head.Revision)
	}
	requireRuntimeObservation(t, before, flowy.LifecycleActivity, flowy.LifecycleFailed, "")
	assertNoAcknowledgedOutcome(t, before, flowy.LifecycleActivity)
	replayed := requireRuntimeObservation(t, observer.snapshot(), flowy.LifecycleActivity, flowy.LifecycleReplayed, "")
	if replayed.Revision != head.Revision || replayed.Attempt != 1 {
		t.Fatalf("replay not addressed to actual commit: %+v", replayed)
	}
}

func TestChildObservationAfterActualCommitAcknowledgementLoss(t *testing.T) {
	// Arrange: the completed child outcome survives the adapter's lost reply.
	observer := &activityObserver{}
	flowy.SetLifecycleObserver(observer)
	t.Cleanup(func() { flowy.SetLifecycleObserver(nil) })
	base := testutil.NewMemoryExecutionStore(nil)
	store := &lostAckExecutionStore{ExecutionStore: base, failAt: 5}
	var calls atomic.Int32
	dispatch := func(context.Context, flowy.ChildInvocation) (flowy.ChildResult, error) {
		calls.Add(1)
		return flowy.ChildResult{State: flowy.ChildCompleted, Payload: []byte("done")}, nil
	}
	// Act: recover from actual stored head, without operator decision or redispatch.
	_, err := task24ChildJoinRunner(t, store, dispatch).Start(context.Background(), "run", durableTestState{})
	if !errors.Is(err, errInjectedCommit) {
		t.Fatal(err)
	}
	before := observer.snapshot()
	head, err := base.LoadExecution(context.Background(), "run")
	if err != nil {
		t.Fatal(err)
	}
	result, err := task24ChildJoinRunner(t, base, dispatch).Resume(context.Background(),
		flowy.ResumeToken{ThreadID: head.ExecutionID, SnapshotRevision: head.Revision})
	// Assert: replay can expose a committed result that its original caller never acknowledged.
	if err != nil || result.State.Value != 4 || calls.Load() != 1 || head.Revision != 5 {
		t.Fatalf("recovery=%+v err=%v calls=%d head=%d", result, err, calls.Load(), head.Revision)
	}
	assertNoAcknowledgedOutcome(t, before, flowy.LifecycleChildResolve)
	replayed := requireRuntimeObservation(t, observer.snapshot(), flowy.LifecycleChildResolve,
		flowy.LifecycleReplayed, "child_completed")
	if replayed.Revision != head.Revision || replayed.ChildExecutionID == "" {
		t.Fatalf("child replay address missing: %+v", replayed)
	}
}

func assertNoAcknowledgedOutcome(
	t *testing.T,
	events []flowy.LifecycleObservation,
	operation flowy.LifecycleOperation,
) {
	t.Helper()
	for _, event := range events {
		if event.Operation == operation && event.Stage == flowy.LifecycleCommitted {
			t.Fatalf("lost acknowledgement reported committed: %+v", event)
		}
	}
}
