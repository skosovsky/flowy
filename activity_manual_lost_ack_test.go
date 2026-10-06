package flowy_test

import (
	"bytes"
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/skosovsky/flowy"
	"github.com/skosovsky/flowy/testutil"
)

type activityDecisionLostAckStore struct {
	flowy.ExecutionStore

	lost atomic.Bool
}

var errActivityDecisionAck = errors.New("resolution acknowledgement lost")

func (s *activityDecisionLostAckStore) CommitExecution(ctx context.Context, revision uint64,
	lease flowy.ExecutionLease, envelope flowy.ExecutionEnvelope) (flowy.ExecutionEnvelope, error) {
	committed, err := s.ExecutionStore.CommitExecution(ctx, revision, lease, envelope)
	if err == nil && bytes.Contains(envelope.JournalPayload, []byte(`"resolutions"`)) &&
		s.lost.CompareAndSwap(false, true) {
		return flowy.ExecutionEnvelope{}, errActivityDecisionAck
	}
	return committed, err
}

func TestManualActivityLostAckInspectAndResume(t *testing.T) {
	// Arrange: resolution commits atomically; only acknowledgement is lost.
	ctx := context.Background()
	store := &activityDecisionLostAckStore{ExecutionStore: testutil.NewMemoryExecutionStore(nil)}
	var calls atomic.Int32
	runner := retryActivityRunner(t, store, nil, flowy.ActivityRetryPolicy{}, flowy.ActivityAmbiguous, &calls)
	failed, err := runner.Start(ctx, "run", durableTestState{})
	if failed == nil || !errors.Is(err, flowy.ErrActivityUnknown) {
		t.Fatalf("start=%+v err=%v", failed, err)
	}
	resolution := manualResolution(loadOnlyActivity(t, store), flowy.ActivityResolveComplete)
	resolution.Outcome = []byte("confirmed")
	// Act: lost ACK recovery reads the head and decision instead of submitting a new identity.
	_, err = runner.ResolveActivity(ctx, failed.ResumeToken, resolution)
	if !errors.Is(err, errActivityDecisionAck) {
		t.Fatalf("ack=%v", err)
	}
	head, err := store.LoadExecution(ctx, "run")
	if err != nil {
		t.Fatal(err)
	}
	record := loadOnlyActivity(t, store)
	if len(record.Resolutions) != 1 || record.Resolutions[0].DecisionID != resolution.DecisionID ||
		record.State != flowy.ActivityCompleted || !bytes.Equal(record.Outcome, resolution.Outcome) {
		t.Fatalf("record=%+v", record)
	}
	token := flowy.ResumeToken{ThreadID: head.ExecutionID, SnapshotRevision: head.Revision}
	_, duplicateErr := runner.ResolveActivity(ctx, token, resolution)
	result, resumeErr := runner.Resume(ctx, token)
	// Assert: duplicate is rejected, inspected token resumes without dispatching again.
	if !errors.Is(duplicateErr, flowy.ErrActivityConflict) || resumeErr != nil || result == nil || calls.Load() != 1 {
		t.Fatalf("duplicate=%v resume=%+v err=%v calls=%d", duplicateErr, result, resumeErr, calls.Load())
	}
}
