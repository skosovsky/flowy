package flowy_test

import (
	"bytes"
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/skosovsky/flowy"
	"github.com/skosovsky/flowy/testutil"
)

type task24ScheduleLostAckStore struct{ flowy.ExecutionStore }

func (s task24ScheduleLostAckStore) CommitExecution(
	ctx context.Context,
	rev uint64,
	lease flowy.ExecutionLease,
	e flowy.ExecutionEnvelope,
) (flowy.ExecutionEnvelope, error) {
	committed, err := s.ExecutionStore.CommitExecution(ctx, rev, lease, e)
	if err == nil && bytes.Contains(e.JournalPayload, []byte(`"retry_schedule"`)) {
		return flowy.ExecutionEnvelope{}, errInjectedCommit
	}
	return committed, err
}
func TestActivityScheduleLostAckUsesPersistedChoice(t *testing.T) {
	// Arrange.
	ctx := context.Background()
	base := testutil.NewMemoryExecutionStore(nil)
	clock := &testExecutionClock{}
	at := time.Now().UTC()
	clock.set(at)
	var calls, classifiers, samples atomic.Int32
	policy := task24RetryPolicy()
	decision := flowy.ActivityFailureDecision{Class: flowy.ActivityRetryable}
	first, err := task24ScheduleRunner(
		t,
		task24ScheduleLostAckStore{ExecutionStore: base},
		clock,
		policy,
		decision,
		func() uint64 { samples.Add(1); return uint64(time.Second) },
		&calls,
		&classifiers,
	).Start(ctx, "run", 0)
	if first == nil || err == nil {
		t.Fatalf("lost ack missing: %+v/%v", first, err)
	}
	head, err := base.LoadExecution(ctx, "run")
	if err != nil {
		t.Fatal(err)
	}
	prepared := loadOnlyActivity(t, base)
	// Act: authoritative head carries schedule even though its response was lost.
	recovered := task24ScheduleRunner(
		t,
		base,
		clock,
		policy,
		decision,
		func() uint64 { t.Fatal("rerolled after lost ack"); return 0 },
		&calls,
		&classifiers,
	)
	token := flowy.ResumeToken{ThreadID: "run", SnapshotRevision: head.Revision}
	_, pendingErr := recovered.Resume(ctx, token)
	clock.set(prepared.NextAttemptAt)
	result, resumeErr := recovered.Resume(ctx, token)
	// Assert.
	if !errors.Is(pendingErr, flowy.ErrActivityRetryPending) || resumeErr != nil ||
		result.Status != flowy.RunStatusCompleted ||
		calls.Load() != 2 ||
		classifiers.Load() != 1 ||
		samples.Load() != 1 ||
		prepared.State != flowy.ActivityPrepared ||
		prepared.Attempts[0].RetrySchedule == nil {
		t.Fatalf(
			"pending=%v resume=%v calls=%d classifiers=%d samples=%d record=%+v",
			pendingErr,
			resumeErr,
			calls.Load(),
			classifiers.Load(),
			samples.Load(),
			prepared,
		)
	}
}
