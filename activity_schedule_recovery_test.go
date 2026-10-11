package flowy_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/skosovsky/flowy"
	"github.com/skosovsky/flowy/checkpoint"
	"github.com/skosovsky/flowy/testutil"
)

func task24ScheduleRunner(t *testing.T, store flowy.ExecutionStore, clock flowy.ExecutionClock,
	policy flowy.ActivityRetryPolicy, decision flowy.ActivityFailureDecision, random func() uint64,
	dispatches, classifiers *atomic.Int32) *flowy.DurableRunner[int, flowy.NoEffect] {
	t.Helper()
	b := flowy.NewGraph[int, flowy.NoEffect](func(_, u int) int { return u })
	b.AddNode("work", func(ctx context.Context, s int) (int, flowy.Directive, error) {
		_, err := flowy.CallActivity(ctx, flowy.ActivityRequest{
			Key: "write", Implementation: "stable", Input: []byte("input"), Retry: policy,
			Classify: func(error) flowy.ActivityFailureDecision { classifiers.Add(1); return decision },
			Dispatch: func(context.Context, flowy.ActivityInvocation) ([]byte, error) {
				if dispatches.Add(1) == 1 {
					return nil, errors.New("known failed request")
				}
				return []byte("done"), nil
			},
		})
		if err != nil {
			return s, flowy.Fail("activity"), err
		}
		return s + 1, flowy.End(), nil
	}).SetEntryPoint("work").AllowNoOutgoingRoute("work")
	g, err := b.Compile()
	if err != nil {
		t.Fatal(err)
	}
	r, err := flowy.NewDurableRunner(
		g,
		store,
		durableDescriptor("schedule"),
		checkpoint.JSONSerializer[int]{},
		checkpoint.JSONSerializer[[]flowy.NoEffect]{},
		flowy.DurableOptions{Owner: "worker", LeaseTTL: time.Minute, Clock: clock, RetryRandom: random},
	)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func TestActivityScheduleDeadlineAndJitterSurviveRestart(t *testing.T) {
	// Arrange.
	ctx := context.Background()
	at := time.Date(2026, 10, 6, 0, 0, 0, 0, time.UTC)
	clock := &testExecutionClock{}
	clock.set(at)
	store := testutil.NewMemoryExecutionStore(nil)
	policy := flowy.ActivityRetryPolicy{
		Label:             "exponential-v1",
		MaxAttempts:       3,
		SafeRetryContract: "downstream-key",
		Schedule: flowy.ActivityRetrySchedule{
			Kind:           flowy.ActivityRetryExponential,
			InitialDelay:   10 * time.Second,
			MaxDelay:       time.Minute,
			Multiplier:     2,
			JitterPermille: 500,
			HintLabel:      "adapter-hint-v1",
		},
	}
	decision := flowy.ActivityFailureDecision{Class: flowy.ActivityRetryable, NotBefore: at.Add(20 * time.Second)}
	var dispatches, classifiers, samples atomic.Int32
	random := func() uint64 { samples.Add(1); return uint64(2 * time.Second) }
	// Act: the original worker selects once, restart must use the journal.
	first, err := task24ScheduleRunner(
		t,
		store,
		clock,
		policy,
		decision,
		random,
		&dispatches,
		&classifiers,
	).Start(ctx, "run", 0)
	if !errors.Is(err, flowy.ErrActivityRetryPending) {
		t.Fatalf("first=%+v err=%v", first, err)
	}
	before := loadOnlyActivity(t, store)
	changedRandom := func() uint64 { samples.Add(1); return 999 }
	pending, pendingErr := task24ScheduleRunner(
		t,
		store,
		clock,
		policy,
		decision,
		changedRandom,
		&dispatches,
		&classifiers,
	).Resume(ctx, first.ResumeToken)
	assertRetryWorkerReleased(t, store)
	after := loadOnlyActivity(t, store)
	// Assert.
	chosen := before.Attempts[0].RetrySchedule
	if !errors.Is(pendingErr, flowy.ErrActivityRetryPending) || pending.ResumeToken != first.ResumeToken ||
		dispatches.Load() != 1 ||
		classifiers.Load() != 1 ||
		samples.Load() != 1 ||
		chosen == nil ||
		chosen.BaseDelay != 10*time.Second ||
		chosen.SelectedDelay != 8*time.Second ||
		!chosen.NotBefore.Equal(decision.NotBefore) ||
		!before.NextAttemptAt.Equal(at.Add(20*time.Second)) ||
		!after.NextAttemptAt.Equal(before.NextAttemptAt) {
		t.Fatalf(
			"before=%+v after=%+v pending=%v dispatch=%d classify=%d random=%d",
			before,
			after,
			pendingErr,
			dispatches.Load(),
			classifiers.Load(),
			samples.Load(),
		)
	}
	clock.set(before.NextAttemptAt)
	completed, completeErr := task24ScheduleRunner(
		t,
		store,
		clock,
		policy,
		decision,
		changedRandom,
		&dispatches,
		&classifiers,
	).Resume(ctx, pending.ResumeToken)
	if completeErr != nil || completed.Status != flowy.RunStatusCompleted || completed.State != 1 ||
		dispatches.Load() != 2 ||
		classifiers.Load() != 1 ||
		samples.Load() != 1 {
		t.Fatalf(
			"completed=%+v err=%v dispatch=%d classify=%d random=%d",
			completed,
			completeErr,
			dispatches.Load(),
			classifiers.Load(),
			samples.Load(),
		)
	}
	history := loadOnlyActivity(t, store)
	if len(history.Attempts) != 2 || history.Identity != before.Identity || history.Attempts[0].RetrySchedule == nil ||
		!history.Attempts[0].RetrySchedule.Deadline.Equal(before.NextAttemptAt) {
		t.Fatalf("history reset: %+v", history)
	}
}

func TestActivityScheduleRejectedHintPersistsWithoutRetry(t *testing.T) {
	// Arrange: a valid-time hint is not covered by a compatible hint label.
	at := time.Date(2026, 10, 6, 0, 0, 0, 0, time.UTC)
	clock := &testExecutionClock{}
	clock.set(at)
	store := testutil.NewMemoryExecutionStore(nil)
	policy := flowy.ActivityRetryPolicy{
		Label:             "fixed-v1",
		MaxAttempts:       2,
		SafeRetryContract: "safe",
		Schedule: flowy.ActivityRetrySchedule{
			Kind:         flowy.ActivityRetryFixed,
			InitialDelay: time.Second,
			MaxDelay:     time.Second,
		},
	}
	var dispatches, classifiers atomic.Int32
	decision := flowy.ActivityFailureDecision{Class: flowy.ActivityRetryable, NotBefore: at.Add(time.Hour)}
	// Act.
	first, err := task24ScheduleRunner(
		t,
		store,
		clock,
		policy,
		decision,
		nil,
		&dispatches,
		&classifiers,
	).Start(context.Background(), "run", 0)
	record := loadOnlyActivity(t, store)
	_, replayErr := task24ScheduleRunner(
		t,
		store,
		clock,
		policy,
		decision,
		nil,
		&dispatches,
		&classifiers,
	).Resume(context.Background(), first.ResumeToken)
	// Assert.
	if !errors.Is(err, flowy.ErrActivityScheduleInvalid) || !errors.Is(replayErr, flowy.ErrActivityScheduleInvalid) ||
		dispatches.Load() != 1 ||
		classifiers.Load() != 1 ||
		record.State != flowy.ActivityFailed ||
		!record.NextAttemptAt.IsZero() ||
		record.Attempts[0].RetrySchedule == nil ||
		record.Attempts[0].RetrySchedule.Rejection != "hint_label" {
		t.Fatalf(
			"record=%+v err=%v replay=%v dispatch=%d classify=%d",
			record,
			err,
			replayErr,
			dispatches.Load(),
			classifiers.Load(),
		)
	}
}

func TestActivityScheduleUnknownDoesNotClassifyOrSampleOnReplay(t *testing.T) {
	// Arrange.
	ctx := context.Background()
	at := time.Date(2026, 10, 6, 0, 0, 0, 0, time.UTC)
	clock := &testExecutionClock{}
	clock.set(at)
	store := testutil.NewMemoryExecutionStore(nil)
	policy := flowy.ActivityRetryPolicy{
		Label:             "safe",
		MaxAttempts:       3,
		SafeRetryContract: "idempotent",
		Schedule: flowy.ActivityRetrySchedule{
			Kind:           flowy.ActivityRetryFixed,
			InitialDelay:   time.Second,
			MaxDelay:       time.Second,
			JitterPermille: 1000,
		},
	}
	var dispatches, classifiers, samples atomic.Int32
	random := func() uint64 { samples.Add(1); return 0 }
	// Act: changing the live classifier does not reinterpret a persisted unknown result.
	first, err := task24ScheduleRunner(
		t,
		store,
		clock,
		policy,
		flowy.ActivityFailureDecision{Class: flowy.ActivityAmbiguous},
		random,
		&dispatches,
		&classifiers,
	).Start(ctx, "run", 0)
	if !errors.Is(err, flowy.ErrActivityUnknown) {
		t.Fatal(err)
	}
	_, replayErr := task24ScheduleRunner(
		t,
		store,
		clock,
		policy,
		flowy.ActivityFailureDecision{Class: flowy.ActivityRetryable},
		random,
		&dispatches,
		&classifiers,
	).Resume(ctx, first.ResumeToken)
	record := loadOnlyActivity(t, store)
	// Assert.
	if !errors.Is(replayErr, flowy.ErrActivityUnknown) || dispatches.Load() != 1 || classifiers.Load() != 1 ||
		samples.Load() != 0 ||
		record.State != flowy.ActivityUnknown ||
		!record.NextAttemptAt.IsZero() ||
		record.Attempts[0].RetrySchedule != nil {
		t.Fatalf(
			"record=%+v err=%v dispatch=%d classify=%d random=%d",
			record,
			replayErr,
			dispatches.Load(),
			classifiers.Load(),
			samples.Load(),
		)
	}
}
