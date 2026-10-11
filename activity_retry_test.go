package flowy_test

import (
	"context"
	"encoding/json"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/skosovsky/flowy"
	"github.com/skosovsky/flowy/checkpoint"
	"github.com/skosovsky/flowy/testutil"
)

type testExecutionClock struct{ nanos atomic.Int64 }

func (c *testExecutionClock) Now() time.Time      { return time.Unix(0, c.nanos.Load()).UTC() }
func (c *testExecutionClock) set(value time.Time) { c.nanos.Store(value.UnixNano()) }

func retryActivityRunner(t *testing.T, store flowy.ExecutionStore, clock flowy.ExecutionClock,
	policy flowy.ActivityRetryPolicy, classification flowy.ActivityFailureClass, calls *atomic.Int32,
) *flowy.DurableRunner[durableTestState, flowy.NoEffect] {
	t.Helper()
	builder := flowy.NewGraph[durableTestState, flowy.NoEffect](
		func(_, update durableTestState) durableTestState { return update },
	)
	builder.AddNode("write", func(ctx context.Context, state durableTestState) (durableTestState, flowy.Directive, error) {
		_, err := flowy.CallActivity(ctx, flowy.ActivityRequest{
			Key:            "write",
			Implementation: "stable",
			Input:          []byte("input"),
			Retry:          policy,
			Classify:       func(error) flowy.ActivityFailureDecision { return flowy.ActivityFailureDecision{Class: classification} },
			Dispatch: func(context.Context, flowy.ActivityInvocation) ([]byte, error) {
				if calls.Add(1) == 1 {
					return nil, errors.New("host-classified dispatch failure")
				}
				return []byte("done"), nil
			},
		})
		if err != nil {
			return state, flowy.Fail("activity"), err
		}
		state.Value++
		return state, flowy.End(), nil
	}).
		AllowNoOutgoingRoute("write").
		SetEntryPoint("write")
	graph, err := builder.Compile()
	if err != nil {
		t.Fatal(err)
	}
	runner, err := flowy.NewDurableRunner(graph, store, durableDescriptor("current"),
		checkpoint.JSONSerializer[durableTestState]{}, checkpoint.JSONSerializer[[]flowy.NoEffect]{},
		flowy.DurableOptions{Owner: "worker", LeaseTTL: time.Minute, Clock: clock})
	if err != nil {
		t.Fatal(err)
	}
	return runner
}

func loadOnlyActivity(t *testing.T, store flowy.ExecutionStore) flowy.ActivityRecord {
	t.Helper()
	envelope, err := store.LoadExecution(context.Background(), "run")
	if err != nil {
		t.Fatal(err)
	}
	var journal map[string]flowy.ActivityRecord
	if err = json.Unmarshal(envelope.JournalPayload, &journal); err != nil {
		t.Fatal(err)
	}
	if len(journal) != 1 {
		t.Fatalf("unexpected journal: %+v", journal)
	}
	for _, record := range journal {
		return record
	}
	t.Fatal("activity missing")
	return flowy.ActivityRecord{}
}

func TestActivityRetryDeadlineSurvivesRestart(t *testing.T) {
	// Arrange.
	ctx := context.Background()
	clock := &testExecutionClock{}
	now := time.Date(2026, time.October, 4, 0, 0, 0, 0, time.UTC)
	clock.set(now)
	store := testutil.NewMemoryExecutionStore(nil)
	policy := flowy.ActivityRetryPolicy{
		Label:       "bounded",
		MaxAttempts: 2,
		Schedule: flowy.ActivityRetrySchedule{
			Kind:         flowy.ActivityRetryFixed,
			InitialDelay: time.Hour,
			MaxDelay:     time.Hour,
		},
		SafeRetryContract: "host-idempotent-write",
	}
	var calls atomic.Int32
	runner := retryActivityRunner(t, store, clock, policy, flowy.ActivityRetryable, &calls)
	// Act: each resume uses a new runner, not an in-memory retry loop.
	failed, startErr := runner.Start(ctx, "run", durableTestState{})
	var pending *flowy.ActivityRetryPendingError
	if !errors.As(startErr, &pending) || failed == nil || !pending.Deadline.Equal(now.Add(time.Hour)) {
		t.Fatalf("deadline not committed: %+v %v", failed, startErr)
	}
	before := loadOnlyActivity(t, store)
	restarted := retryActivityRunner(t, store, clock, policy, flowy.ActivityRetryable, &calls)
	blocked, resumeErr := restarted.Resume(ctx, failed.ResumeToken)
	// Assert: no early dispatch, attempt increment or revision reset.
	if !errors.Is(resumeErr, flowy.ErrActivityRetryPending) || calls.Load() != 1 ||
		blocked.ResumeToken != failed.ResumeToken {
		t.Fatalf("early retry: %+v %v calls=%d", blocked, resumeErr, calls.Load())
	}
	assertRetryWorkerReleased(t, store)
	after := loadOnlyActivity(t, store)
	if len(after.Attempts) != 1 || after.State != flowy.ActivityPrepared ||
		!after.NextAttemptAt.Equal(
			before.NextAttemptAt,
		) || after.Attempts[0].Classification != flowy.ActivityRetryable {
		t.Fatalf("restart reset retry: %+v", after)
	}
	clock.set(pending.Deadline)
	ready := retryActivityRunner(t, store, clock, policy, flowy.ActivityRetryable, &calls)
	result, readyErr := ready.Resume(ctx, blocked.ResumeToken)
	if readyErr != nil || result.State.Value != 1 || calls.Load() != 2 {
		t.Fatalf("due retry failed: %+v %v", result, readyErr)
	}
	completed := loadOnlyActivity(t, store)
	if completed.Identity != before.Identity || len(completed.Attempts) != 2 ||
		completed.State != flowy.ActivityCompleted {
		t.Fatalf("retry created a new activity: %+v", completed)
	}
}

func TestActivityRetryNeverReinterpretsUnknown(t *testing.T) {
	// Arrange: even a safe policy does not automatically retry an unknown result.
	ctx := context.Background()
	store := testutil.NewMemoryExecutionStore(nil)
	clock := &testExecutionClock{}
	clock.set(time.Now())
	policy := flowy.ActivityRetryPolicy{
		Schedule:          flowy.ActivityRetrySchedule{Kind: flowy.ActivityRetryFixed},
		Label:             "bounded",
		MaxAttempts:       2,
		SafeRetryContract: "host-idempotent-write",
	}
	var calls atomic.Int32
	runner := retryActivityRunner(t, store, clock, policy, flowy.ActivityAmbiguous, &calls)
	// Act.
	failed, err := runner.Start(ctx, "run", durableTestState{})
	if !errors.Is(err, flowy.ErrActivityUnknown) || failed == nil {
		t.Fatalf("unknown not persisted: %+v %v", failed, err)
	}
	restarted := retryActivityRunner(t, store, clock, policy, flowy.ActivityRetryable, &calls)
	_, resumeErr := restarted.Resume(ctx, failed.ResumeToken)
	// Assert: changing the live classifier never reinterprets an abandoned outcome.
	if !errors.Is(resumeErr, flowy.ErrActivityUnknown) || calls.Load() != 1 {
		t.Fatalf("unknown blindly retried: %v", resumeErr)
	}
}

func assertRetryWorkerReleased(t *testing.T, store flowy.ExecutionStore) {
	t.Helper()
	ctx := context.Background()
	lease, err := store.AcquireExecution(ctx, "run", "other-worker", time.Minute)
	if err != nil {
		t.Fatalf("backoff holds the worker lease: %v", err)
	}
	if releaseErr := store.ReleaseExecution(ctx, lease); releaseErr != nil {
		t.Fatal(releaseErr)
	}
}

func TestActivityRetryAbandonedDispatchRemainsUnknown(t *testing.T) {
	// Arrange: the classifier would allow retry, but its result was never committed.
	ctx := context.Background()
	store := &faultExecutionStore{ExecutionStore: testutil.NewMemoryExecutionStore(nil), failAt: 4}
	clock := &testExecutionClock{}
	clock.set(time.Now())
	policy := flowy.ActivityRetryPolicy{
		Schedule:          flowy.ActivityRetrySchedule{Kind: flowy.ActivityRetryFixed},
		Label:             "bounded",
		MaxAttempts:       2,
		SafeRetryContract: "host-idempotent-write",
	}
	var calls atomic.Int32
	runner := retryActivityRunner(t, store, clock, policy, flowy.ActivityRetryable, &calls)
	// Act: lose the worker after dispatch and restart with the same policy.
	failed, err := runner.Start(ctx, "run", durableTestState{})
	if !errors.Is(err, errInjectedCommit) || failed == nil {
		t.Fatalf("fault not reached: %+v %v", failed, err)
	}
	abandoned := loadOnlyActivity(t, store)
	restarted := retryActivityRunner(t, store, clock, policy, flowy.ActivityRetryable, &calls)
	_, resumeErr := restarted.Resume(ctx, failed.ResumeToken)
	// Assert: an unrecorded classification cannot justify redispatch.
	if abandoned.State != flowy.ActivityRunning || !errors.Is(resumeErr, flowy.ErrActivityUnknown) ||
		calls.Load() != 1 {
		t.Fatalf("abandoned dispatch retried: %+v %v calls=%d", abandoned, resumeErr, calls.Load())
	}
	unknown := loadOnlyActivity(t, store)
	if unknown.State != flowy.ActivityUnknown || unknown.Attempts[0].Classification != flowy.ActivityAmbiguous {
		t.Fatalf("unknown decision not persisted: %+v", unknown)
	}
}

func TestActivityDefinitiveFailureIsSticky(t *testing.T) {
	for _, test := range []struct {
		name           string
		classification flowy.ActivityFailureClass
		attempts       int
		want           error
	}{
		{name: "non retryable", classification: flowy.ActivityNonRetryable, attempts: 2, want: flowy.ErrActivityFailed},
		{name: "attempt limit", classification: flowy.ActivityRetryable, attempts: 1, want: flowy.ErrActivityAttemptsExhausted},
	} {
		t.Run(
			test.name,
			func(t *testing.T) { assertStickyActivityFailure(t, test.classification, test.attempts, test.want) },
		)
	}
}

func assertStickyActivityFailure(t *testing.T, classification flowy.ActivityFailureClass, attempts int, want error) {
	t.Helper()
	// Arrange.
	ctx := context.Background()
	store := testutil.NewMemoryExecutionStore(nil)
	clock := &testExecutionClock{}
	clock.set(time.Now())
	policy := flowy.ActivityRetryPolicy{Schedule: flowy.ActivityRetrySchedule{Kind: flowy.ActivityRetryFixed},
		Label:             "bounded",
		MaxAttempts:       attempts,
		SafeRetryContract: "host-idempotent-write",
	}
	var calls atomic.Int32
	runner := retryActivityRunner(t, store, clock, policy, classification, &calls)
	// Act.
	failed, err := runner.Start(ctx, "run", durableTestState{})
	if !errors.Is(err, want) || failed == nil {
		t.Fatalf("failure not recorded: %+v %v", failed, err)
	}
	restarted := retryActivityRunner(t, store, clock, policy, flowy.ActivityRetryable, &calls)
	_, resumeErr := restarted.Resume(ctx, failed.ResumeToken)
	// Assert: a new classifier/worker does not reset the committed decision or limit.
	if !errors.Is(resumeErr, want) || calls.Load() != 1 {
		t.Fatalf("definitive failure retried: %v calls=%d", resumeErr, calls.Load())
	}
}
