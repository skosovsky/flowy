package flowy_test

import (
	"bytes"
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/skosovsky/flowy"
	"github.com/skosovsky/flowy/checkpoint"
	"github.com/skosovsky/flowy/testutil"
)

type resolutionTakeoverStore struct {
	flowy.ExecutionStore

	takeover func(context.Context, flowy.ExecutionLease) error
}

func (s *resolutionTakeoverStore) CommitExecution(
	ctx context.Context,
	revision uint64,
	lease flowy.ExecutionLease,
	envelope flowy.ExecutionEnvelope,
) (flowy.ExecutionEnvelope, error) {
	if bytes.Contains(envelope.JournalPayload, []byte(`"resolutions"`)) {
		if err := s.takeover(ctx, lease); err != nil {
			return flowy.ExecutionEnvelope{}, err
		}
	}
	return s.ExecutionStore.CommitExecution(ctx, revision, lease, envelope)
}

func TestManualActivityLostLeaseCannotCommitDecision(t *testing.T) {
	t.Parallel()
	// Arrange: storage clock and same-owner takeover are controlled at commit boundary.
	ctx := context.Background()
	clock := &testExecutionClock{}
	now := time.Date(2026, time.October, 4, 0, 0, 0, 0, time.UTC)
	clock.set(now)
	base := testutil.NewMemoryExecutionStore(clock.Now)
	var successor flowy.ExecutionLease
	store := &resolutionTakeoverStore{
		ExecutionStore: base,
		takeover: func(ctx context.Context, lease flowy.ExecutionLease) error {
			clock.set(now.Add(time.Minute))
			var err error
			successor, err = base.AcquireExecution(ctx, lease.ExecutionID, lease.Owner, time.Minute)
			return err
		},
	}
	var calls atomic.Int32
	runner := retryActivityRunner(t, store, nil, flowy.ActivityRetryPolicy{}, flowy.ActivityAmbiguous, &calls)
	failed, err := runner.Start(ctx, "run", durableTestState{})
	if failed == nil || !errors.Is(err, flowy.ErrActivityUnknown) {
		t.Fatalf("unknown missing: %+v %v", failed, err)
	}
	resolution := manualResolution(loadOnlyActivity(t, store), flowy.ActivityResolveComplete)
	// Act.
	_, resolveErr := runner.ResolveActivity(ctx, failed.ResumeToken, resolution)
	// Assert: OCC alone would have accepted; the live successor fence rejects the stale decision.
	latest, loadErr := base.LoadExecution(ctx, "run")
	if !errors.Is(resolveErr, flowy.ErrLeaseLost) || loadErr != nil ||
		latest.Revision != failed.ResumeToken.SnapshotRevision ||
		calls.Load() != 1 ||
		len(loadOnlyActivity(t, base).Resolutions) != 0 {
		t.Fatalf("stale decision committed: %v latest=%+v load=%v", resolveErr, latest, loadErr)
	}
	if releaseErr := base.ReleaseExecution(ctx, successor); releaseErr != nil {
		t.Fatal(releaseErr)
	}
}

func TestManualActivityCommitFailureLeavesNoDecision(t *testing.T) {
	t.Parallel()
	// Arrange: initial/intent/running/unknown commits succeed; resolution commit fails.
	ctx := context.Background()
	base := testutil.NewMemoryExecutionStore(nil)
	store := &faultExecutionStore{ExecutionStore: base, failAt: 5}
	var calls atomic.Int32
	runner := retryActivityRunner(t, store, nil, flowy.ActivityRetryPolicy{}, flowy.ActivityAmbiguous, &calls)
	failed, err := runner.Start(ctx, "run", durableTestState{})
	if failed == nil || !errors.Is(err, flowy.ErrActivityUnknown) {
		t.Fatalf("unknown missing: %+v %v", failed, err)
	}
	// Act.
	_, resolveErr := runner.ResolveActivity(
		ctx,
		failed.ResumeToken,
		manualResolution(loadOnlyActivity(t, store), flowy.ActivityResolveComplete),
	)
	// Assert.
	latest, loadErr := base.LoadExecution(ctx, "run")
	if !errors.Is(resolveErr, errInjectedCommit) || loadErr != nil ||
		latest.Revision != failed.ResumeToken.SnapshotRevision ||
		calls.Load() != 1 ||
		len(loadOnlyActivity(t, base).Resolutions) != 0 {
		t.Fatalf("failed decision became visible: %v latest=%+v load=%v", resolveErr, latest, loadErr)
	}
	assertRetryWorkerReleased(t, base)
}

func manualResolution(record flowy.ActivityRecord, action flowy.ActivityResolutionAction) flowy.ActivityResolution {
	return flowy.ActivityResolution{
		Identity:       record.Identity,
		InputDigest:    record.InputDigest,
		Implementation: record.Implementation,
		DecisionID:     "operator-decision",
		Action:         action,
		Reason:         "operator confirmed downstream evidence",
		Evidence:       "host-evidence-reference",
	}
}

func TestManualActivityCompletionDoesNotRedispatch(t *testing.T) {
	t.Parallel()
	// Arrange: remote write succeeded, but its outcome could not be committed.
	ctx := context.Background()
	base := testutil.NewMemoryExecutionStore(nil)
	store := &faultExecutionStore{ExecutionStore: base, failAt: 4}
	var calls atomic.Int32
	runner := activityTestRunner(t, store, &calls, false)
	failed, startErr := runner.Start(ctx, "run", durableTestState{})
	if failed == nil || !errors.Is(startErr, flowy.ErrActivityUnknown) {
		t.Fatalf("missing unknown: %+v %v", failed, startErr)
	}
	original := loadOnlyActivity(t, store)
	resolution := manualResolution(original, flowy.ActivityResolveComplete)
	resolution.Outcome = []byte("confirmed remote result")
	// Act.
	token, err := runner.ResolveActivity(ctx, failed.ResumeToken, resolution)
	if err != nil {
		t.Fatal(err)
	}
	resolved := loadOnlyActivity(t, store)
	// Assert: only the decision commits; old attempt remains unknown, not successful.
	if calls.Load() != 1 || resolved.State != flowy.ActivityCompleted || resolved.Origin != flowy.ActivityManual ||
		string(resolved.Outcome) != string(resolution.Outcome) ||
		len(resolved.Resolutions) != 1 ||
		resolved.Resolutions[0].SourceRevision != failed.ResumeToken.SnapshotRevision ||
		resolved.Resolutions[0].PriorState != flowy.ActivityRunning ||
		resolved.Attempts[0].State != flowy.ActivityUnknown {
		t.Fatalf("invalid manual outcome: %+v calls=%d", resolved, calls.Load())
	}
	if _, staleErr := runner.ResolveActivity(
		ctx,
		failed.ResumeToken,
		resolution,
	); !errors.Is(
		staleErr,
		flowy.ErrConcurrencyConflict,
	) {
		t.Fatalf("stale decision accepted: %v", staleErr)
	}
	if _, duplicateErr := runner.ResolveActivity(
		ctx,
		token,
		resolution,
	); !errors.Is(
		duplicateErr,
		flowy.ErrActivityConflict,
	) {
		t.Fatalf("duplicate decision accepted: %v", duplicateErr)
	}
	resumed, err := activityTestRunner(t, store, &calls, false).Resume(ctx, token)
	if err != nil || resumed.State.Value != 1 || calls.Load() != 1 {
		t.Fatalf("manual completion redispatched: %+v %v calls=%d", resumed, err, calls.Load())
	}
	assertRetryWorkerReleased(t, base)
}

func TestManualActivityRetryPreservesDeadlineAndAttempts(t *testing.T) {
	t.Parallel()
	// Arrange: ambiguity is not proof of absence, even with a named safe policy.
	ctx := context.Background()
	clock := &testExecutionClock{}
	now := time.Date(2026, time.October, 4, 0, 0, 0, 0, time.UTC)
	clock.set(now)
	store := testutil.NewMemoryExecutionStore(nil)
	policy := flowy.ActivityRetryPolicy{
		Label:       "safe-bounded",
		MaxAttempts: 2,
		Schedule: flowy.ActivityRetrySchedule{
			Kind:         flowy.ActivityRetryFixed,
			InitialDelay: time.Hour,
			MaxDelay:     time.Hour,
		},
		SafeRetryContract: "host-idempotent",
	}
	var calls atomic.Int32
	runner := retryActivityRunner(t, store, clock, policy, flowy.ActivityAmbiguous, &calls)
	failed, startErr := runner.Start(ctx, "run", durableTestState{})
	if failed == nil || !errors.Is(startErr, flowy.ErrActivityUnknown) {
		t.Fatalf("missing unknown: %+v %v", failed, startErr)
	}
	resolution := manualResolution(loadOnlyActivity(t, store), flowy.ActivityResolveRetry)
	resolution.SafeRetryContract = policy.SafeRetryContract
	// Act.
	token, err := runner.ResolveActivity(ctx, failed.ResumeToken, resolution)
	if err != nil {
		t.Fatal(err)
	}
	before, pendingErr := retryActivityRunner(
		t,
		store,
		clock,
		policy,
		flowy.ActivityAmbiguous,
		&calls,
	).Resume(ctx, token)
	// Assert: resolution and early resume do not dispatch or reset the attempt.
	prepared := loadOnlyActivity(t, store)
	if before == nil || !errors.Is(pendingErr, flowy.ErrActivityRetryPending) || calls.Load() != 1 ||
		len(prepared.Attempts) != 1 ||
		prepared.Attempts[0].State != flowy.ActivityUnknown ||
		!prepared.NextAttemptAt.Equal(now.Add(time.Hour)) ||
		len(prepared.Resolutions) != 1 {
		t.Fatalf("unsafe manual retry: %+v err=%v calls=%d", prepared, pendingErr, calls.Load())
	}
	assertRetryWorkerReleased(t, store)
	clock.set(now.Add(time.Hour))
	resumed, err := retryActivityRunner(t, store, clock, policy, flowy.ActivityAmbiguous, &calls).Resume(ctx, token)
	after := loadOnlyActivity(t, store)
	if err != nil || resumed.State.Value != 1 || calls.Load() != 2 || len(after.Attempts) != 2 ||
		after.Attempts[0].State != flowy.ActivityUnknown ||
		after.Attempts[1].State != flowy.ActivityCompleted ||
		len(after.Resolutions) != 1 {
		t.Fatalf("manual retry recovery: %+v %v calls=%d", after, err, calls.Load())
	}
}

func TestManualActivityDefinitiveFailureDoesNotDispatch(t *testing.T) {
	t.Parallel()
	// Arrange.
	ctx := context.Background()
	store := testutil.NewMemoryExecutionStore(nil)
	var calls atomic.Int32
	runner := retryActivityRunner(t, store, nil, flowy.ActivityRetryPolicy{}, flowy.ActivityAmbiguous, &calls)
	failed, err := runner.Start(ctx, "run", durableTestState{})
	if failed == nil || !errors.Is(err, flowy.ErrActivityUnknown) {
		t.Fatalf("missing unknown: %+v %v", failed, err)
	}
	// Act.
	token, err := runner.ResolveActivity(
		ctx,
		failed.ResumeToken,
		manualResolution(loadOnlyActivity(t, store), flowy.ActivityResolveFail),
	)
	if err != nil {
		t.Fatal(err)
	}
	_, resumeErr := runner.Resume(ctx, token)
	// Assert.
	resolved := loadOnlyActivity(t, store)
	if !errors.Is(resumeErr, flowy.ErrActivityFailed) || calls.Load() != 1 || resolved.State != flowy.ActivityFailed ||
		resolved.Classification != flowy.ActivityNonRetryable ||
		resolved.Origin != flowy.ActivityManual ||
		resolved.Attempts[0].State != flowy.ActivityUnknown {
		t.Fatalf("manual failure redispatched: %+v %v calls=%d", resolved, resumeErr, calls.Load())
	}
}

func TestManualActivityInvalidDecisionLeavesHistoryUnchanged(t *testing.T) {
	t.Parallel()
	for _, scenario := range []string{"identity", "digest", "implementation", "decision", "evidence", "action", "unsafe-retry"} {
		t.Run(scenario, func(t *testing.T) {
			t.Parallel()
			// Arrange.
			ctx := context.Background()
			store := testutil.NewMemoryExecutionStore(nil)
			var calls atomic.Int32
			runner := retryActivityRunner(t, store, nil, flowy.ActivityRetryPolicy{}, flowy.ActivityAmbiguous, &calls)
			failed, err := runner.Start(ctx, "run", durableTestState{})
			if failed == nil || !errors.Is(err, flowy.ErrActivityUnknown) {
				t.Fatalf("missing unknown: %+v %v", failed, err)
			}
			resolution := invalidManualResolution(loadOnlyActivity(t, store), scenario)
			// Act.
			_, resolveErr := runner.ResolveActivity(ctx, failed.ResumeToken, resolution)
			// Assert.
			latest, loadErr := store.LoadExecution(ctx, "run")
			if !errors.Is(resolveErr, flowy.ErrActivityConflict) || loadErr != nil ||
				latest.Revision != failed.ResumeToken.SnapshotRevision ||
				calls.Load() != 1 ||
				len(loadOnlyActivity(t, store).Resolutions) != 0 {
				t.Fatalf("invalid decision mutated source: %v latest=%+v load=%v", resolveErr, latest, loadErr)
			}
		})
	}
}

func invalidManualResolution(record flowy.ActivityRecord, scenario string) flowy.ActivityResolution {
	resolution := manualResolution(record, flowy.ActivityResolveComplete)
	switch scenario {
	case "identity":
		resolution.Identity = "absent"
	case "digest":
		resolution.InputDigest = "changed"
	case "implementation":
		resolution.Implementation = "changed"
	case "decision":
		resolution.DecisionID = ""
	case "evidence":
		resolution.Evidence = ""
	case "action":
		resolution.Action = "invalid"
	case "unsafe-retry":
		resolution.Action = flowy.ActivityResolveRetry
	}
	return resolution
}

func TestManualActivityRetryRejectsWrongContractAndExhaustion(t *testing.T) {
	t.Parallel()
	for _, scenario := range []string{"wrong-contract", "exhausted"} {
		t.Run(scenario, func(t *testing.T) {
			t.Parallel()
			// Arrange: every unknown attempt remains counted against the original limit.
			ctx := context.Background()
			store := testutil.NewMemoryExecutionStore(nil)
			policy := flowy.ActivityRetryPolicy{
				Schedule:          flowy.ActivityRetrySchedule{Kind: flowy.ActivityRetryFixed},
				Label:             "safe",
				MaxAttempts:       2,
				SafeRetryContract: "host-idempotent",
			}
			if scenario == "exhausted" {
				policy.MaxAttempts = 1
			}
			var calls atomic.Int32
			runner := retryActivityRunner(t, store, nil, policy, flowy.ActivityAmbiguous, &calls)
			failed, err := runner.Start(ctx, "run", durableTestState{})
			if failed == nil || !errors.Is(err, flowy.ErrActivityUnknown) {
				t.Fatalf("unknown missing: %+v %v", failed, err)
			}
			resolution := manualResolution(loadOnlyActivity(t, store), flowy.ActivityResolveRetry)
			resolution.SafeRetryContract = policy.SafeRetryContract
			if scenario == "wrong-contract" {
				resolution.SafeRetryContract = "different"
			}
			// Act.
			_, resolveErr := runner.ResolveActivity(ctx, failed.ResumeToken, resolution)
			// Assert: authorization cannot silently replace/reset the persisted policy.
			latest, loadErr := store.LoadExecution(ctx, "run")
			if !errors.Is(resolveErr, flowy.ErrActivityConflict) || loadErr != nil ||
				latest.Revision != failed.ResumeToken.SnapshotRevision ||
				len(loadOnlyActivity(t, store).Attempts) != 1 ||
				calls.Load() != 1 {
				t.Fatalf("unsafe policy bypass: %v source=%+v load=%v", resolveErr, latest, loadErr)
			}
		})
	}
}

func TestManualActivityResolutionDoesNotInvokeCodecOrNode(t *testing.T) {
	t.Parallel()
	// Arrange: a runner with a deliberately unusable state codec may resolve raw metadata.
	ctx := context.Background()
	store := testutil.NewMemoryExecutionStore(nil)
	var dispatches, probes atomic.Int32
	failed, err := retryActivityRunner(
		t,
		store,
		nil,
		flowy.ActivityRetryPolicy{},
		flowy.ActivityAmbiguous,
		&dispatches,
	).Start(ctx, "run", durableTestState{})
	if failed == nil || !errors.Is(err, flowy.ErrActivityUnknown) {
		t.Fatalf("unknown missing: %+v %v", failed, err)
	}
	builder := flowy.NewGraph[durableTestState, flowy.NoEffect](
		func(_, update durableTestState) durableTestState { return update },
	)
	builder.AddNode("write", func(_ context.Context, state durableTestState) (durableTestState, flowy.Directive, error) {
		probes.Add(100)
		return state, flowy.End(), nil
	}).
		AllowNoOutgoingRoute("write").
		SetEntryPoint("write")
	graph, err := builder.Compile()
	if err != nil {
		t.Fatal(err)
	}
	runner, err := flowy.NewDurableRunner(
		graph,
		store,
		durableDescriptor("current"),
		failingDecode{calls: &probes},
		checkpoint.JSONSerializer[[]flowy.NoEffect]{},
		flowy.DurableOptions{Owner: "operator", LeaseTTL: time.Minute},
	)
	if err != nil {
		t.Fatal(err)
	}
	// Act.
	token, err := runner.ResolveActivity(
		ctx,
		failed.ResumeToken,
		manualResolution(loadOnlyActivity(t, store), flowy.ActivityResolveComplete),
	)
	// Assert.
	if err != nil || token.SnapshotRevision != failed.ResumeToken.SnapshotRevision+1 || probes.Load() != 0 ||
		dispatches.Load() != 1 {
		t.Fatalf(
			"resolution invoked runtime/codec: %+v %v probes=%d dispatches=%d",
			token,
			err,
			probes.Load(),
			dispatches.Load(),
		)
	}
}
