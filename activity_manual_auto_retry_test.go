package flowy_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/skosovsky/flowy"
	"github.com/skosovsky/flowy/testutil"
)

func TestManualRetryThenAutomaticRetryRetainsDecisionWithoutManualOrigin(t *testing.T) {
	t.Parallel()
	// Arrange.
	ctx := context.Background()
	store := testutil.NewMemoryExecutionStore(nil)
	clock := &testExecutionClock{}
	now := time.Date(2026, 10, 4, 0, 0, 0, 0, time.UTC)
	clock.set(now)
	var calls atomic.Int32
	ambiguous, retryable := errors.New("ambiguous"), errors.New("retryable")
	policy := flowy.ActivityRetryPolicy{
		Label:             "bounded",
		MaxAttempts:       3,
		Delay:             time.Hour,
		SafeRetryContract: "idempotent",
	}
	request := flowy.ActivityRequest{Key: "operation", Implementation: "host", Input: []byte("input"), Retry: policy,
		Classify: func(err error) flowy.ActivityFailureClass {
			if errors.Is(err, ambiguous) {
				return flowy.ActivityAmbiguous
			}
			return flowy.ActivityRetryable
		},
		Dispatch: func(context.Context, flowy.ActivityInvocation) ([]byte, error) {
			switch calls.Add(1) {
			case 1:
				return nil, ambiguous
			case 2:
				return nil, retryable
			default:
				return []byte("done"), nil
			}
		}}
	options := flowy.DurableOptions{Owner: "worker", LeaseTTL: time.Minute, Clock: clock}
	bind := func() *flowy.DurableRunner[durableTestState, flowy.NoEffect] {
		return activityReferenceRunnerOptions(t, store, "current", "node", request, options)
	}
	failed, err := bind().Start(ctx, "run", durableTestState{})
	if !errors.Is(err, flowy.ErrActivityUnknown) || failed == nil {
		t.Fatalf("seed: %v", err)
	}
	resolution := manualResolution(loadOnlyActivity(t, store), flowy.ActivityResolveRetry)
	resolution.SafeRetryContract = policy.SafeRetryContract
	token, err := bind().ResolveActivity(ctx, failed.ResumeToken, resolution)
	if err != nil {
		t.Fatal(err)
	}
	// Act: the manually authorized attempt fails definitively retryable, then automatic retry succeeds.
	clock.set(now.Add(time.Hour))
	second, secondErr := bind().Resume(ctx, token)
	prepared := loadOnlyActivity(t, store)
	if !errors.Is(secondErr, flowy.ErrActivityRetryPending) || second == nil || prepared.Origin != "" ||
		len(prepared.Resolutions) != 1 || !prepared.NextAttemptAt.Equal(now.Add(2*time.Hour)) {
		t.Fatalf("stale manual outcome: %v %+v", secondErr, prepared)
	}
	clock.set(now.Add(2 * time.Hour))
	_, finalErr := bind().Resume(ctx, second.ResumeToken)
	// Assert: unknown and failed history persist with one operator decision, not a fabricated manual completion.
	completed := loadOnlyActivity(t, store)
	if finalErr != nil || calls.Load() != 3 || completed.Origin != flowy.ActivityLive || len(completed.Attempts) != 3 ||
		completed.Attempts[0].State != flowy.ActivityUnknown || completed.Attempts[1].State != flowy.ActivityFailed ||
		len(completed.Resolutions) != 1 || completed.Resolutions[0].Attempt != 1 {
		t.Fatalf("manual/auto retry history lost: %v %+v calls=%d", finalErr, completed, calls.Load())
	}
}
