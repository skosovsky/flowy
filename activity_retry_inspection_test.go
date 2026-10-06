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

func TestPendingRetryInspectionDoesNotDispatchOrMutate(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name  string
		class flowy.ActivityFailureClass
		max   int
		want  int
	}{
		{name: "scheduled", class: flowy.ActivityRetryable, max: 2, want: 1},
		{name: "unknown", class: flowy.ActivityAmbiguous, max: 2, want: 0},
		{name: "exhausted", class: flowy.ActivityRetryable, max: 1, want: 0},
		{name: "definitive", class: flowy.ActivityNonRetryable, max: 2, want: 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			// Arrange: only a safely classified, unexhausted pending retry is discoverable.
			ctx := context.Background()
			store := testutil.NewMemoryExecutionStore(nil)
			clock := &testExecutionClock{}
			clock.set(time.Date(2026, 10, 4, 0, 0, 0, 0, time.UTC))
			var calls atomic.Int32
			policy := flowy.ActivityRetryPolicy{Label: "inspection", MaxAttempts: tc.max,
				Delay: time.Hour, SafeRetryContract: "host-idempotent-write"}
			_, _ = retryActivityRunner(t, store, clock, policy, tc.class, &calls).Start(ctx, "run", durableTestState{})
			before, err := store.LoadExecution(ctx, "run")
			if err != nil {
				t.Fatal(err)
			}
			// Act.
			candidates, inspectErr := flowy.InspectPendingActivityRetries(before)
			// Assert: metadata inspection has no execution or persistence effects.
			if inspectErr != nil || len(candidates) != tc.want || calls.Load() != 1 {
				t.Fatalf("unexpected candidates: %+v err=%v calls=%d", candidates, inspectErr, calls.Load())
			}
			if len(candidates) != 0 {
				assertPendingInspectionAddress(t, candidates[0], before, policy, clock.Now().Add(time.Hour))
				candidates[0].ResumeToken.ThreadID = "host-mutated"
			}
			after, loadErr := store.LoadExecution(ctx, "run")
			if loadErr != nil || after.Digest != before.Digest || after.Revision != before.Revision {
				t.Fatalf("inspection mutated execution: %+v err=%v", after, loadErr)
			}
		})
	}
}

func assertPendingInspectionAddress(t *testing.T, candidate flowy.PendingActivityRetry,
	envelope flowy.ExecutionEnvelope, policy flowy.ActivityRetryPolicy, deadline time.Time,
) {
	t.Helper()
	if candidate.Identity == "" || candidate.Node != envelope.Progress.ExecutionPointer ||
		candidate.Activation != envelope.Activation || candidate.Attempts != 1 || candidate.Policy != policy ||
		candidate.Descriptor != envelope.Descriptor || !candidate.Deadline.Equal(deadline) ||
		candidate.ResumeToken.ThreadID != envelope.ExecutionID || candidate.ResumeToken.SnapshotRevision != envelope.Revision {
		t.Fatalf("inspection lost persisted retry address: %+v", candidate)
	}
}

func TestPendingRetryInspectionRejectsCorruptEnvelope(t *testing.T) {
	t.Parallel()
	// Arrange.
	store := testutil.NewMemoryExecutionStore(nil)
	clock := &testExecutionClock{}
	clock.set(time.Now().UTC())
	var calls atomic.Int32
	policy := flowy.ActivityRetryPolicy{Label: "inspection", MaxAttempts: 2,
		Delay: time.Hour, SafeRetryContract: "host-idempotent-write"}
	_, _ = retryActivityRunner(t, store, clock, policy, flowy.ActivityRetryable, &calls).
		Start(context.Background(), "run", durableTestState{})
	envelope, err := store.LoadExecution(context.Background(), "run")
	if err != nil {
		t.Fatal(err)
	}
	// Act: changing a sealed header cannot fabricate a valid recovery observation.
	envelope.RuntimeProfile = &flowy.WaitCapabilityProfile{}
	_, inspectErr := flowy.InspectPendingActivityRetries(envelope)
	// Assert.
	if !errors.Is(inspectErr, flowy.ErrExecutionCorrupt) {
		t.Fatalf("unsealed mutation accepted: %v", inspectErr)
	}
	envelope, err = flowy.SealExecutionEnvelope(envelope)
	if err != nil {
		t.Fatal(err)
	}
	_, inspectErr = flowy.InspectPendingActivityRetries(envelope)
	if !errors.Is(inspectErr, flowy.ErrExecutionCorrupt) || calls.Load() != 1 {
		t.Fatalf("resealed invalid profile accepted: %v calls=%d", inspectErr, calls.Load())
	}
}
