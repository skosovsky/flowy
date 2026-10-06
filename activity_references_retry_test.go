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

func TestActivityMigratedRetryPreservesIdentityAttemptsAndDeadline(t *testing.T) {
	t.Parallel()
	// Arrange.
	ctx := context.Background()
	store := testutil.NewMemoryExecutionStore(nil)
	clock := &testExecutionClock{}
	now := time.Date(2026, 10, 4, 0, 0, 0, 0, time.UTC)
	clock.set(now)
	var calls atomic.Int32
	var identity string
	request := flowy.ActivityRequest{
		Key:            "operation",
		Implementation: "host",
		Input:          []byte("input"),
		Retry: flowy.ActivityRetryPolicy{
			Label:       "bounded",
			MaxAttempts: 2,
			Schedule: flowy.ActivityRetrySchedule{
				Kind:         flowy.ActivityRetryFixed,
				InitialDelay: time.Hour,
				MaxDelay:     time.Hour,
			},
			SafeRetryContract: "idempotent",
		},
		Classify: func(error) flowy.ActivityFailureDecision {
			return flowy.ActivityFailureDecision{Class: flowy.ActivityRetryable}
		},
		Dispatch: func(_ context.Context, invocation flowy.ActivityInvocation) ([]byte, error) {
			if calls.Add(1) == 1 {
				identity = invocation.Identity
				return nil, errors.New("retryable failure")
			}
			if invocation.Identity != identity || invocation.Attempt != 2 {
				return nil, errors.New("retry address reset")
			}
			return []byte("done"), nil
		},
	}
	options := flowy.DurableOptions{Owner: "worker", LeaseTTL: time.Minute, Clock: clock}
	old := activityReferenceRunnerOptions(t, store, "old", "old-node", request, options)
	if _, err := old.Start(ctx, "run", durableTestState{}); !errors.Is(err, flowy.ErrActivityRetryPending) {
		t.Fatal(err)
	}
	source, err := store.LoadExecution(ctx, "run")
	if err != nil {
		t.Fatal(err)
	}
	options.Migrations = []flowy.ExecutionMigration{
		{ID: "move", Source: source.Descriptor, Target: durableDescriptor("new"),
			Transform: func(state flowy.MigrationState) (flowy.MigrationState, error) {
				state.ExecutionPointer = "new-node"
				state.JournalReferences = map[string]string{"operation": identity}
				return state, nil
			}},
	}
	// Act: migration commits, but the persisted deadline still prevents early dispatch.
	target := activityReferenceRunnerOptions(t, store, "new", "new-node", request, options)
	_, earlyErr := target.Resume(ctx, flowy.ResumeToken{ThreadID: "run", SnapshotRevision: source.Revision})
	prepared := loadOnlyActivity(t, store)
	if !errors.Is(earlyErr, flowy.ErrActivityRetryPending) || calls.Load() != 1 || len(prepared.Attempts) != 1 ||
		prepared.Identity != identity || !prepared.NextAttemptAt.Equal(now.Add(time.Hour)) {
		t.Fatalf("migration reset retry: %v %+v", earlyErr, prepared)
	}
	latest, err := store.LoadExecution(ctx, "run")
	if err != nil {
		t.Fatal(err)
	}
	clock.set(now.Add(time.Hour))
	options.Migrations = nil
	restarted := activityReferenceRunnerOptions(t, store, "new", "new-node", request, options)
	_, resumeErr := restarted.Resume(ctx, flowy.ResumeToken{ThreadID: "run", SnapshotRevision: latest.Revision})
	// Assert: the old failed attempt and source address remain immutable.
	completed := loadOnlyActivity(t, store)
	if resumeErr != nil || calls.Load() != 2 || completed.Identity != identity || completed.Node != "old-node" ||
		len(
			completed.Attempts,
		) != 2 || completed.Attempts[0].State != flowy.ActivityFailed || completed.State != flowy.ActivityCompleted {
		t.Fatalf("migrated retry lost history: %v %+v calls=%d", resumeErr, completed, calls.Load())
	}
}
