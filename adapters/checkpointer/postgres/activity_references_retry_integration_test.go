//go:build integration

package postgres

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/skosovsky/flowy"
)

func TestIntegrationActivityMigratedRetryPersistentRestart(t *testing.T) {
	// Arrange: independent pools and explicit runtime clocks around a persisted deadline.
	ctx, pool := racePool(t)
	if _, err := pool.Exec(ctx, ExecutionSchemaSQL()); err != nil {
		t.Fatal(err)
	}
	id := testThreadID(t)
	now := time.Date(2026, 10, 4, 0, 0, 0, 0, time.UTC)
	var calls atomic.Int32
	identity := ""
	request := flowy.ActivityRequest{
		Key:            "operation",
		Implementation: "host",
		Input:          []byte("input"),
		Retry: flowy.ActivityRetryPolicy{
			Label:             "bounded",
			MaxAttempts:       2,
			Delay:             time.Hour,
			SafeRetryContract: "idempotent",
		},
		Classify: func(error) flowy.ActivityFailureClass { return flowy.ActivityRetryable },
		Dispatch: func(_ context.Context, invocation flowy.ActivityInvocation) ([]byte, error) {
			if calls.Add(1) == 1 {
				identity = invocation.Identity
				return nil, errors.New("retryable")
			}
			if invocation.Identity != identity || invocation.Attempt != 2 {
				return nil, errors.New("retry address reset")
			}
			return []byte("confirmed"), nil
		},
	}
	options := flowy.DurableOptions{Owner: "worker", LeaseTTL: time.Minute, Clock: persistedRetryClock{at: now}}
	store := NewExecutionStore(pool)
	old := persistentReferenceRunnerOptions(t, store, referenceDescriptor("old"), "old-node", request, options)
	if _, err := old.Start(ctx, id, intState{}); !errors.Is(err, flowy.ErrActivityRetryPending) {
		t.Fatal(err)
	}
	source, err := store.LoadExecution(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	pool.Close()
	options.Migrations = []flowy.ExecutionMigration{
		{ID: "move", Source: source.Descriptor, Target: referenceDescriptor("new"),
			Transform: func(state flowy.MigrationState) (flowy.MigrationState, error) {
				state.ExecutionPointer = "new-node"
				state.JournalReferences = map[string]string{"operation": identity}
				return state, nil
			}},
	}
	earlyCtx, earlyPool := reopenPool(t, pool)
	earlyStore := NewExecutionStore(earlyPool)
	earlyRunner := persistentReferenceRunnerOptions(
		t,
		earlyStore,
		referenceDescriptor("new"),
		"new-node",
		request,
		options,
	)
	// Act: migration must not reset backoff even after the original pool is gone.
	_, earlyErr := earlyRunner.Resume(earlyCtx, flowy.ResumeToken{ThreadID: id, SnapshotRevision: source.Revision})
	prepared, err := earlyStore.LoadExecution(earlyCtx, id)
	if err != nil {
		t.Fatal(err)
	}
	entry := persistentReferenceEntry(t, prepared)
	if !errors.Is(earlyErr, flowy.ErrActivityRetryPending) || calls.Load() != 1 || entry.Identity != identity ||
		len(entry.Attempts) != 1 || !entry.NextAttemptAt.Equal(now.Add(time.Hour)) {
		t.Fatalf("migration reset deadline: %v %+v", earlyErr, entry)
	}
	earlyPool.Close()
	resumeCtx, resumePool := reopenPool(t, pool)
	options.Clock, options.Migrations = persistedRetryClock{at: now.Add(time.Hour)}, nil
	restartedStore := NewExecutionStore(resumePool)
	restarted := persistentReferenceRunnerOptions(
		t,
		restartedStore,
		referenceDescriptor("new"),
		"new-node",
		request,
		options,
	)
	_, resumeErr := restarted.Resume(resumeCtx, flowy.ResumeToken{ThreadID: id, SnapshotRevision: prepared.Revision})
	// Assert: retry uses the original identity and retains the failed first attempt.
	latest, err := restartedStore.LoadExecution(resumeCtx, id)
	if err != nil {
		t.Fatal(err)
	}
	completed := persistentReferenceEntry(t, latest)
	if resumeErr != nil || calls.Load() != 2 || completed.Identity != identity || completed.Node != "old-node" ||
		completed.State != flowy.ActivityCompleted || len(completed.Attempts) != 2 || completed.Attempts[0].State != flowy.ActivityFailed {
		t.Fatalf("persistent retry history lost: %v %+v", resumeErr, completed)
	}
}
