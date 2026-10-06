//go:build integration

package postgres

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/skosovsky/flowy"
)

// Close the actual originating pool at a chosen aggregate boundary. Recovery
// must use new connections; an in-memory fault sentinel cannot satisfy this test.
type activityPoolFaultStore struct {
	flowy.ExecutionStore

	pool    *pgxpool.Pool
	commits atomic.Int32
	failAt  int32
}

func (s *activityPoolFaultStore) CommitExecution(ctx context.Context, revision uint64,
	lease flowy.ExecutionLease, envelope flowy.ExecutionEnvelope,
) (flowy.ExecutionEnvelope, error) {
	if s.commits.Add(1) == s.failAt {
		s.pool.Close()
	}
	return s.ExecutionStore.CommitExecution(ctx, revision, lease, envelope)
}

func TestActivityPersistentCrashBoundaries(t *testing.T) {
	for _, boundary := range []struct {
		name         string
		failAt       int32
		initialCalls int32
	}{
		{name: "intent", failAt: 2, initialCalls: 0},
		{name: "outcome", failAt: 4, initialCalls: 1},
		{name: "step", failAt: 5, initialCalls: 1},
	} {
		t.Run(boundary.name, func(t *testing.T) {
			assertActivityPersistentCrashBoundary(t, boundary.failAt, boundary.initialCalls)
		})
	}
}

func assertActivityPersistentCrashBoundary(t *testing.T, failAt, initialCalls int32) {
	t.Helper()
	// Arrange: successful remote writes, but originating storage connection disappears.
	ctx, pool := racePool(t)
	if _, err := pool.Exec(ctx, ExecutionSchemaSQL()); err != nil {
		t.Fatal(err)
	}
	id := testThreadID(t)
	store := &activityPoolFaultStore{ExecutionStore: NewExecutionStore(pool), pool: pool,
		commits: atomic.Int32{}, failAt: failAt}
	var calls, reconciles atomic.Int32
	request := flowy.ActivityRequest{Key: "operation", Implementation: "host", Input: []byte("input"),
		Dispatch: func(context.Context, flowy.ActivityInvocation) ([]byte, error) {
			calls.Add(1)
			return []byte("remote receipt"), nil
		}}
	runner := persistentReferenceRunner(t, store, referenceDescriptor("fault"), "node", request, nil)
	// Act: interrupt exactly before intent, outcome or step commit and discard original pool/runner.
	failed, startErr := runner.Start(ctx, id, intState{})
	if startErr == nil || failed == nil || failed.State.Value != 0 || failed.RunMeta.StepCount != 0 ||
		failed.ResumeToken.SnapshotRevision == 0 || calls.Load() != initialCalls {
		t.Fatalf(
			"fault did not interrupt expected boundary: result=%+v err=%v calls=%d",
			failed,
			startErr,
			calls.Load(),
		)
	}
	if failAt < 5 && !errors.Is(startErr, flowy.ErrActivityJournalUnavailable) {
		t.Fatalf("real outage lost stable classification: %v", startErr)
	}
	restartCtx, restartPool := racePool(t)
	restartedStore := NewExecutionStore(restartPool)
	source, err := restartedStore.LoadExecution(restartCtx, id)
	if err != nil {
		t.Fatal(err)
	}
	if source.Revision != uint64(failAt-1) || source.Terminal != nil {
		t.Fatalf("partial transition or false terminal: %+v", source)
	}
	restarted := persistentReferenceRunner(t, restartedStore, referenceDescriptor("fault"), "node", request, nil)
	assertActivityCrashLeaseExpiry(restartCtx, t, restartPool, restarted, failed.ResumeToken, source.Revision)
	result, resumeErr := restarted.Resume(restartCtx, failed.ResumeToken)
	if failAt == 4 {
		assertPersistentUnknownWithoutRedispatch(t, result, resumeErr, calls.Load())
		request.Reconcile = func(context.Context, flowy.ActivityRecord) ([]byte, error) {
			reconciles.Add(1)
			return []byte("remote receipt"), nil
		}
		restartPool.Close()
		resolveCtx, resolvePool := racePool(t)
		restartedStore = NewExecutionStore(resolvePool)
		restarted = persistentReferenceRunner(t, restartedStore, referenceDescriptor("fault"), "node", request, nil)
		result, resumeErr = restarted.Resume(resolveCtx, result.ResumeToken)
		restartCtx = resolveCtx
	}
	// Assert: one remote write total; persisted outcome/cursor/terminal survive fresh connections.
	if resumeErr != nil || result == nil || result.Status != flowy.RunStatusCompleted || calls.Load() != 1 {
		t.Fatalf("restart repeated/lost remote write: result=%+v err=%v calls=%d", result, resumeErr, calls.Load())
	}
	latest, err := restartedStore.LoadExecution(restartCtx, id)
	if err != nil {
		t.Fatal(err)
	}
	assertPersistentRecoveredActivity(t, latest, source, failAt, reconciles.Load())
}

func assertActivityCrashLeaseExpiry(ctx context.Context, t *testing.T, pool *pgxpool.Pool,
	runner *flowy.DurableRunner[intState, flowy.NoEffect], token flowy.ResumeToken, revision uint64,
) {
	t.Helper()
	// A dead worker's pool closure cannot revoke its persisted lease. Assert that
	// takeover is refused, then model elapsed TTL only for this fresh fixture ID.
	if _, err := runner.Resume(ctx, token); !errors.Is(err, flowy.ErrThreadLeaseBusy) {
		t.Fatalf("takeover before lease expiry: %v", err)
	}
	tag, err := pool.Exec(ctx, `UPDATE flowy_executions
SET lease_expiry=clock_timestamp()-interval '1 second'
WHERE execution_id=$1 AND revision=$2 AND lease_owner='worker'`, token.ThreadID, revision)
	if err != nil || tag.RowsAffected() != 1 {
		t.Fatalf("fixture lease expiry: rows=%d err=%v", tag.RowsAffected(), err)
	}
}

func assertPersistentRecoveredActivity(t *testing.T, latest, source flowy.ExecutionEnvelope,
	failAt, reconciles int32,
) {
	t.Helper()
	entry := persistentReferenceEntry(t, latest)
	expectedOrigin := flowy.ActivityLive
	if failAt == 4 {
		expectedOrigin = flowy.ActivityReconciled
	}
	if entry.State != flowy.ActivityCompleted || entry.Origin != expectedOrigin || len(entry.Attempts) != 1 ||
		string(
			entry.Outcome,
		) != "remote receipt" || latest.Terminal == nil || latest.Terminal.Status != flowy.RunStatusCompleted {
		t.Fatalf("recovery journal/terminal inconsistent: entry=%+v terminal=%+v", entry, latest.Terminal)
	}
	if failAt == 2 {
		if len(source.JournalPayload) != 0 {
			t.Fatal("failed intent was published")
		}
	}
	if failAt == 4 && (reconciles != 1 || entry.Attempts[0].State != flowy.ActivityUnknown) {
		t.Fatalf("unknown provenance lost: reconciles=%d attempts=%+v", reconciles, entry.Attempts)
	}
}

func assertPersistentUnknownWithoutRedispatch(t *testing.T, result *flowy.RunResult[intState, flowy.NoEffect],
	err error, calls int32,
) {
	t.Helper()
	if !errors.Is(err, flowy.ErrActivityUnknown) || result == nil || calls != 1 {
		t.Fatalf("abandoned remote write blindly retried: result=%+v err=%v calls=%d", result, err, calls)
	}
}
