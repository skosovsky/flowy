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

func TestActivityRetryDiscoveryPersistentProfileDeadlineAndNoDispatch(t *testing.T) {
	// Arrange: plain and not-yet-due heads precede a due, explicitly bound retry.
	ctx, pool := racePool(t)
	if _, err := pool.Exec(ctx, ExecutionSchemaSQL()); err != nil {
		t.Fatal(err)
	}
	profile := postgresWaitProfile()
	store, err := NewWaitExecutionStore(pool, profile)
	if err != nil {
		t.Fatal(err)
	}
	base := testThreadID(t)
	at := time.Date(2026, 10, 4, 0, 0, 0, 0, time.UTC)
	var plainCalls, futureCalls, dueCalls atomic.Int32
	if _, err = postgresRetryRunner(t, NewExecutionStore(pool), at, &plainCalls).
		Start(ctx, base+"01", persistedRetryState{}); !errors.Is(err, flowy.ErrActivityRetryPending) {
		t.Fatalf("plain pending fixture: %v", err)
	}
	if _, err = postgresRetryRunner(t, store, at.Add(time.Hour), &futureCalls, &profile).
		Start(ctx, base+"02", persistedRetryState{}); !errors.Is(err, flowy.ErrActivityRetryPending) {
		t.Fatalf("future pending fixture: %v", err)
	}
	armed, err := postgresRetryRunner(t, store, at, &dueCalls, &profile).Start(ctx, base+"03", persistedRetryState{})
	var pending *flowy.ActivityRetryPendingError
	if !errors.As(err, &pending) || armed == nil {
		t.Fatalf("bound pending fixture: %+v err=%v", armed, err)
	}
	before, err := store.LoadExecution(ctx, base+"03")
	if err != nil {
		t.Fatal(err)
	}
	assertRetryDiscoveryLeaseReleased(ctx, t, store, base+"03")
	pool.Close()
	restartCtx, restartPool := racePool(t)
	restarted, err := NewWaitExecutionStore(restartPool, profile)
	if err != nil {
		t.Fatal(err)
	}
	// Act: bounded keyset pages never adopt plain executions or dispatch candidates.
	plain, err := restarted.DiscoverDueActivityRetries(restartCtx, pending.Deadline, base, 1)
	if err != nil {
		t.Fatal(err)
	}
	future, err := restarted.DiscoverDueActivityRetries(restartCtx, pending.Deadline, plain.AfterExecutionID, 1)
	if err != nil {
		t.Fatal(err)
	}
	due, err := restarted.DiscoverDueActivityRetries(restartCtx, pending.Deadline, future.AfterExecutionID, 1)
	if err != nil {
		t.Fatal(err)
	}
	// Assert.
	assertRetryDiscoveryPages(t, base, plain, future, due)
	if plainCalls.Load() != 1 || futureCalls.Load() != 1 || dueCalls.Load() != 1 {
		t.Fatalf("discovery adopted/dispatched/reset: plain=%+v future=%+v due=%+v", plain, future, due)
	}
	candidate := due.Retries[0]
	assertDiscoveredRetryContract(t, candidate, armed.ResumeToken, pending)
	after, loadErr := restarted.LoadExecution(restartCtx, base+"03")
	if loadErr != nil || after.Digest != before.Digest || after.Revision != before.Revision {
		t.Fatalf("discovery changed history: %+v err=%v", after, loadErr)
	}
	result, resumeErr := postgresRetryRunner(t, restarted, candidate.Deadline, &dueCalls, &profile).
		Resume(restartCtx, candidate.ResumeToken)
	if resumeErr != nil || result.State.Value != 1 || dueCalls.Load() != 2 {
		t.Fatalf("discovered due retry not bounded: %+v err=%v calls=%d", result, resumeErr, dueCalls.Load())
	}
	settled, scanErr := restarted.DiscoverDueActivityRetries(restartCtx, pending.Deadline, base+"02", 1)
	if scanErr != nil || len(settled.Retries) != 0 {
		t.Fatalf("completed execution rediscovered: %+v err=%v", settled, scanErr)
	}
}

func assertRetryDiscoveryLeaseReleased(ctx context.Context, t *testing.T, store flowy.ExecutionStore, id string) {
	t.Helper()
	lease, err := store.AcquireExecution(ctx, id, "worker-release-probe", time.Minute)
	if err != nil {
		t.Fatalf("pending retry retained worker lease: %v", err)
	}
	if err = store.ReleaseExecution(ctx, lease); err != nil {
		t.Fatal(err)
	}
}

func assertRetryDiscoveryPages(t *testing.T, base string, plain, future, due ActivityRetryScanPage) {
	t.Helper()
	if len(plain.Retries) != 0 || !plain.More || plain.AfterExecutionID != base+"01" || len(future.Retries) != 0 ||
		future.AfterExecutionID != base+"02" || len(due.Retries) != 1 {
		t.Fatalf("discovery pagination/profile/deadline mismatch: plain=%+v future=%+v due=%+v", plain, future, due)
	}
}

func assertDiscoveredRetryContract(t *testing.T, candidate flowy.PendingActivityRetry,
	token flowy.ResumeToken, pending *flowy.ActivityRetryPendingError,
) {
	t.Helper()
	if candidate.ResumeToken != token || candidate.Identity != pending.Identity || candidate.Attempts != 1 ||
		!candidate.Deadline.Equal(pending.Deadline) || candidate.Policy.MaxAttempts != 2 ||
		candidate.Policy.SafeRetryContract != "host-idempotency" {
		t.Fatalf("candidate lost persisted contract: %+v", candidate)
	}
}
