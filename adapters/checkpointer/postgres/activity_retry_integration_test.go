//go:build integration

package postgres

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/skosovsky/flowy"
	"github.com/skosovsky/flowy/checkpoint"
)

type persistedRetryState struct{ Value int }
type persistedRetryClock struct{ at time.Time }

func (c persistedRetryClock) Now() time.Time { return c.at }

func postgresRetryRunner(t *testing.T, store flowy.ExecutionStore, at time.Time, calls *atomic.Int32,
	profiles ...*flowy.WaitCapabilityProfile,
) *flowy.DurableRunner[persistedRetryState, flowy.NoEffect] {
	t.Helper()
	builder := flowy.NewGraph[persistedRetryState, flowy.NoEffect](
		func(_, update persistedRetryState) persistedRetryState { return update },
	)
	builder.AddNode("write", func(ctx context.Context, state persistedRetryState) (persistedRetryState, flowy.Directive, error) {
		_, err := flowy.CallActivity(ctx, flowy.ActivityRequest{
			Key:            "write",
			Implementation: "stable",
			Input:          []byte("input"),
			Retry: flowy.ActivityRetryPolicy{
				Label:       "bounded",
				MaxAttempts: 2,
				Schedule: flowy.ActivityRetrySchedule{
					Kind:         flowy.ActivityRetryFixed,
					InitialDelay: time.Hour,
					MaxDelay:     time.Hour,
				},
				SafeRetryContract: "host-idempotency",
			},
			Classify: func(error) flowy.ActivityFailureDecision {
				return flowy.ActivityFailureDecision{Class: flowy.ActivityRetryable}
			},
			Dispatch: func(context.Context, flowy.ActivityInvocation) ([]byte, error) {
				if calls.Add(1) == 1 {
					return nil, errors.New("dispatch failed")
				}
				return []byte("confirmed"), nil
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
	var profile *flowy.WaitCapabilityProfile
	if len(profiles) != 0 {
		profile = profiles[0]
	}
	runner, err := flowy.NewDurableRunner(
		graph,
		store,
		flowy.ExecutionDescriptor{
			GraphID:       "retry",
			GraphRevision: "current",
			StateCodec:    "json", EffectsCodec: "host-effects-v1",
			ExecutionContract: "sync",
			ReplayPolicy:      flowy.StepReplayPolicy{Label: "test-safe-steps", Mode: flowy.StepReplaySafe},
		},
		checkpoint.JSONSerializer[persistedRetryState]{},
		checkpoint.JSONSerializer[[]flowy.NoEffect]{},
		flowy.DurableOptions{
			Owner:       "reused-owner",
			LeaseTTL:    time.Minute,
			Clock:       persistedRetryClock{at: at},
			WaitProfile: profile,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	return runner
}

func TestActivityRetryPersistentRestart(t *testing.T) {
	// Arrange: the worker clock advances, while persistence outlives connection pools.
	ctx, pool := racePool(t)
	if _, err := pool.Exec(ctx, ExecutionSchemaSQL()); err != nil {
		t.Fatal(err)
	}
	id := testThreadID(t)
	at := time.Date(2026, time.October, 4, 0, 0, 0, 0, time.UTC)
	store := NewExecutionStore(pool)
	var calls atomic.Int32
	runner := postgresRetryRunner(t, store, at, &calls)
	// Act: commit retry, discard the pool, then resume using independent connections.
	failed, err := runner.Start(ctx, id, persistedRetryState{})
	var pending *flowy.ActivityRetryPendingError
	if !errors.As(err, &pending) || failed == nil || !pending.Deadline.Equal(at.Add(time.Hour)) {
		t.Fatalf("retry not persisted: %+v %v", failed, err)
	}
	before, loadErr := store.LoadExecution(ctx, id)
	if loadErr != nil {
		t.Fatal(loadErr)
	}
	pool.Close()
	restartCtx, restartPool := racePool(t)
	restartedStore := NewExecutionStore(restartPool)
	restarted := postgresRetryRunner(t, restartedStore, at, &calls)
	blocked, blockedErr := restarted.Resume(restartCtx, failed.ResumeToken)
	// Assert: no dispatch or journal mutation before the persisted deadline.
	if !errors.Is(blockedErr, flowy.ErrActivityRetryPending) || blocked == nil || calls.Load() != 1 {
		t.Fatalf("early dispatch: %+v %v calls=%d", blocked, blockedErr, calls.Load())
	}
	after, afterErr := restartedStore.LoadExecution(restartCtx, id)
	if afterErr != nil || after.Revision != before.Revision ||
		!bytes.Equal(after.JournalPayload, before.JournalPayload) {
		t.Fatalf("restart reset persisted attempts: %+v %v", after, afterErr)
	}
	ready := postgresRetryRunner(t, restartedStore, pending.Deadline, &calls)
	result, readyErr := ready.Resume(restartCtx, blocked.ResumeToken)
	if readyErr != nil || result.State.Value != 1 || calls.Load() != 2 {
		t.Fatalf("due retry failed: %+v %v", result, readyErr)
	}
	assertPostgresRetryJournal(restartCtx, t, restartedStore, id)
}

func assertPostgresRetryJournal(ctx context.Context, t *testing.T, store flowy.ExecutionStore, id string) {
	t.Helper()
	envelope, err := store.LoadExecution(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	var journal map[string]flowy.ActivityRecord
	if decodeErr := json.Unmarshal(envelope.JournalPayload, &journal); decodeErr != nil {
		t.Fatal(decodeErr)
	}
	if len(journal) != 1 {
		t.Fatalf("retry changed activity identity: %+v", journal)
	}
	for _, record := range journal {
		if record.State != flowy.ActivityCompleted || len(record.Attempts) != 2 ||
			record.Attempts[0].Classification != flowy.ActivityRetryable || record.Attempts[1].State != flowy.ActivityCompleted {
			t.Fatalf("attempt history lost: %+v", record)
		}
	}
}
