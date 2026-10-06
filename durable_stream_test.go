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

func TestDurableStreamTerminalEventMatchesCommittedOutcome(t *testing.T) {
	// Arrange.
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	store := testutil.NewMemoryExecutionStore(nil)
	var calls atomic.Int32
	runner := activityTestRunner(t, store, &calls, false)
	// Act: create a durable stream, drain it, then stream its cached completion.
	handle, err := runner.Stream(ctx, "run", durableTestState{})
	if err != nil {
		t.Fatal(err)
	}
	assertCommittedStreamCompletion(ctx, t, store, handle)
	result, resultErr := handle.WaitResult()
	if resultErr != nil {
		t.Fatal(resultErr)
	}
	replayed, err := runner.ResumeStream(ctx, result.ResumeToken)
	if err != nil {
		t.Fatal(err)
	}
	assertCommittedStreamCompletion(ctx, t, store, replayed)
	// Assert: completion replay emits the same persisted state without dispatch.
	cached, cachedErr := replayed.WaitResult()
	if cachedErr != nil || cached.State.Value != 1 || cached.ResumeToken != result.ResumeToken || calls.Load() != 1 {
		t.Fatalf("terminal stream replay executed: %+v %v calls=%d", cached, cachedErr, calls.Load())
	}
}

func assertCommittedStreamCompletion(ctx context.Context, t *testing.T, store flowy.ExecutionStore,
	handle flowy.StreamHandle[durableTestState, flowy.NoEffect],
) {
	t.Helper()
	completed := 0
	for event := range handle.Events() {
		if event.Type != flowy.EventCompleted {
			continue
		}
		completed++
		envelope, err := store.LoadExecution(ctx, "run")
		if err != nil || envelope.Terminal == nil || envelope.Terminal.Status != flowy.RunStatusCompleted {
			t.Fatalf("completion announced before commit: %+v %v", envelope, err)
		}
	}
	if completed != 1 {
		t.Fatalf("expected one terminal event, got %d", completed)
	}
}

func TestDurableStreamFailedCommitNeverAnnouncesCompletion(t *testing.T) {
	// Arrange: initial+intent+running+outcome succeed, terminal commit fails.
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	store := &faultExecutionStore{ExecutionStore: testutil.NewMemoryExecutionStore(nil), failAt: 5}
	var calls atomic.Int32
	runner := activityTestRunner(t, store, &calls, false)
	// Act.
	handle, err := runner.Stream(ctx, "run", durableTestState{})
	if err != nil {
		t.Fatal(err)
	}
	for event := range handle.Events() {
		if event.Type == flowy.EventCompleted {
			t.Fatal("uncommitted completion announced")
		}
	}
	failed, waitErr := handle.WaitResult()
	// Assert: recover via persisted journal outcome without repeating dispatch.
	if !errors.Is(waitErr, errInjectedCommit) || failed == nil || calls.Load() != 1 {
		t.Fatalf("commit failure hidden: %+v %v", failed, waitErr)
	}
	recovered, recoverErr := runner.ResumeStream(ctx, failed.ResumeToken)
	if recoverErr != nil {
		t.Fatal(recoverErr)
	}
	assertCommittedStreamCompletion(ctx, t, store, recovered)
	result, resultErr := recovered.WaitResult()
	if resultErr != nil || result.State.Value != 1 || calls.Load() != 1 {
		t.Fatalf("stream recovery dispatched: %+v %v", result, resultErr)
	}
}
