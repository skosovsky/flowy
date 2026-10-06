package flowy_test

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/skosovsky/flowy"
	"github.com/skosovsky/flowy/checkpoint"
	"github.com/skosovsky/flowy/testutil"
)

type interruptionState struct {
	Value int
	Items map[string]int
}

type deadlineCommitStore struct {
	flowy.ExecutionStore

	commits atomic.Int32
}

func (s *deadlineCommitStore) CommitExecution(ctx context.Context, revision uint64,
	lease flowy.ExecutionLease, envelope flowy.ExecutionEnvelope,
) (flowy.ExecutionEnvelope, error) {
	const stepCommit = 5 // initial, prepared, running, completed activity, step
	if s.commits.Add(1) == stepCommit {
		<-ctx.Done()
		return flowy.ExecutionEnvelope{}, ctx.Err()
	}
	return s.ExecutionStore.CommitExecution(ctx, revision, lease, envelope)
}

func TestDurableInterruptionPreservesCommittedEntry(t *testing.T) {
	for _, cause := range []error{context.Canceled, context.DeadlineExceeded, flowy.ErrHandoffRequested} {
		for _, stream := range []bool{false, true} {
			for _, activity := range []bool{false, true} {
				name := fmt.Sprintf("%v/stream=%t/activity=%t", cause, stream, activity)
				t.Run(name, func(t *testing.T) { assertDurableInterruption(t, cause, stream, activity) })
			}
		}
	}
}

func assertDurableInterruption(t *testing.T, cause error, stream, activity bool) {
	t.Helper()
	// Arrange: map aliases and emitted effects must not cross an uncommitted step.
	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(context.Canceled)
	store := testutil.NewMemoryExecutionStore(nil)
	var calls, dispatches atomic.Int32
	runner := interruptionRunner(t, store, &calls, &dispatches, activity, func() { cancel(cause) })
	// Act: interrupt after the activity outcome but before the directive commit.
	var first *flowy.RunResult[interruptionState, string]
	var startErr error
	if stream {
		handle, err := runner.Stream(ctx, "interrupted", interruptionState{Items: map[string]int{"count": 0}})
		if err != nil {
			t.Fatal(err)
		}
		for range handle.Events() {
		}
		first, startErr = handle.WaitResult()
	} else {
		first, startErr = runner.Start(ctx, "interrupted", interruptionState{Items: map[string]int{"count": 0}})
	}
	if first == nil || (!errors.Is(cause, flowy.ErrHandoffRequested) && startErr == nil) ||
		(errors.Is(cause, flowy.ErrHandoffRequested) && startErr != nil) {
		t.Fatalf("interruption missing: %+v %v", first, startErr)
	}
	assertInterruptedEntry(t, store, first)
	resumeCtx, resumeCancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer resumeCancel()
	handle, err := runner.ResumeStream(resumeCtx, first.ResumeToken)
	if err != nil {
		t.Fatal(err)
	}
	for range handle.Events() {
	}
	result, resumeErr := handle.WaitResult()
	// Assert: one logical update/effect; completed activity replays its original input.
	wantDispatches := int32(0)
	if activity {
		wantDispatches = 1
	}
	if resumeErr != nil || result == nil || result.Status != flowy.RunStatusCompleted ||
		result.State.Value != 1 || result.State.Items["count"] != 1 || len(result.Effects) != 1 ||
		result.Effects[0] != "committed effect" || calls.Load() != 2 || dispatches.Load() != wantDispatches {
		t.Fatalf("replay crossed partial update: %+v err=%v calls=%d dispatches=%d",
			result, resumeErr, calls.Load(), dispatches.Load())
	}
}

func interruptionRunner(t *testing.T, store flowy.ExecutionStore, calls, dispatches *atomic.Int32,
	activity bool, interrupt func(),
) *flowy.DurableRunner[interruptionState, string] {
	t.Helper()
	b := flowy.NewGraph[interruptionState, string](
		func(_, update interruptionState) interruptionState { return update },
	)
	b.AddNode("node", func(ctx context.Context, state interruptionState) (interruptionState, flowy.Directive, error) {
		attempt := calls.Add(1)
		if activity {
			_, err := flowy.CallActivity(ctx, flowy.ActivityRequest{Key: "write", Implementation: "host",
				Input: []byte(fmt.Sprint(state.Value, state.Items["count"])),
				Dispatch: func(context.Context, flowy.ActivityInvocation) ([]byte, error) {
					dispatches.Add(1)
					return []byte("receipt"), nil
				}})
			if err != nil {
				return state, flowy.End(), err
			}
		}
		state.Value++
		state.Items["count"]++
		if attempt == 1 {
			interrupt()
		}
		return state, flowy.Effect(flowy.End(), "committed effect"), nil
	}).AllowNoOutgoingRoute("node").SetEntryPoint("node")
	graph, err := b.Compile()
	if err != nil {
		t.Fatal(err)
	}
	runner, err := flowy.NewDurableRunner(graph, store, durableDescriptor("interruption"),
		checkpoint.JSONSerializer[interruptionState]{}, checkpoint.JSONSerializer[[]string]{},
		flowy.DurableOptions{Owner: "host", LeaseTTL: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	return runner
}

func assertInterruptedEntry(t *testing.T, store flowy.ExecutionStore,
	result *flowy.RunResult[interruptionState, string],
) {
	t.Helper()
	envelope, err := store.LoadExecution(context.Background(), "interrupted")
	if err != nil {
		t.Fatal(err)
	}
	state, err := (checkpoint.JSONSerializer[interruptionState]{}).Unmarshal(envelope.Progress.StatePayload)
	if err != nil {
		t.Fatal(err)
	}
	effects, err := (checkpoint.JSONSerializer[[]string]{}).Unmarshal(envelope.EffectsPayload)
	if err != nil || state.Value != 0 || state.Items["count"] != 0 || len(effects) != 0 ||
		envelope.Progress.ExecutionPointer != "node" || envelope.Activation != 1 || envelope.Terminal != nil ||
		envelope.RunMeta.StepCount != 0 || result.State.Value != 0 || result.State.Items["count"] != 0 ||
		len(result.Effects) != 0 || result.ResumeToken.SnapshotRevision != envelope.Revision {
		t.Fatalf("partial entry published: envelope=%+v state=%+v effects=%v result=%+v err=%v",
			envelope, state, effects, result, err)
	}
}

func TestDurableConsumerStopPreservesCommittedEntry(t *testing.T) {
	// Arrange: coordinate RequestStop after a successful activity and in-place update.
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	store := testutil.NewMemoryExecutionStore(nil)
	var calls, dispatches atomic.Int32
	entered, release := make(chan struct{}), make(chan struct{})
	runner := interruptionRunner(t, store, &calls, &dispatches, true, func() {
		close(entered)
		<-release
	})
	handle, err := runner.Stream(ctx, "interrupted", interruptionState{Items: map[string]int{"count": 0}})
	if err != nil {
		t.Fatal(err)
	}
	// Act: close the consumer while handler output has not committed.
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	handle.RequestStop()
	close(release)
	for range handle.Events() {
	}
	first, waitErr := handle.WaitResult()
	if waitErr != nil && !errors.Is(waitErr, context.Canceled) {
		t.Fatal(waitErr)
	}
	if first == nil {
		t.Fatal("missing interrupted result")
	}
	assertInterruptedEntry(t, store, first)
	result, resumeErr := runner.Resume(ctx, first.ResumeToken)
	// Assert: original activity input replays once with no duplicate effect.
	if resumeErr != nil || result == nil || result.State.Value != 1 || result.State.Items["count"] != 1 ||
		len(result.Effects) != 1 || calls.Load() != 2 || dispatches.Load() != 1 {
		t.Fatalf("consumer stop lost continuation: %+v %v calls=%d dispatches=%d",
			result, resumeErr, calls.Load(), dispatches.Load())
	}
}

func TestDurableParentDeadlinePreservesCommittedEntry(t *testing.T) {
	// Arrange: the parent deadline expires after a completed activity, before step commit.
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	store := testutil.NewMemoryExecutionStore(nil)
	var calls, dispatches atomic.Int32
	runner := interruptionRunner(t, store, &calls, &dispatches, true, func() { <-ctx.Done() })
	// Act: wait for the real parent deadline, then recover under a new parent context.
	first, startErr := runner.Start(ctx, "interrupted", interruptionState{Items: map[string]int{"count": 0}})
	if first == nil || startErr == nil || !errors.Is(ctx.Err(), context.DeadlineExceeded) {
		t.Fatalf("deadline missing: %+v %v parent=%v", first, startErr, ctx.Err())
	}
	assertInterruptedEntry(t, store, first)
	resumeCtx, resumeCancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer resumeCancel()
	result, resumeErr := runner.Resume(resumeCtx, first.ResumeToken)
	// Assert: completed activity replays with its original input and one logical update/effect.
	if resumeErr != nil || result == nil || result.State.Value != 1 || result.State.Items["count"] != 1 ||
		len(result.Effects) != 1 || calls.Load() != 2 || dispatches.Load() != 1 {
		t.Fatalf("deadline lost continuation: %+v %v calls=%d dispatches=%d",
			result, resumeErr, calls.Load(), dispatches.Load())
	}
}

func TestDurableDeadlineDuringCommitReturnsEntry(t *testing.T) {
	for _, stream := range []bool{false, true} {
		t.Run(strconv.FormatBool(stream), func(t *testing.T) { assertDurableDeadlineDuringCommit(t, stream) })
	}
}

func assertDurableDeadlineDuringCommit(t *testing.T, stream bool) {
	t.Helper()
	// Arrange: force deadline expiration inside the actual step commit call.
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	store := &deadlineCommitStore{ExecutionStore: testutil.NewMemoryExecutionStore(nil)}
	var calls, dispatches atomic.Int32
	runner := interruptionRunner(t, store, &calls, &dispatches, true, func() {})
	// Act: handler completes while live; the storage boundary rejects after deadline.
	initial := interruptionState{Items: map[string]int{"count": 0}}
	var first *flowy.RunResult[interruptionState, string]
	var startErr error
	if stream {
		handle, err := runner.Stream(ctx, "interrupted", initial)
		if err != nil {
			t.Fatal(err)
		}
		for range handle.Events() {
		}
		first, startErr = handle.WaitResult()
	} else {
		first, startErr = runner.Start(ctx, "interrupted", initial)
	}
	if first == nil || !errors.Is(startErr, context.DeadlineExceeded) {
		t.Fatalf("commit deadline not observed: %+v %v", first, startErr)
	}
	assertInterruptedEntry(t, store, first)
	resumeCtx, resumeCancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer resumeCancel()
	result, resumeErr := runner.Resume(resumeCtx, first.ResumeToken)
	// Assert: the error/result pair never acknowledges a partial state/effect update.
	if resumeErr != nil || result == nil || result.State.Value != 1 || len(result.Effects) != 1 ||
		result.State.Items["count"] != 1 || calls.Load() != 2 || dispatches.Load() != 1 {
		t.Fatalf("commit deadline replay changed: %+v %v calls=%d dispatches=%d",
			result, resumeErr, calls.Load(), dispatches.Load())
	}
}
