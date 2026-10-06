package flowy_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/skosovsky/flowy"
	"github.com/skosovsky/flowy/checkpoint"
	"github.com/skosovsky/flowy/testutil"
)

var errDurableNodeFailure = errors.New("definitive node failure")

func failureRunner(
	t *testing.T,
	store flowy.ExecutionStore,
	calls *atomic.Int32,
	nodeError bool,
) *flowy.DurableRunner[durableTestState, flowy.NoEffect] {
	t.Helper()
	builder := flowy.NewGraph[durableTestState, flowy.NoEffect](
		func(_, update durableTestState) durableTestState { return update },
	)
	builder.AddNode("node", func(_ context.Context, state durableTestState) (durableTestState, flowy.Directive, error) {
		calls.Add(1)
		state.Value++
		if nodeError {
			return state, flowy.End(), errDurableNodeFailure
		}
		return state, flowy.Fail("definitive directive failure"), nil
	}).AllowNoOutgoingRoute("node").SetEntryPoint("node")
	return compileActivityTestRunner(t, builder, store)
}

func TestDurableTerminalFailureReplaysErrorWithoutExecution(t *testing.T) {
	t.Parallel()
	for _, nodeError := range []bool{false, true} {
		for _, stream := range []bool{false, true} {
			t.Run(
				map[bool]string{false: "directive", true: "node-error"}[nodeError]+map[bool]string{false: "/sync", true: "/stream"}[stream],
				func(t *testing.T) {
					t.Parallel()
					// Arrange.
					ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
					defer cancel()
					store := testutil.NewMemoryExecutionStore(nil)
					var calls atomic.Int32
					runner := failureRunner(t, store, &calls, nodeError)
					// Act: execute once, then use an independent runner to recover the failure.
					live, liveEvents, liveErr := invokeFailure(ctx, t, runner, store, nil, stream)
					if live == nil || liveErr == nil || live.Status != flowy.RunStatusFailed {
						t.Fatalf("live failure missing: %+v %v", live, liveErr)
					}
					if nodeError && !errors.Is(liveErr, errDurableNodeFailure) {
						t.Fatalf("live error identity lost: %v", liveErr)
					}
					restarted := failureRunner(t, store, &calls, nodeError)
					cached, replayEvents, cachedErr := invokeFailure(
						ctx,
						t,
						restarted,
						store,
						&live.ResumeToken,
						stream,
					)
					// Assert.
					assertCachedFailure(t, cached, cachedErr, live, liveErr, calls.Load())
					if stream && (liveEvents != 1 || replayEvents != 1) {
						t.Fatalf("terminal events: live=%d replay=%d", liveEvents, replayEvents)
					}
				},
			)
		}
	}
}

func assertCachedFailure(
	t *testing.T,
	cached *flowy.RunResult[durableTestState, flowy.NoEffect],
	cachedErr error,
	live *flowy.RunResult[durableTestState, flowy.NoEffect],
	liveErr error,
	calls int32,
) {
	t.Helper()
	var persisted *flowy.PersistedExecutionError
	if cached == nil || cached.Status != flowy.RunStatusFailed || cached.Reason != live.Reason ||
		!errors.As(
			cachedErr,
			&persisted,
		) || !errors.Is(cachedErr, flowy.ErrExecutionFailed) || cachedErr.Error() != liveErr.Error() || calls != 1 {
		t.Fatalf("failed outcome replay: %+v err=%v calls=%d", cached, cachedErr, calls)
	}
}

func invokeFailure(
	ctx context.Context,
	t *testing.T,
	runner *flowy.DurableRunner[durableTestState, flowy.NoEffect],
	store flowy.ExecutionStore,
	token *flowy.ResumeToken,
	stream bool,
) (*flowy.RunResult[durableTestState, flowy.NoEffect], int, error) {
	t.Helper()
	if !stream {
		if token == nil {
			result, err := runner.Start(ctx, "run", durableTestState{})
			return result, 0, err
		}
		result, err := runner.Resume(ctx, *token)
		return result, 0, err
	}
	var handle flowy.StreamHandle[durableTestState, flowy.NoEffect]
	var err error
	if token == nil {
		handle, err = runner.Stream(ctx, "run", durableTestState{})
	} else {
		handle, err = runner.ResumeStream(ctx, *token)
	}
	if err != nil {
		return nil, 0, err
	}
	failed := 0
	for event := range handle.Events() {
		if event.Type == flowy.EventFailed {
			failed++
			stored, loadErr := store.LoadExecution(ctx, "run")
			if loadErr != nil || stored.Terminal == nil || stored.Terminal.Failure == nil || event.Error == nil {
				t.Fatalf("failure announced before commit: %+v %v event=%+v", stored, loadErr, event)
			}
		}
	}
	result, waitErr := handle.WaitResult()
	return result, failed, waitErr
}

func TestDurableFailureCommitErrorRemainsRecoverable(t *testing.T) {
	t.Parallel()
	// Arrange: fail the terminal write after the initial envelope.
	ctx := context.Background()
	store := &faultExecutionStore{ExecutionStore: testutil.NewMemoryExecutionStore(nil), failAt: 2}
	var calls atomic.Int32
	runner := failureRunner(t, store, &calls, true)
	// Act.
	handle, err := runner.Stream(ctx, "run", durableTestState{})
	if err != nil {
		t.Fatal(err)
	}
	for event := range handle.Events() {
		if event.Type == flowy.EventFailed {
			t.Fatal("uncommitted failure event")
		}
	}
	result, runErr := handle.WaitResult()
	// Assert: no fabricated failure, no retry of the failed write within this invocation.
	stored, loadErr := store.LoadExecution(ctx, "run")
	if !errors.Is(runErr, errInjectedCommit) || result == nil || loadErr != nil || stored.Terminal != nil ||
		calls.Load() != 1 ||
		store.commits.Load() != 2 {
		t.Fatalf("failed write hidden: %+v %v stored=%+v load=%v", result, runErr, stored, loadErr)
	}
	_, resumeErr := runner.Resume(ctx, result.ResumeToken)
	if !errors.Is(resumeErr, errDurableNodeFailure) || calls.Load() != 2 {
		t.Fatalf("recoverable failure did not resume: %v calls=%d", resumeErr, calls.Load())
	}
}

func TestDurableCorruptTerminalRejectsBeforeCodec(t *testing.T) {
	t.Parallel()
	for name, terminal := range map[string]*flowy.ExecutionTerminal{
		"status":                 {Status: "invalid"},
		"completed-with-failure": {Status: flowy.RunStatusCompleted, Failure: &flowy.ExecutionFailure{Message: "invalid"}},
		"failed-without-failure": {Status: flowy.RunStatusFailed},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			// Arrange: metadata is corrupt and state cannot be decoded by the current codec.
			ctx := context.Background()
			store := testutil.NewMemoryExecutionStore(nil)
			lease, err := store.AcquireExecution(ctx, "run", "seed", time.Minute)
			if err != nil {
				t.Fatal(err)
			}
			source, err := store.CommitExecution(
				ctx,
				0,
				lease,
				flowy.ExecutionEnvelope{
					ExecutionID: "run",
					Descriptor:  durableDescriptor("current"),
					Progress:    flowy.MigrationState{ExecutionPointer: "node", StatePayload: []byte("opaque")},
					Terminal:    terminal,
				},
			)
			if err != nil {
				t.Fatal(err)
			}
			if releaseErr := store.ReleaseExecution(ctx, lease); releaseErr != nil {
				t.Fatal(releaseErr)
			}
			var calls atomic.Int32
			builder := flowy.NewGraph[durableTestState, flowy.NoEffect](
				func(_, update durableTestState) durableTestState { return update },
			)
			builder.AddNode("node", func(_ context.Context, state durableTestState) (durableTestState, flowy.Directive, error) {
				calls.Add(100)
				return state, flowy.End(), nil
			}).
				AllowNoOutgoingRoute("node").
				SetEntryPoint("node")
			graph, err := builder.Compile()
			if err != nil {
				t.Fatal(err)
			}
			runner, err := flowy.NewDurableRunner(
				graph,
				store,
				durableDescriptor("current"),
				failingDecode{calls: &calls},
				checkpoint.JSONSerializer[[]flowy.NoEffect]{},
				flowy.DurableOptions{Owner: "worker", LeaseTTL: time.Minute},
			)
			if err != nil {
				t.Fatal(err)
			}
			// Act.
			_, resumeErr := runner.Resume(ctx, flowy.ResumeToken{ThreadID: "run", SnapshotRevision: source.Revision})
			// Assert.
			if !errors.Is(resumeErr, flowy.ErrInvalidSnapshot) || calls.Load() != 0 {
				t.Fatalf("corrupt terminal decoded: %v calls=%d", resumeErr, calls.Load())
			}
		})
	}
}
