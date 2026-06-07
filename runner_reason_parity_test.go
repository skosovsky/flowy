package flowy

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestInfraFailureStreamEventReasonMatchesSync(t *testing.T) {
	t.Parallel()

	t.Run("handoff resolve invalid pointer", func(t *testing.T) {
		t.Parallel()
		g, cp, opts := infraFailureHandoffResolveGraph(t)
		assertInfraFailureStreamSync(
			t,
			g,
			cp,
			opts,
			RunStatusFailed,
			EventFailed,
			"",
			ReasonHandoffPointerResolveFailed,
		)
	})

	t.Run("handoff save hard fail", func(t *testing.T) {
		t.Parallel()
		g, cp, opts := infraFailureHandoffSaveGraph(t)
		assertInfraFailureStreamSync(t, g, cp, opts, RunStatusFailed, EventFailed, "router", ReasonHandoffSaveFailed)
	})
}

//nolint:gocognit // table-driven sync/stream parity across suspend, handoff, context cancel
func TestSkipOnSaveErrorStreamEventReasonMatchesSyncResult(t *testing.T) {
	t.Parallel()

	type state struct{ Ticks int }

	skipOnSaveOpts := []RunOption[state, NoEffect]{
		WithCheckpointErrorPolicy[state, NoEffect](CheckpointPolicySkipOnSaveError),
	}

	tests := []struct {
		name          string
		build         func() (*Graph[state, NoEffect], Checkpointer[state, NoEffect], []RunOption[state, NoEffect])
		terminalEvent EventType
		useCancelCtx  bool
	}{
		{
			name: "suspend",
			build: func() (*Graph[state, NoEffect], Checkpointer[state, NoEffect], []RunOption[state, NoEffect]) {
				cp := &failingMemoryCP[state, NoEffect]{failSave: true}
				b := NewGraph[state, NoEffect](func(_ state, u state) state { return u })
				b.AddNode("wait", func(_ context.Context, s state) (state, Directive, error) {
					return s, Suspend("hold"), nil
				})
				b.AllowNoOutgoingRoute("wait")
				b.SetEntryPoint("wait")
				g, _ := b.Compile()
				return g, cp, skipOnSaveOpts
			},
			terminalEvent: EventSuspended,
		},
		{
			name: "handoff",
			build: func() (*Graph[state, NoEffect], Checkpointer[state, NoEffect], []RunOption[state, NoEffect]) {
				cp := &failingMemoryCP[state, NoEffect]{failSave: true}
				b := NewGraph[state, NoEffect](func(_ state, u state) state { return u })
				b.AddNode("work", func(_ context.Context, s state) (state, Directive, error) {
					return s, Handoff("bg"), nil
				})
				b.AllowNoOutgoingRoute("work")
				b.SetEntryPoint("work")
				g, _ := b.Compile()
				opts := make([]RunOption[state, NoEffect], len(skipOnSaveOpts), len(skipOnSaveOpts)+1)
				copy(opts, skipOnSaveOpts)
				opts = append(opts, WithHandoffOutbox[state, NoEffect](&stubHandoffOutbox{}))
				return g, cp, opts
			},
			terminalEvent: EventHandoff,
		},
		{
			name: "context cancel",
			build: func() (*Graph[state, NoEffect], Checkpointer[state, NoEffect], []RunOption[state, NoEffect]) {
				cp := &failingMemoryCP[state, NoEffect]{failSave: true}
				b := NewGraph[state, NoEffect](func(_ state, u state) state { return u })
				b.AddNode("loop", func(_ context.Context, s state) (state, Directive, error) {
					s.Ticks++
					return s, Completed(), nil
				})
				b.AddEdge("loop", "loop")
				b.SetEntryPoint("loop")
				g, _ := b.Compile(WithMaxSteps(50))
				return g, cp, skipOnSaveOpts
			},
			terminalEvent: EventContextCanceled,
			useCancelCtx:  true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			g, cp, opts := tc.build()
			runCtx := context.Background()
			if tc.useCancelCtx {
				var cancel context.CancelFunc
				runCtx, cancel = context.WithCancel(context.Background())
				cancel()
			}

			syncRes, syncErr := g.NewRunner(cp).Start(runCtx, "skip-on-save-sync-th", state{}, opts...)
			if tc.useCancelCtx {
				if syncErr == nil {
					t.Fatalf("expected context canceled, got %+v", syncRes)
				}
			} else if syncErr != nil {
				t.Fatalf("unexpected sync error: %v res=%+v", syncErr, syncRes)
			}

			streamCtx := context.Background()
			if tc.useCancelCtx {
				var cancel context.CancelFunc
				streamCtx, cancel = context.WithCancel(context.Background())
				cancel()
			}
			handle, err := g.NewRunner(cp).Stream(streamCtx, "skip-on-save-stream-th", state{}, opts...)
			if err != nil {
				t.Fatalf("stream: %v", err)
			}
			events, _ := CollectEventsAndWait(context.Background(), handle)

			assertTerminalEventReasonMatchesSync(t, events, tc.terminalEvent, syncRes.Reason)
			if tc.name == "handoff" {
				assertHandoffRunMetaNoneOnSkip(t, syncRes)
			}
		})
	}
}

func TestTerminalRetentionFailureEventReasonMatchesResult(t *testing.T) {
	t.Parallel()

	type state struct{}

	tests := []struct {
		name          string
		directive     Directive
		wantReason    string
		terminalEvent EventType
	}{
		{
			name:          "suspend",
			directive:     Suspend("hold"),
			wantReason:    "hold_retention_failed",
			terminalEvent: EventSuspended,
		},
		{
			name:          "handoff",
			directive:     Handoff("bg"),
			wantReason:    "bg_retention_failed",
			terminalEvent: EventHandoff,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			g, cp := retentionFailureGraph[state, NoEffect](t, tc.directive)

			syncRes, syncErr := g.NewRunner(cp).Start(context.Background(), "retention-sync-th", state{})
			if syncErr == nil {
				t.Fatalf("expected retention error on sync start, got %+v", syncRes)
			}
			if syncRes.Reason != tc.wantReason {
				t.Fatalf("sync result reason: want %q, got %q", tc.wantReason, syncRes.Reason)
			}

			handle, err := g.NewRunner(cp).Stream(context.Background(), "retention-stream-th", state{})
			if err != nil {
				t.Fatalf("stream: %v", err)
			}
			events, waitErr := CollectEventsAndWait(context.Background(), handle)
			if waitErr == nil {
				t.Fatal("expected retention error on stream Wait")
			}

			assertTerminalEventReasonMatchesSync(t, events, tc.terminalEvent, syncRes.Reason)
		})
	}
}

func TestSuspendSaveHardFailStreamEventReasonMatchesSync(t *testing.T) {
	t.Parallel()

	type state struct{}

	cp := &failingMemoryCP[state, NoEffect]{failSave: true}
	b := NewGraph[state, NoEffect](func(_ state, u state) state { return u })
	b.AddNode("wait", func(_ context.Context, s state) (state, Directive, error) {
		return s, Suspend("hold"), nil
	})
	b.AllowNoOutgoingRoute("wait")
	b.SetEntryPoint("wait")
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	assertInfraFailureStreamSync(t, g, cp, nil, RunStatusFailed, EventFailed, "wait", ReasonSuspendSaveFailed)
}

func TestMaxStepsExceededStreamEventReasonMatchesSync(t *testing.T) {
	t.Parallel()

	type state struct{}
	b := NewGraph[state, NoEffect](func(_ state, u state) state { return u })
	b.AddNode("loop", func(_ context.Context, s state) (state, Directive, error) {
		return s, Completed(), nil
	})
	b.AddEdge("loop", "loop")
	b.SetEntryPoint("loop")
	g, err := b.Compile(WithMaxSteps(1))
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	assertTerminalErrorStreamReasonMatchesSync(
		t,
		g,
		newMemoryCP[state, NoEffect](),
		"max-steps-sync-th",
		"max-steps-stream-th",
		nil,
		ErrMaxStepsExceeded,
		ErrMaxStepsExceeded.Error(),
	)
}

func TestLeaseLostStreamEventReasonMatchesSync(t *testing.T) {
	t.Parallel()

	lease := NewMemoryLeaseManager()
	leaseOpts := []RunOption[struct{}, NoEffect]{
		WithRunLease[struct{}, NoEffect]("worker-a", 200*time.Millisecond),
	}
	runnerOpts := []RunnerOption[struct{}, NoEffect]{
		WithLeaseManager[struct{}, NoEffect](lease),
	}

	readySync := make(chan struct{})
	runnerSync := leaseLostBlockingGraph(t, readySync).NewRunnerWithOptions(
		newMemoryCP[struct{}, NoEffect](),
		runnerOpts,
	)
	syncDone := make(chan struct{})
	var syncRes *RunResult[struct{}, NoEffect]
	var syncErr error
	go func() {
		syncRes, syncErr = runnerSync.Start(context.Background(), "lease-sync-th", struct{}{}, leaseOpts...)
		close(syncDone)
	}()
	<-readySync
	stealLeaseAndWait(t, lease, "lease-sync-th")
	<-syncDone
	if !errors.Is(syncErr, ErrLeaseLost) {
		t.Fatalf("sync error: want ErrLeaseLost, got res=%+v err=%v", syncRes, syncErr)
	}
	if syncRes == nil || syncRes.Reason != ErrLeaseLost.Error() {
		t.Fatalf("sync reason: want %q, got res=%+v", ErrLeaseLost.Error(), syncRes)
	}

	readyStream := make(chan struct{})
	runnerStream := leaseLostBlockingGraph(t, readyStream).NewRunnerWithOptions(
		newMemoryCP[struct{}, NoEffect](),
		runnerOpts,
	)
	handle, streamErr := runnerStream.Stream(context.Background(), "lease-stream-th", struct{}{}, leaseOpts...)
	if streamErr != nil {
		t.Fatalf("stream: %v", streamErr)
	}
	outCh := BeginStreamCollect(handle)
	<-readyStream
	stealLeaseAndWait(t, lease, "lease-stream-th")
	events, waitErr := awaitStreamCollect(t, handle, outCh, 5*time.Second)
	if !errors.Is(waitErr, ErrLeaseLost) {
		t.Fatalf("stream Wait: want ErrLeaseLost, got %v", waitErr)
	}
	requireTerminalEventReason(t, events, EventFailed, ErrLeaseLost.Error())
}

func TestNamedBudgetExceededStreamReasonMatchesSync(t *testing.T) {
	t.Parallel()

	type state struct{}
	b := NewGraph[state, NoEffect](func(_ state, u state) state { return u })
	b.AddNode("loop", func(ctx context.Context, s state) (state, Directive, error) {
		_ = UseBudget(ctx, "api", 1)
		return s, Completed(), nil
	})
	b.AddEdge("loop", "loop")
	b.SetEntryPoint("loop")
	g, err := b.Compile(WithNamedBudget("api", 2))
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	assertTerminalErrorStreamReasonMatchesSync(
		t,
		g,
		newMemoryCP[state, NoEffect](),
		"budget-sync-th",
		"budget-stream-th",
		nil,
		ErrBudgetExceeded,
		ErrBudgetExceeded.Error(),
	)
}

func TestRetryBudgetExceededStreamReasonMatchesSync(t *testing.T) {
	t.Parallel()

	type state struct {
		Attempts int
	}
	b := NewGraph[state, NoEffect](func(_ state, u state) state { return u })
	b.AddNode("work", func(_ context.Context, s state) (state, Directive, error) {
		s.Attempts++
		return s, Retry(1), nil
	})
	b.AddNode("fallback", func(_ context.Context, s state) (state, Directive, error) {
		return s, Completed(), nil
	})
	b.AddRetryRoute("work", "fallback")
	b.AddEdge("fallback", "work")
	b.SetEntryPoint("work")
	b.AllowNoOutgoingRoute("work")
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	assertTerminalErrorStreamReasonMatchesSync(
		t,
		g,
		newMemoryCP[state, NoEffect](),
		"retry-budget-sync-th",
		"retry-budget-stream-th",
		nil,
		ErrRetryBudgetExceeded,
		ErrRetryBudgetExceeded.Error(),
	)
}

type reasonPairState struct{ N int }

func loopReasonPairGraphForCloseTests(t *testing.T) (*Graph[reasonPairState, NoEffect], <-chan struct{}) {
	t.Helper()
	ready := make(chan struct{})
	b := NewGraph[reasonPairState, NoEffect](func(_ reasonPairState, u reasonPairState) reasonPairState { return u })
	b.AddNode("loop", func(_ context.Context, s reasonPairState) (reasonPairState, Directive, error) {
		select {
		case <-ready:
		default:
			close(ready)
		}
		return s, Completed(), nil
	})
	b.AddEdge("loop", "loop")
	b.SetEntryPoint("loop")
	g, err := b.Compile(WithMaxSteps(10_000))
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	return g, ready
}

func TestStreamCancelAndRequestStopSyncStreamReasonPair(t *testing.T) {
	t.Parallel()

	t.Run("parent_cancel_sync_stream_reason_pair", func(t *testing.T) {
		t.Parallel()

		cp := newMemoryCP[reasonPairState, NoEffect]()
		g, ready := loopReasonPairGraphForCloseTests(t)

		ctx, cancel := context.WithCancel(context.Background())
		handle, err := g.NewRunner(cp).Stream(ctx, "close-reason-pair-th", reasonPairState{})
		if err != nil {
			t.Fatalf("stream: %v", err)
		}
		outCh := BeginStreamCollect(handle)
		<-ready
		cancel()
		events, waitErr := awaitStreamCollect(t, handle, outCh, 5*time.Second)
		if waitErr != nil && !errors.Is(waitErr, context.Canceled) {
			t.Fatalf("unexpected wait error after cancel: %v", waitErr)
		}

		syncRes := requireSyncContextCanceledResult(t, g, cp, "close-reason-pair-sync-th")
		requireTerminalEventReason(t, events, EventContextCanceled, syncRes.Reason)
	})

	t.Run("request_stop_sync_stream_reason_pair", func(t *testing.T) {
		t.Parallel()

		cp := newMemoryCP[reasonPairState, NoEffect]()
		g, ready := loopReasonPairGraphForCloseTests(t)

		handle, err := g.NewRunner(cp).Stream(context.Background(), "close-reason-stop-th", reasonPairState{})
		if err != nil {
			t.Fatalf("stream: %v", err)
		}
		outCh := BeginStreamCollect(handle)
		<-ready
		handle.RequestStop()
		events, waitErr := awaitStreamCollect(t, handle, outCh, 5*time.Second)
		if waitErr != nil && !errors.Is(waitErr, context.Canceled) {
			t.Fatalf("unexpected wait error after RequestStop: %v", waitErr)
		}

		syncRes := requireSyncContextCanceledResult(t, g, cp, "close-reason-stop-sync-th")
		requireStreamCancelReasonMatchesSync(t, events, cp, "close-reason-stop-th", syncRes.Reason)
	})
}

func TestStreamSuspendPersistVsEventDroppedTerminalEventStillReturnsSuccess(t *testing.T) {
	t.Parallel()

	type state struct{ N int }
	cp := newMemoryCP[state, NoEffect]()
	b := NewGraph[state, NoEffect](func(_ state, u state) state { return u })
	b.AddNode("wait", func(_ context.Context, s state) (state, Directive, error) {
		s.N = 1
		return s, Suspend("hold"), nil
	})
	b.AllowNoOutgoingRoute("wait")
	b.SetEntryPoint("wait")
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	handle, err := g.NewRunner(cp).Stream(context.Background(), "suspend-drop-th", state{})
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
	var count int
	waitErr := ConsumeEventsAndWait(context.Background(), handle, func(RunEvent[state, NoEffect]) bool {
		count++
		return count < 2
	})
	if waitErr != nil {
		t.Fatalf("Wait: got %v want nil", waitErr)
	}
	snap := requireSnapshotPresent(t, cp, "suspend-drop-th")
	if snap.ExecutionPointer != "wait" {
		t.Fatalf("expected pointer wait, got %q", snap.ExecutionPointer)
	}
	if snap.Revision <= 0 {
		t.Fatalf("expected positive revision, got %d", snap.Revision)
	}
}
