package flowy

import (
	"context"
	"errors"
	"testing"
)

func TestTerminalStatusOnInfraFailure(t *testing.T) {
	t.Parallel()

	type state struct{}

	tests := []struct {
		name          string
		setup         func() (*Graph[state, NoEffect], Checkpointer[state, NoEffect], []RunOption[state, NoEffect])
		wantStatus    RunStatus
		wantReason    string
		wantResumeTok bool
		wantErr       error
	}{
		{
			name: "handoff resolve invalid pointer",
			setup: func() (*Graph[state, NoEffect], Checkpointer[state, NoEffect], []RunOption[state, NoEffect]) {
				b := NewGraph[state, NoEffect](func(_ state, u state) state { return u })
				b.AddNode("work", func(_ context.Context, s state) (state, Directive, error) {
					return s, Handoff("bg"), nil
				})
				b.AllowNoOutgoingRoute("work")
				b.SetEntryPoint("work")
				g, _ := b.Compile()
				resolver := WithSuspendPointerResolver[state, NoEffect](
					func(_ state, _ ExecutionPointer) (ExecutionPointer, error) {
						return "ghost", nil
					},
				)
				return g, newMemoryCP[state, NoEffect](), []RunOption[state, NoEffect]{resolver}
			},
			wantStatus:    RunStatusFailed,
			wantReason:    ReasonHandoffPointerResolveFailed,
			wantResumeTok: false,
		},
		{
			name: "handoff save hard fail",
			setup: func() (*Graph[state, NoEffect], Checkpointer[state, NoEffect], []RunOption[state, NoEffect]) {
				b := NewGraph[state, NoEffect](func(_ state, u state) state { return u })
				b.AddNode("work", func(_ context.Context, s state) (state, Directive, error) {
					return s, Handoff("bg"), nil
				})
				b.AllowNoOutgoingRoute("work")
				b.SetEntryPoint("work")
				g, _ := b.Compile()
				return g, &failingMemoryCP[state, NoEffect]{failSave: true}, nil
			},
			wantStatus:    RunStatusFailed,
			wantReason:    ReasonHandoffSaveFailed,
			wantResumeTok: false,
		},
		{
			name: "handoff schedule fail",
			setup: func() (*Graph[state, NoEffect], Checkpointer[state, NoEffect], []RunOption[state, NoEffect]) {
				b := NewGraph[state, NoEffect](func(_ state, u state) state { return u })
				b.AddNode("work", func(_ context.Context, s state) (state, Directive, error) {
					return s, Handoff("bg"), nil
				})
				b.AllowNoOutgoingRoute("work")
				b.SetEntryPoint("work")
				g, _ := b.Compile()
				return g, newMemoryCP[state, NoEffect](), []RunOption[state, NoEffect]{
					WithHandoffScheduler[state, NoEffect](&stubHandoffScheduler{err: errors.New("broker down")}),
				}
			},
			wantStatus:    RunStatusHandoff,
			wantResumeTok: true,
			wantErr:       ErrHandoffScheduleFailed,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			g, cp, opts := tc.setup()
			res, err := g.NewRunner(cp).Start(context.Background(), "infra-th", state{}, opts...)
			if tc.wantErr != nil {
				if !errors.Is(err, tc.wantErr) {
					t.Fatalf("expected %v, got %v res=%+v", tc.wantErr, err, res)
				}
			} else if err == nil {
				t.Fatalf("expected error, got res=%+v", res)
			}
			if res.Status != tc.wantStatus {
				t.Fatalf("expected status %s, got %s", tc.wantStatus, res.Status)
			}
			if tc.wantReason != "" && res.Reason != tc.wantReason {
				t.Fatalf("expected reason %q, got %q", tc.wantReason, res.Reason)
			}
			hasToken := res.ResumeToken.ThreadID != "" && res.ResumeToken.SnapshotRevision > 0
			if hasToken != tc.wantResumeTok {
				t.Fatalf("ResumeToken presence=%v want=%v token=%+v", hasToken, tc.wantResumeTok, res.ResumeToken)
			}
		})
	}
}

func TestStreamHandoffScheduleFailureEmitsHandoffEvent(t *testing.T) {
	t.Parallel()

	type state struct{}

	scheduler := &stubHandoffScheduler{err: errors.New("broker down")}
	b := NewGraph[state, NoEffect](func(_ state, u state) state { return u })
	b.AddNode("work", func(_ context.Context, s state) (state, Directive, error) {
		return s, Handoff("bg"), nil
	})
	b.AllowNoOutgoingRoute("work")
	b.SetEntryPoint("work")
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	runner := g.NewRunner(newMemoryCP[state, NoEffect]())
	handle, err := runner.Stream(context.Background(), "stream-sched-fail-th", state{},
		WithHandoffScheduler[state, NoEffect](scheduler),
	)
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
	events, waitErr := CollectEventsAndWait(context.Background(), handle)
	if !errors.Is(waitErr, ErrHandoffScheduleFailed) {
		t.Fatalf("expected schedule failure, got %v", waitErr)
	}

	foundHandoff := false
	for _, ev := range events {
		if ev.Type == EventHandoff {
			foundHandoff = true
		}
	}
	if !foundHandoff {
		t.Fatalf("expected EventHandoff on schedule failure, got %+v", events)
	}
	for _, ev := range events {
		if ev.Type == EventHandoff && ev.Reason != "bg" {
			t.Fatalf("expected handoff reason bg on schedule fail, got %q", ev.Reason)
		}
	}

	syncRes, syncErr := runner.Start(context.Background(), "stream-sched-fail-sync-th", state{},
		WithHandoffScheduler[state, NoEffect](scheduler),
	)
	if !errors.Is(syncErr, ErrHandoffScheduleFailed) {
		t.Fatalf("expected sync schedule failure, got %v res=%+v", syncErr, syncRes)
	}
	assertTerminalEventReasonMatchesSync(t, events, EventHandoff, syncRes.Reason)
}

func TestContextCancelSaveHardFailEventConsistency(t *testing.T) {
	t.Parallel()

	type state struct{ Ticks int }

	cp := &failingMemoryCP[state, NoEffect]{failSave: true}
	b := NewGraph[state, NoEffect](func(_ state, u state) state { return u })
	b.AddNode("loop", func(_ context.Context, s state) (state, Directive, error) {
		s.Ticks++
		return s, Completed(), nil
	})
	b.AddEdge("loop", "loop")
	b.SetEntryPoint("loop")
	g, err := b.Compile(WithMaxSteps(100))
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	handle, err := g.NewRunner(cp).Stream(ctx, "cancel-save-fail-th", state{})
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
	cancel()

	events, waitErr := CollectEventsAndWait(context.Background(), handle)
	if waitErr == nil {
		t.Fatal("expected error on cancel save hard fail")
	}

	foundCtxCanceled := false
	foundFailed := false
	for _, ev := range events {
		if ev.Type == EventContextCanceled {
			foundCtxCanceled = true
			if ev.Reason != "context_canceled_save_failed" {
				t.Fatalf("expected context_canceled_save_failed reason, got %q", ev.Reason)
			}
		}
		if ev.Type == EventFailed {
			foundFailed = true
		}
	}
	if !foundCtxCanceled || foundFailed {
		t.Fatalf("expected EventContextCanceled without EventFailed, events=%+v", events)
	}

	syncCtx, syncCancel := context.WithCancel(context.Background())
	syncCancel()
	syncRes, syncErr := g.NewRunner(cp).Start(syncCtx, "cancel-save-fail-sync-th", state{})
	if syncErr == nil {
		t.Fatalf("expected sync cancel save failure, got %+v", syncRes)
	}
	if syncRes.Reason != "context_canceled_save_failed" {
		t.Fatalf("sync reason: want context_canceled_save_failed, got %q", syncRes.Reason)
	}
	assertTerminalEventReasonMatchesSync(t, events, EventContextCanceled, syncRes.Reason)
}

func TestSuspendSaveHardFail(t *testing.T) {
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

	res, err := g.NewRunner(cp).Start(context.Background(), "suspend-save-fail-th", state{})
	if err == nil {
		t.Fatalf("expected save failure, got res=%+v", res)
	}
	if res.Status != RunStatusFailed {
		t.Fatalf("expected failed status, got %s", res.Status)
	}
	if res.ResumeToken.ThreadID != "" {
		t.Fatalf("expected zero ResumeToken, got %+v", res.ResumeToken)
	}
	if res.Reason != ReasonSuspendSaveFailed {
		t.Fatalf("expected reason %q, got %q", ReasonSuspendSaveFailed, res.Reason)
	}
}

func TestTerminalRetentionFailure(t *testing.T) {
	t.Parallel()

	type state struct{ N int }

	tests := []struct {
		name       string
		directive  func(state) Directive
		wantStatus RunStatus
		wantReason string
	}{
		{
			name: "suspend",
			directive: func(_ state) Directive {
				return Suspend("hold")
			},
			wantStatus: RunStatusSuspended,
			wantReason: "hold_retention_failed",
		},
		{
			name: "handoff",
			directive: func(_ state) Directive {
				return Handoff("bg")
			},
			wantStatus: RunStatusHandoff,
			wantReason: "bg_retention_failed",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			cp := &failingMemoryCP[state, NoEffect]{
				memoryCP:  *newMemoryCP[state, NoEffect](),
				failPrune: true,
			}
			b := NewGraph[state, NoEffect](func(_ state, u state) state { return u })
			b.AddNode("work", func(_ context.Context, s state) (state, Directive, error) {
				return s, tc.directive(s), nil
			})
			b.AllowNoOutgoingRoute("work")
			b.SetEntryPoint("work")
			g, err := b.Compile(WithRetentionLimit(2))
			if err != nil {
				t.Fatalf("compile: %v", err)
			}

			res, err := g.NewRunner(cp).Start(context.Background(), "retention-fail-th", state{})
			if err == nil {
				t.Fatalf("expected retention error, got res=%+v", res)
			}
			if res.Status != tc.wantStatus {
				t.Fatalf("expected status %s, got %s", tc.wantStatus, res.Status)
			}
			if res.Reason != tc.wantReason {
				t.Fatalf("expected reason %q, got %q", tc.wantReason, res.Reason)
			}
			if _, loadErr := cp.Load(context.Background(), "retention-fail-th"); loadErr != nil {
				t.Fatalf("checkpoint must be persisted despite retention failure: %v", loadErr)
			}
		})
	}
}

func TestTerminalRetentionFailureContextCancel(t *testing.T) {
	t.Parallel()

	type state struct{ Ticks int }

	cp := &failingMemoryCP[state, NoEffect]{
		memoryCP:  *newMemoryCP[state, NoEffect](),
		failPrune: true,
	}
	b := NewGraph[state, NoEffect](func(_ state, u state) state { return u })
	b.AddNode("loop", func(_ context.Context, s state) (state, Directive, error) {
		s.Ticks++
		return s, Completed(), nil
	})
	b.AddEdge("loop", "loop")
	b.SetEntryPoint("loop")
	g, err := b.Compile(WithMaxSteps(50), WithRetentionLimit(2))
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	handle, err := g.NewRunner(cp).Stream(ctx, "retention-cancel-th", state{})
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
	cancel()

	events, waitErr := CollectEventsAndWait(context.Background(), handle)
	if waitErr == nil {
		t.Fatal("expected retention error on context cancel")
	}

	wantReason := "context_canceled_retention_failed"
	requireTerminalEventReason(t, events, EventContextCanceled, wantReason)

	syncCtx, syncCancel := context.WithCancel(context.Background())
	syncCancel()
	syncRes, syncErr := g.NewRunner(cp).Start(syncCtx, "retention-cancel-sync-th", state{})
	if syncErr == nil {
		t.Fatalf("expected retention error on sync cancel, got %+v", syncRes)
	}
	assertTerminalEventReasonMatchesSync(t, events, EventContextCanceled, syncRes.Reason)
	if syncRes.Reason != wantReason {
		t.Fatalf("sync result reason: want %q, got %q", wantReason, syncRes.Reason)
	}
}

func TestEventCheckpointFailedCarriesSaveError(t *testing.T) {
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

	handle, err := g.NewRunner(cp).Stream(context.Background(), "cp-err-payload-th", state{},
		WithCheckpointErrorPolicy[state, NoEffect](CheckpointPolicySkipOnSaveError),
	)
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
	events, _ := CollectEventsAndWait(context.Background(), handle)
	var cpEvent *RunEvent[state, NoEffect]
	for i := range events {
		if events[i].Type == EventCheckpointFailed {
			cpEvent = &events[i]
			break
		}
	}
	if cpEvent == nil {
		t.Fatalf("expected EventCheckpointFailed, got %+v", events)
	}
	if cpEvent.Error == nil {
		t.Fatal("expected non-nil Error on EventCheckpointFailed")
	}
}

func TestPostRunCleanupRetentionFailureDoesNotFailCompleted(t *testing.T) {
	t.Parallel()

	type state struct{}

	cp := &failingMemoryCP[state, NoEffect]{
		memoryCP:  *newMemoryCP[state, NoEffect](),
		failPrune: true,
	}
	b := NewGraph[state, NoEffect](func(_ state, u state) state { return u })
	b.AddNode("done", func(_ context.Context, s state) (state, Directive, error) {
		return s, End(), nil
	})
	b.SetEntryPoint("done")
	b.AllowNoOutgoingRoute("done")
	g, err := b.Compile(WithRetentionLimit(2))
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	res, err := g.NewRunner(cp).Start(context.Background(), "postrun-ret-th", state{})
	if err != nil {
		t.Fatalf("postRunCleanup retention must not fail completed run: %v", err)
	}
	if res.Status != RunStatusCompleted {
		t.Fatalf("expected completed, got %s", res.Status)
	}
}
