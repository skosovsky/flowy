package flowy

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

type stubHandoffScheduler struct {
	mu    sync.Mutex
	calls []ResumeToken
	err   error
}

func (s *stubHandoffScheduler) ScheduleContinuation(_ context.Context, token ResumeToken) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls = append(s.calls, token)
	return s.err
}

func (s *stubHandoffScheduler) lastToken() ResumeToken {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.calls) == 0 {
		return ResumeToken{}
	}
	return s.calls[len(s.calls)-1]
}

func TestSuspendPointerResolverRewritesSave(t *testing.T) {
	t.Parallel()

	type state struct{ N int }

	b := NewGraph[state, NoEffect](func(_ state, u state) state { return u })
	b.AddNode("wait", func(_ context.Context, s state) (state, Directive, error) {
		return s, Suspend("hold"), nil
	})
	b.AddNode("router", func(_ context.Context, s state) (state, Directive, error) {
		return s, Suspend("hold"), nil
	})
	b.AllowNoOutgoingRoute("wait")
	b.AllowNoOutgoingRoute("router")
	b.SetEntryPoint("wait")
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	cp := newMemoryCP[state, NoEffect]()
	runner := g.NewRunner(cp)
	_, err = runner.Start(context.Background(), "resolver-th", state{},
		WithSuspendPointerResolver[state, NoEffect](func(_ state, _ ExecutionPointer) (ExecutionPointer, error) {
			return "router", nil
		}),
	)
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	if cp.last.ExecutionPointer != "router" {
		t.Fatalf("expected saved pointer router, got %q", cp.last.ExecutionPointer)
	}
}

func TestHandoffSchedulerFailurePreservesSnapshot(t *testing.T) {
	t.Parallel()

	type state struct{ N int }

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

	cp := newMemoryCP[state, NoEffect]()
	runner := g.NewRunner(cp)
	res, err := runner.Start(context.Background(), "schedule-fail-th", state{},
		WithHandoffScheduler[state, NoEffect](scheduler),
	)
	if !errors.Is(err, ErrHandoffScheduleFailed) {
		t.Fatalf("expected ErrHandoffScheduleFailed, got %v res=%+v", err, res)
	}
	if res == nil || res.ResumeToken.ThreadID != "schedule-fail-th" {
		t.Fatalf("expected populated ResumeToken for retry, got %+v", res)
	}
	if _, loadErr := cp.Load(context.Background(), "schedule-fail-th"); loadErr != nil {
		t.Fatalf("snapshot must be preserved after schedule failure: %v", loadErr)
	}
}

func TestHandoffSchedulerSuccess(t *testing.T) {
	t.Parallel()

	type state struct{}

	scheduler := &stubHandoffScheduler{}
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

	cp := newMemoryCP[state, NoEffect]()
	runner := g.NewRunner(cp)
	res, err := runner.Start(context.Background(), "schedule-ok-th", state{},
		WithHandoffScheduler[state, NoEffect](scheduler),
	)
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	token := scheduler.lastToken()
	if token.ThreadID != "schedule-ok-th" || token.Generation != res.ResumeToken.Generation {
		t.Fatalf("scheduler token mismatch: got %+v result token %+v", token, res.ResumeToken)
	}
}

func TestResumeEmptyToken(t *testing.T) {
	t.Parallel()

	type state struct{}
	g, err := NewGraph[state, NoEffect](func(_ state, u state) state { return u }).
		AddNode("n", func(_ context.Context, s state) (state, Directive, error) {
			return s, End(), nil
		}).
		SetEntryPoint("n").
		AllowNoOutgoingRoute("n").
		Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	_, err = g.NewRunner(newMemoryCP[state, NoEffect]()).Resume(context.Background(), ResumeToken{})
	if !errors.Is(err, ErrInvalidResumeToken) {
		t.Fatalf("expected ErrInvalidResumeToken, got %v", err)
	}
}

func TestResumeStaleToken(t *testing.T) {
	t.Parallel()

	type state struct{ N int }

	b := NewGraph[state, NoEffect](func(_ state, u state) state { return u })
	b.AddNode("step", func(_ context.Context, s state) (state, Directive, error) {
		s.N++
		if s.N < 3 {
			return s, Suspend("more"), nil
		}
		return s, End(), nil
	})
	b.AllowNoOutgoingRoute("step")
	b.SetEntryPoint("step")
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	cp := newMemoryCP[state, NoEffect]()
	runner := g.NewRunner(cp)
	first, err := runner.Start(context.Background(), "stale-th", state{})
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	staleToken := first.ResumeToken

	second, err := runner.Resume(context.Background(), staleToken)
	if err != nil {
		t.Fatalf("first resume: %v", err)
	}
	if second.Status != RunStatusSuspended {
		t.Fatalf("expected second suspend, got %s", second.Status)
	}

	_, err = runner.Resume(context.Background(), staleToken)
	if !errors.Is(err, ErrStaleResumeToken) {
		t.Fatalf("expected ErrStaleResumeToken on stale token, got %v", err)
	}
}

func TestResumeViaRunResultToken(t *testing.T) {
	t.Parallel()

	type state struct {
		Approved  bool
		Confirmed bool
	}

	b := NewGraph[state, NoEffect](func(_ state, u state) state { return u })
	b.AddNode("wait", func(_ context.Context, s state) (state, Directive, error) {
		if !s.Approved {
			return s, Suspend("input"), nil
		}
		if !s.Confirmed {
			return s, Suspend("confirm"), nil
		}
		return s, End(), nil
	})
	b.AllowNoOutgoingRoute("wait")
	b.SetEntryPoint("wait")
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	cp := newMemoryCP[state, NoEffect]()
	runner := g.NewRunner(cp)
	suspended, err := runner.Start(context.Background(), "hitl-th", state{})
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	if suspended.ResumeToken.ThreadID == "" {
		t.Fatal("expected ResumeToken on suspend result")
	}

	resumed, err := runner.Resume(context.Background(), suspended.ResumeToken,
		WithStateOverlay[state, NoEffect](state{Approved: true}, func(base, overlay state) state {
			base.Approved = overlay.Approved
			return base
		}),
	)
	if err != nil {
		t.Fatalf("resume: %v", err)
	}
	if resumed.Status != RunStatusSuspended || !resumed.State.Approved {
		t.Fatalf("expected second suspend after approval, got %+v", resumed)
	}

	_, err = runner.Resume(context.Background(), suspended.ResumeToken)
	if !errors.Is(err, ErrStaleResumeToken) {
		t.Fatalf("expected stale token after revision bump, got %v", err)
	}
}

func TestCheckpointSoftWarnTerminalSave(t *testing.T) {
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

	runner := g.NewRunner(cp)
	handle, err := runner.Stream(context.Background(), "soft-warn-th", state{},
		WithCheckpointErrorPolicy[state, NoEffect](CheckpointPolicySoftWarn),
	)
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
	events := collectEvents(t, handle.Events(), 2*time.Second)
	if err := handle.Done(); err != nil {
		t.Fatalf("done: %v", err)
	}

	foundCheckpointFailed := false
	foundSuspended := false
	for _, ev := range events {
		if ev.Type == EventCheckpointFailed {
			foundCheckpointFailed = true
		}
		if ev.Type == EventSuspended {
			foundSuspended = true
		}
	}
	if !foundCheckpointFailed {
		t.Fatalf("expected EventCheckpointFailed in stream, got %+v", events)
	}
	if !foundSuspended {
		t.Fatalf("expected EventSuspended despite save failure, got %+v", events)
	}
	foundSuspendReason := false
	for _, ev := range events {
		if ev.Type == EventSuspended {
			if ev.Reason != "suspended_checkpoint_skipped" {
				t.Fatalf("expected suspended_checkpoint_skipped reason, got %q", ev.Reason)
			}
			foundSuspendReason = true
		}
	}
	if !foundSuspendReason {
		t.Fatal("expected suspended event with checkpoint_skipped reason")
	}
}

type failingMemoryCP[T, E any] struct {
	memoryCP[T, E]

	failSave  bool
	failPrune bool
}

func (f *failingMemoryCP[T, E]) Save(_ context.Context, snapshot Snapshot[T, E]) error {
	if f.failSave {
		return errors.New("save failed")
	}
	return f.memoryCP.Save(context.Background(), snapshot)
}

func (f *failingMemoryCP[T, E]) Prune(_ context.Context, threadID string, retainCount int) error {
	if f.failPrune {
		return errors.New("prune failed")
	}
	return f.memoryCP.Prune(context.Background(), threadID, retainCount)
}

func TestSoftWarnDoesNotEmitResumeToken(t *testing.T) {
	t.Parallel()

	type state struct{}

	cp := &failingMemoryCP[state, NoEffect]{failSave: true}
	b := NewGraph[state, NoEffect](func(_ state, u state) state { return u })
	b.AddNode("wait", func(_ context.Context, s state) (state, Directive, error) {
		return s, Suspend("hold"), nil
	})
	b.AddNode("work", func(_ context.Context, s state) (state, Directive, error) {
		return s, Handoff("bg"), nil
	})
	b.AllowNoOutgoingRoute("wait")
	b.AllowNoOutgoingRoute("work")
	b.SetEntryPoint("wait")
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	runner := g.NewRunner(cp)
	suspended, err := runner.Start(context.Background(), "soft-no-token-suspend", state{},
		WithCheckpointErrorPolicy[state, NoEffect](CheckpointPolicySoftWarn),
	)
	if err != nil {
		t.Fatalf("suspend start: %v", err)
	}
	if suspended.ResumeToken.ThreadID != "" || suspended.ResumeToken.Generation != 0 {
		t.Fatalf("expected zero ResumeToken on soft warn suspend, got %+v", suspended.ResumeToken)
	}
	if suspended.Reason != "suspended_checkpoint_skipped" {
		t.Fatalf("expected suspended_checkpoint_skipped reason, got %q", suspended.Reason)
	}

	handoffG, err := NewGraph[state, NoEffect](func(_ state, u state) state { return u }).
		AddNode("work", func(_ context.Context, s state) (state, Directive, error) {
			return s, Handoff("bg"), nil
		}).
		AllowNoOutgoingRoute("work").
		SetEntryPoint("work").
		Compile()
	if err != nil {
		t.Fatalf("compile handoff graph: %v", err)
	}

	scheduler := &stubHandoffScheduler{}
	handoffRes, err := handoffG.NewRunner(cp).Start(context.Background(), "soft-no-token-handoff", state{},
		WithCheckpointErrorPolicy[state, NoEffect](CheckpointPolicySoftWarn),
		WithHandoffScheduler[state, NoEffect](scheduler),
	)
	if err != nil {
		t.Fatalf("handoff start: %v", err)
	}
	if handoffRes.ResumeToken.ThreadID != "" || handoffRes.ResumeToken.Generation != 0 {
		t.Fatalf("expected zero ResumeToken on soft warn handoff, got %+v", handoffRes.ResumeToken)
	}
	if handoffRes.Reason != "handoff_checkpoint_skipped" {
		t.Fatalf("expected handoff_checkpoint_skipped reason, got %q", handoffRes.Reason)
	}
	if len(scheduler.calls) != 0 {
		t.Fatalf("scheduler must not run without persisted handoff save, calls=%d", len(scheduler.calls))
	}
}

func TestCheckpointSoftWarnWithoutStream(t *testing.T) {
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

	res, err := g.NewRunner(cp).Start(context.Background(), "soft-sync-th", state{},
		WithCheckpointErrorPolicy[state, NoEffect](CheckpointPolicySoftWarn),
	)
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	if res.Status != RunStatusSuspended {
		t.Fatalf("expected suspended, got %s", res.Status)
	}
	if res.ResumeToken.ThreadID != "" {
		t.Fatalf("expected zero ResumeToken without persisted save, got %+v", res.ResumeToken)
	}
}

func TestCheckpointSoftWarnHandoffSave(t *testing.T) {
	t.Parallel()

	type state struct{}

	cp := &failingMemoryCP[state, NoEffect]{failSave: true}
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

	scheduler := &stubHandoffScheduler{}
	handle, err := g.NewRunner(cp).Stream(context.Background(), "soft-handoff-th", state{},
		WithCheckpointErrorPolicy[state, NoEffect](CheckpointPolicySoftWarn),
		WithHandoffScheduler[state, NoEffect](scheduler),
	)
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
	events := collectEvents(t, handle.Events(), 2*time.Second)
	if err := handle.Done(); err != nil {
		t.Fatalf("done: %v", err)
	}

	foundCheckpoint := false
	foundHandoff := false
	for _, ev := range events {
		if ev.Type == EventCheckpointFailed {
			foundCheckpoint = true
		}
		if ev.Type == EventHandoff {
			foundHandoff = true
		}
	}
	if !foundCheckpoint || !foundHandoff {
		t.Fatalf("expected checkpoint_failed and handoff events, got %+v", events)
	}
	for _, ev := range events {
		if ev.Type == EventHandoff && ev.Reason != "handoff_checkpoint_skipped" {
			t.Fatalf("expected handoff_checkpoint_skipped reason, got %q", ev.Reason)
		}
	}
	if len(scheduler.calls) != 0 {
		t.Fatal("scheduler must not run when handoff save was soft-warned")
	}
}

func TestCheckpointSoftWarnContextCancel(t *testing.T) {
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
	cancel()

	res, err := g.NewRunner(cp).Start(ctx, "soft-cancel-th", state{},
		WithCheckpointErrorPolicy[state, NoEffect](CheckpointPolicySoftWarn),
	)
	if err == nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context canceled, got res=%+v err=%v", res, err)
	}
	if res.Reason != "context_canceled_checkpoint_skipped" {
		t.Fatalf("expected checkpoint_skipped reason, got %q", res.Reason)
	}
	if _, loadErr := cp.Load(context.Background(), "soft-cancel-th"); !errors.Is(loadErr, ErrThreadNotFound) {
		t.Fatalf("expected no persisted snapshot on soft warn cancel, got %v", loadErr)
	}
}

func TestSuspendPointerResolverInvalidPointer(t *testing.T) {
	t.Parallel()

	type state struct{}

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

	cp := newMemoryCP[state, NoEffect]()
	_, err = g.NewRunner(cp).Start(context.Background(), "invalid-ptr-th", state{},
		WithSuspendPointerResolver[state, NoEffect](func(_ state, _ ExecutionPointer) (ExecutionPointer, error) {
			return "ghost", nil
		}),
	)
	if err == nil {
		t.Fatal("expected error for invalid suspend pointer")
	}
	if _, loadErr := cp.Load(context.Background(), "invalid-ptr-th"); !errors.Is(loadErr, ErrThreadNotFound) {
		t.Fatalf("snapshot must not be saved on invalid pointer, load err=%v", loadErr)
	}
}

func TestSuspendPointerResolverOnHandoff(t *testing.T) {
	t.Parallel()

	type state struct{}

	b := NewGraph[state, NoEffect](func(_ state, u state) state { return u })
	b.AddNode("work", func(_ context.Context, s state) (state, Directive, error) {
		return s, Handoff("bg"), nil
	})
	b.AddNode("router", func(_ context.Context, s state) (state, Directive, error) {
		return s, Completed(), nil
	})
	b.AllowNoOutgoingRoute("work")
	b.AllowNoOutgoingRoute("router")
	b.SetEntryPoint("work")
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	cp := newMemoryCP[state, NoEffect]()
	_, err = g.NewRunner(cp).Start(context.Background(), "handoff-resolver-th", state{},
		WithSuspendPointerResolver[state, NoEffect](func(_ state, _ ExecutionPointer) (ExecutionPointer, error) {
			return "router", nil
		}),
	)
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	if cp.last.ExecutionPointer != "router" {
		t.Fatalf("expected saved pointer router, got %q", cp.last.ExecutionPointer)
	}
}

func TestTerminalStatusOnInfraFailure(t *testing.T) {
	t.Parallel()

	type state struct{}

	tests := []struct {
		name          string
		setup         func() (*Graph[state, NoEffect], Checkpointer[state, NoEffect], []RunOption[state, NoEffect])
		wantStatus    RunStatus
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
			hasToken := res.ResumeToken.ThreadID != "" && res.ResumeToken.Generation > 0
			if hasToken != tc.wantResumeTok {
				t.Fatalf("ResumeToken presence=%v want=%v token=%+v", hasToken, tc.wantResumeTok, res.ResumeToken)
			}
		})
	}
}

func TestHandoffSchedulerDoesNotDeleteOnScheduleFailure(t *testing.T) {
	t.Parallel()

	type state struct{}

	baseCP := newMemoryCP[state, NoEffect]()
	cp := &deleteSpyCP[state, NoEffect]{memoryCP: *baseCP}
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

	_, err = g.NewRunner(cp).Start(context.Background(), "no-delete-th", state{},
		WithHandoffScheduler[state, NoEffect](scheduler),
	)
	if !errors.Is(err, ErrHandoffScheduleFailed) {
		t.Fatalf("expected schedule failure, got %v", err)
	}
	if cp.deleteCalls != 0 {
		t.Fatalf("Delete must not run on schedule failure, calls=%d", cp.deleteCalls)
	}
}

func TestStreamResumeEmptyToken(t *testing.T) {
	t.Parallel()

	type state struct{}
	g, err := NewGraph[state, NoEffect](func(_ state, u state) state { return u }).
		AddNode("n", func(_ context.Context, s state) (state, Directive, error) {
			return s, End(), nil
		}).
		SetEntryPoint("n").
		AllowNoOutgoingRoute("n").
		Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	_, err = g.NewRunner(newMemoryCP[state, NoEffect]()).StreamResume(context.Background(), ResumeToken{})
	if !errors.Is(err, ErrInvalidResumeToken) {
		t.Fatalf("expected ErrInvalidResumeToken, got %v", err)
	}
}

func TestAsNodeSuspendNotResumable(t *testing.T) {
	t.Parallel()

	type innerState struct{ Step int }
	type outerState struct{ Step int }

	innerBuilder := NewGraph[innerState, NoEffect](func(_ innerState, u innerState) innerState { return u })
	innerBuilder.AddNode("work", func(_ context.Context, s innerState) (innerState, Directive, error) {
		s.Step++
		if s.Step < 2 {
			return s, Suspend("inner-wait"), nil
		}
		return s, End(), nil
	})
	innerBuilder.SetEntryPoint("work")
	innerBuilder.AllowNoOutgoingRoute("work")
	inner, err := innerBuilder.Compile()
	if err != nil {
		t.Fatalf("compile inner: %v", err)
	}

	outerBuilder := NewGraph[outerState, NoEffect](func(_ outerState, u outerState) outerState { return u })
	outerBuilder.AddNode("inline", func(ctx context.Context, s outerState) (outerState, Directive, error) {
		_, d, runErr := inner.AsNode()(ctx, innerState{})
		return s, d, runErr
	})
	outerBuilder.SetEntryPoint("inline")
	outerBuilder.AllowNoOutgoingRoute("inline")
	outer, err := outerBuilder.Compile()
	if err != nil {
		t.Fatalf("compile outer: %v", err)
	}

	cp := newMemoryCP[outerState, NoEffect]()
	runner := outer.NewRunner(cp)

	first, err := runner.Start(context.Background(), "asnode-th", outerState{})
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	if first.Status != RunStatusSuspended {
		t.Fatalf("expected suspended, got %s", first.Status)
	}
	if first.ResumeToken.ThreadID == "" {
		t.Fatal("expected parent ResumeToken on suspend")
	}

	second, err := runner.Resume(context.Background(), first.ResumeToken)
	if err != nil {
		t.Fatalf("parent resume: %v", err)
	}
	if second.Status != RunStatusSuspended {
		t.Fatalf("AsNode inner suspend is not resumable; inline graph restarts, got %s", second.Status)
	}
}

type deleteSpyCP[T, E any] struct {
	memoryCP[T, E]

	deleteCalls int
}

func (d *deleteSpyCP[T, E]) Delete(_ context.Context, threadID string) error {
	d.deleteCalls++
	return d.memoryCP.Delete(context.Background(), threadID)
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
	events := collectEvents(t, handle.Events(), 2*time.Second)
	if err := handle.Done(); !errors.Is(err, ErrHandoffScheduleFailed) {
		t.Fatalf("expected schedule failure, got %v", err)
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

	events := collectEvents(t, handle.Events(), 2*time.Second)
	if err := handle.Done(); err == nil {
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
}

func TestSuspendPointerResolverReturnsError(t *testing.T) {
	t.Parallel()

	type state struct{}

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

	cp := newMemoryCP[state, NoEffect]()
	_, err = g.NewRunner(cp).Start(context.Background(), "resolver-err-th", state{},
		WithSuspendPointerResolver[state, NoEffect](func(_ state, _ ExecutionPointer) (ExecutionPointer, error) {
			return "", errors.New("resolver rejected")
		}),
	)
	if err == nil {
		t.Fatal("expected resolver error")
	}
	if _, loadErr := cp.Load(context.Background(), "resolver-err-th"); !errors.Is(loadErr, ErrThreadNotFound) {
		t.Fatalf("snapshot must not be saved, load err=%v", loadErr)
	}
}

type countingFailingMemoryCP[T, E any] struct {
	memoryCP[T, E]

	saveCount int
}

func (c *countingFailingMemoryCP[T, E]) Save(_ context.Context, snapshot Snapshot[T, E]) error {
	c.saveCount++
	if c.saveCount > 1 {
		return errors.New("save failed")
	}
	return c.memoryCP.Save(context.Background(), snapshot)
}

func TestCheckpointSoftWarnOnStreamResume(t *testing.T) {
	t.Parallel()

	type state struct{}

	cp := &countingFailingMemoryCP[state, NoEffect]{memoryCP: *newMemoryCP[state, NoEffect]()}
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

	runner := g.NewRunner(cp)
	first, err := runner.Start(context.Background(), "stream-resume-soft-th", state{})
	if err != nil {
		t.Fatalf("start: %v", err)
	}

	handle, err := runner.StreamResume(context.Background(), first.ResumeToken,
		WithCheckpointErrorPolicy[state, NoEffect](CheckpointPolicySoftWarn),
	)
	if err != nil {
		t.Fatalf("stream resume: %v", err)
	}
	events := collectEvents(t, handle.Events(), 2*time.Second)
	if err := handle.Done(); err != nil {
		t.Fatalf("done: %v", err)
	}

	foundCheckpoint := false
	for _, ev := range events {
		if ev.Type == EventCheckpointFailed {
			foundCheckpoint = true
		}
	}
	if !foundCheckpoint {
		t.Fatalf("expected EventCheckpointFailed on StreamResume SoftWarn, got %+v", events)
	}
}

func TestStreamResumeStaleToken(t *testing.T) {
	t.Parallel()

	type state struct{ N int }

	b := NewGraph[state, NoEffect](func(_ state, u state) state { return u })
	b.AddNode("step", func(_ context.Context, s state) (state, Directive, error) {
		s.N++
		if s.N < 3 {
			return s, Suspend("more"), nil
		}
		return s, End(), nil
	})
	b.AllowNoOutgoingRoute("step")
	b.SetEntryPoint("step")
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	cp := newMemoryCP[state, NoEffect]()
	runner := g.NewRunner(cp)
	first, err := runner.Start(context.Background(), "stream-stale-th", state{})
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	staleToken := first.ResumeToken

	second, err := runner.Resume(context.Background(), staleToken)
	if err != nil {
		t.Fatalf("resume: %v", err)
	}
	if second.Status != RunStatusSuspended {
		t.Fatalf("expected suspended, got %s", second.Status)
	}

	_, err = runner.StreamResume(context.Background(), staleToken)
	if !errors.Is(err, ErrStaleResumeToken) {
		t.Fatalf("expected stale token on StreamResume, got %v", err)
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

func TestSubgraphDoesNotInheritHandoffScheduler(t *testing.T) {
	t.Parallel()

	type childState struct{}
	type parentState struct{}

	scheduler := &stubHandoffScheduler{}
	subBuilder := NewGraph[childState, NoEffect](func(_ childState, u childState) childState { return u })
	subBuilder.AddNode("work", func(_ context.Context, s childState) (childState, Directive, error) {
		return s, Suspend("inner-wait"), nil
	})
	subBuilder.AllowNoOutgoingRoute("work")
	subBuilder.SetEntryPoint("work")
	sub, err := subBuilder.Compile()
	if err != nil {
		t.Fatalf("compile subgraph: %v", err)
	}

	parentBuilder := NewGraph[parentState, NoEffect](func(_ parentState, u parentState) parentState { return u })
	parentBuilder.AddNode("sub", SubgraphNode(
		sub,
		func(_ parentState) childState { return childState{} },
		func(s parentState, _ childState) parentState { return s },
	))
	parentBuilder.SetEntryPoint("sub")
	parentBuilder.AllowNoOutgoingRoute("sub")
	parentGraph, err := parentBuilder.Compile()
	if err != nil {
		t.Fatalf("compile parent: %v", err)
	}

	res, err := parentGraph.NewRunner(newMemoryCP[parentState, NoEffect]()).
		Start(context.Background(), "sub-scheduler-th", parentState{},
			WithHandoffScheduler[parentState, NoEffect](scheduler),
		)
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	if res.Status != RunStatusSuspended {
		t.Fatalf("expected parent suspended from inner suspend, got %s", res.Status)
	}
	if len(scheduler.calls) != 0 {
		t.Fatalf("parent scheduler must not run on inner subgraph suspend, calls=%d", len(scheduler.calls))
	}
}
