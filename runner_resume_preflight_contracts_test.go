package flowy

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"
)

type preflightState struct {
	Value string
}

func preflightGraph(t *testing.T) *Graph[preflightState, NoEffect] {
	t.Helper()
	b := NewGraph[preflightState, NoEffect](func(_ preflightState, u preflightState) preflightState {
		return u
	})
	b.AddNode("work", func(_ context.Context, s preflightState) (preflightState, Directive, error) {
		s.Value = "ran"
		return s, End(), nil
	})
	b.AllowNoOutgoingRoute("work")
	b.SetEntryPoint("work")
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	return g
}

func TestEvaluateResumeReadyReturnsNormalizedDecision(t *testing.T) {
	t.Parallel()

	cp := newMemoryCP[preflightState, NoEffect]()
	rev, err := cp.Save(context.Background(), 0, Snapshot[preflightState, NoEffect]{
		ThreadID:         "preflight-ready-th",
		ExecutionPointer: "work",
		State:            preflightState{Value: "loaded"},
	})
	if err != nil {
		t.Fatalf("seed: %v", err)
	}

	decision, err := preflightGraph(t).NewRunner(cp).EvaluateResume(
		context.Background(),
		ResumeToken{ThreadID: "preflight-ready-th", SnapshotRevision: rev},
	)
	if err != nil {
		t.Fatalf("EvaluateResume: %v", err)
	}
	if !decision.Ready() || decision.Status != ResumeDecisionReady {
		t.Fatalf("expected ready decision, got %+v", decision)
	}
	if decision.ThreadID != "preflight-ready-th" || decision.SnapshotRevision != rev {
		t.Fatalf("unexpected metadata: %+v", decision)
	}
	if decision.ExecutionPointer != "work" || decision.State.Value != "loaded" {
		t.Fatalf("unexpected execution decision: %+v", decision)
	}
}

func TestEvaluateResumeStaleTokenReturnsTypedDecision(t *testing.T) {
	t.Parallel()

	cp := newMemoryCP[preflightState, NoEffect]()
	rev, err := cp.Save(context.Background(), 0, Snapshot[preflightState, NoEffect]{
		ThreadID:         "preflight-stale-th",
		ExecutionPointer: "work",
		State:            preflightState{},
	})
	if err != nil {
		t.Fatalf("seed: %v", err)
	}

	decision, err := preflightGraph(t).NewRunner(cp).EvaluateResume(
		context.Background(),
		ResumeToken{ThreadID: "preflight-stale-th", SnapshotRevision: 99},
	)
	if !errors.Is(err, ErrConcurrencyConflict) {
		t.Fatalf("expected ErrConcurrencyConflict, got decision=%+v err=%v", decision, err)
	}
	if decision.Status != ResumeDecisionStaleToken || decision.Reason != "stale_token" {
		t.Fatalf("unexpected decision: %+v", decision)
	}
	if decision.ResumeToken.ThreadID != "preflight-stale-th" ||
		decision.ResumeToken.SnapshotRevision != rev ||
		decision.SnapshotRevision != rev {
		t.Fatalf("expected current core-issued token, got %+v", decision)
	}
}

func TestEvaluateResumeInvalidSnapshotReturnsTypedDecision(t *testing.T) {
	t.Parallel()

	cp := newMemoryCP[preflightState, NoEffect]()
	rev, err := cp.Save(context.Background(), 0, Snapshot[preflightState, NoEffect]{
		ThreadID: "preflight-invalid-th",
		State:    preflightState{},
	})
	if err != nil {
		t.Fatalf("seed: %v", err)
	}

	decision, err := preflightGraph(t).NewRunner(cp).EvaluateResume(
		context.Background(),
		ResumeToken{ThreadID: "preflight-invalid-th", SnapshotRevision: rev},
	)
	if !errors.Is(err, ErrInvalidSnapshot) {
		t.Fatalf("expected ErrInvalidSnapshot, got decision=%+v err=%v", decision, err)
	}
	if decision.Status != ResumeDecisionInvalidSnapshot || decision.Reason != "invalid_snapshot" {
		t.Fatalf("unexpected decision: %+v", decision)
	}
}

func TestEvaluateResumeLoadEnvelopeErrorReturnsInvalidSnapshotDecision(t *testing.T) {
	t.Parallel()

	cp := &loadErrorCP[preflightState, NoEffect]{
		err: fmt.Errorf("%w: checkpoint: invalid stored snapshot", ErrSnapshotEnvelopeInvalid),
	}

	decision, err := preflightGraph(t).NewRunner(cp).EvaluateResume(
		context.Background(),
		ResumeToken{ThreadID: "preflight-envelope-th", SnapshotRevision: 1},
	)
	if !errors.Is(err, ErrSnapshotEnvelopeInvalid) {
		t.Fatalf("expected ErrSnapshotEnvelopeInvalid, got decision=%+v err=%v", decision, err)
	}
	if !errors.Is(err, ErrInvalidSnapshot) {
		t.Fatalf("expected ErrInvalidSnapshot classification, got %v", err)
	}
	if errors.Is(err, ErrThreadNotFound) {
		t.Fatalf("load envelope error must not be classified as thread not found: %v", err)
	}
	if decision.Status != ResumeDecisionInvalidSnapshot || decision.Reason != "invalid_snapshot" {
		t.Fatalf("unexpected decision: %+v", decision)
	}
}

func TestEvaluateResumeTransientLoadErrorReturnsLoadFailedDecision(t *testing.T) {
	t.Parallel()

	cp := &loadErrorCP[preflightState, NoEffect]{
		err: context.DeadlineExceeded,
	}

	decision, err := preflightGraph(t).NewRunner(cp).EvaluateResume(
		context.Background(),
		ResumeToken{ThreadID: "preflight-load-failed-th", SnapshotRevision: 1},
	)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected context deadline error, got decision=%+v err=%v", decision, err)
	}
	if errors.Is(err, ErrInvalidSnapshot) || errors.Is(err, ErrSnapshotEnvelopeInvalid) {
		t.Fatalf("transient load error must not be invalid snapshot: %v", err)
	}
	if decision.Status != ResumeDecisionLoadFailed || decision.Reason != string(ResumeDecisionLoadFailed) {
		t.Fatalf("unexpected decision: %+v", decision)
	}
	if decision.ThreadID != "preflight-load-failed-th" {
		t.Fatalf("decision thread id: got %q", decision.ThreadID)
	}
}

func TestEvaluateHandoffRecoveryFreshPendingReturnsTypedDecision(t *testing.T) {
	t.Parallel()

	cp := newMemoryCP[preflightState, NoEffect]()
	rev, err := cp.Save(context.Background(), 0, Snapshot[preflightState, NoEffect]{
		ThreadID:         "preflight-pending-th",
		ExecutionPointer: "work",
		State:            preflightState{},
		RunMeta: RunMetadata{
			HandoffStatus:    HandoffStatusPending,
			HandoffPendingAt: time.Now().UTC(),
		},
	})
	if err != nil {
		t.Fatalf("seed: %v", err)
	}

	decision, err := preflightGraph(t).NewRunnerWithOptions(
		cp,
		[]RunnerOption[preflightState, NoEffect]{
			WithHandoffStaleAfter[preflightState, NoEffect](time.Hour),
		},
	).EvaluateHandoffRecovery(context.Background(), "preflight-pending-th")
	if !errors.Is(err, ErrHandoffPending) {
		t.Fatalf("expected ErrHandoffPending, got decision=%+v err=%v", decision, err)
	}
	if decision.Status != ResumeDecisionHandoffPending ||
		decision.HandoffStatus != HandoffStatusPending ||
		decision.SnapshotRevision != rev {
		t.Fatalf("unexpected decision: %+v", decision)
	}
}

func TestEvaluateHandoffRecoveryRequiresCheckpointer(t *testing.T) {
	t.Parallel()

	_, err := preflightGraph(t).NewRunner(nil).EvaluateHandoffRecovery(
		context.Background(),
		"missing-cp-th",
	)
	if err == nil || err.Error() != "flowy: checkpointer is required for EvaluateHandoffRecovery" {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestEvaluateHandoffRecoveryLoadErrorReturnsStructuredDecision(t *testing.T) {
	t.Parallel()

	cp := &loadErrorCP[preflightState, NoEffect]{
		snapshot: Snapshot[preflightState, NoEffect]{
			ThreadID:         "preflight-recovery-load-failed-th",
			ExecutionPointer: "work",
			Revision:         7,
			State:            preflightState{Value: "partial"},
			RunMeta: RunMetadata{
				HandoffStatus: HandoffStatusPending,
			},
		},
		revision: 7,
		err:      context.Canceled,
	}

	decision, err := preflightGraph(t).NewRunner(cp).EvaluateHandoffRecovery(
		context.Background(),
		"preflight-recovery-load-failed-th",
	)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context canceled, got decision=%+v err=%v", decision, err)
	}
	if decision.Status != ResumeDecisionLoadFailed || decision.Reason != string(ResumeDecisionLoadFailed) {
		t.Fatalf("unexpected decision: %+v", decision)
	}
	if decision.ThreadID != "preflight-recovery-load-failed-th" ||
		decision.ResumeToken.ThreadID != "preflight-recovery-load-failed-th" ||
		decision.ResumeToken.SnapshotRevision != 7 ||
		decision.SnapshotRevision != 7 ||
		decision.ExecutionPointer != "work" ||
		decision.HandoffStatus != HandoffStatusPending {
		t.Fatalf("decision metadata not structured: %+v", decision)
	}
}

func TestEvaluateHandoffRecoveryDecisionMatrix(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		status     HandoffStatus
		pendingAt  time.Time
		opts       []RecoverStaleHandoffOption
		wantStatus ResumeDecisionStatus
		wantErr    error
	}{
		{
			name:       "orphaned",
			status:     HandoffStatusOrphaned,
			wantStatus: ResumeDecisionHandoffRecoverable,
			wantErr:    ErrHandoffOrphaned,
		},
		{
			name:       "stale pending",
			status:     HandoffStatusPending,
			pendingAt:  time.Now().Add(-time.Hour),
			opts:       []RecoverStaleHandoffOption{WithRecoverStaleAfter(time.Second)},
			wantStatus: ResumeDecisionHandoffRecoverable,
			wantErr:    ErrHandoffPending,
		},
		{
			name:       "enqueued",
			status:     HandoffStatusEnqueued,
			wantStatus: ResumeDecisionHandoffAlreadyScheduled,
			wantErr:    ErrHandoffAlreadyEnqueued,
		},
		{
			name:       "none",
			status:     HandoffStatusNone,
			wantStatus: ResumeDecisionHandoffNotRecoverable,
			wantErr:    ErrHandoffNotRecoverable,
		},
		{
			name:       "unknown",
			status:     HandoffStatus("mystery"),
			wantStatus: ResumeDecisionHandoffNotRecoverable,
			wantErr:    ErrHandoffNotRecoverable,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			cp := newMemoryCP[preflightState, NoEffect]()
			threadID := "preflight-recovery-" + tc.name
			if _, err := cp.Save(context.Background(), 0, Snapshot[preflightState, NoEffect]{
				ThreadID:         threadID,
				ExecutionPointer: "work",
				State:            preflightState{},
				RunMeta: RunMetadata{
					HandoffStatus:    tc.status,
					HandoffPendingAt: tc.pendingAt,
				},
			}); err != nil {
				t.Fatalf("seed: %v", err)
			}

			decision, err := preflightGraph(t).NewRunner(cp).EvaluateHandoffRecovery(
				context.Background(),
				threadID,
				tc.opts...,
			)
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("expected %v, got decision=%+v err=%v", tc.wantErr, decision, err)
			}
			if decision.Status != tc.wantStatus {
				t.Fatalf("expected status %s, got %+v", tc.wantStatus, decision)
			}
		})
	}
}

func TestEvaluateHandoffRecoveryRejectsInvalidResumeTarget(t *testing.T) {
	t.Parallel()

	cp := newMemoryCP[preflightState, NoEffect]()
	if _, err := cp.Save(context.Background(), 0, Snapshot[preflightState, NoEffect]{
		ThreadID:         "preflight-recovery-bad-target-th",
		ExecutionPointer: "ghost",
		State:            preflightState{},
		RunMeta: RunMetadata{
			HandoffStatus: HandoffStatusOrphaned,
		},
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	outbox := &stubHandoffOutbox{}
	runner := preflightGraph(t).NewRunnerWithOptions(cp, []RunnerOption[preflightState, NoEffect]{
		WithRunnerHandoffOutbox[preflightState, NoEffect](outbox),
	})
	decision, err := runner.EvaluateHandoffRecovery(
		context.Background(),
		"preflight-recovery-bad-target-th",
	)
	if !errors.Is(err, ErrInvalidSnapshot) || !errors.Is(err, ErrResumeStartNodeNotFound) {
		t.Fatalf("expected invalid snapshot/start node error, got decision=%+v err=%v", decision, err)
	}
	if decision.Status != ResumeDecisionInvalidSnapshot || decision.Reason != "invalid_pointer" {
		t.Fatalf("unexpected decision: %+v", decision)
	}
	result, recoverErr := runner.RecoverStaleHandoff(
		context.Background(),
		"preflight-recovery-bad-target-th",
	)
	if !errors.Is(recoverErr, ErrInvalidSnapshot) || !errors.Is(recoverErr, ErrResumeStartNodeNotFound) {
		t.Fatalf("expected recovery invalid snapshot/start node error, got result=%+v err=%v", result, recoverErr)
	}
	if len(outbox.calls) != 0 {
		t.Fatalf("bad resume target must not enqueue handoff, got %d calls", len(outbox.calls))
	}
}

func TestStreamWaitResultReturnsTerminalOutcome(t *testing.T) {
	t.Parallel()

	type state struct{}

	cp := newMemoryCP[state, NoEffect]()
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

	handle, err := g.NewRunner(cp).Stream(context.Background(), "wait-result-th", state{})
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
	outcome, waitErr := handle.WaitResult()
	if waitErr != nil {
		t.Fatalf("WaitResult: %v", waitErr)
	}
	if outcome == nil || outcome.Status != RunStatusSuspended || outcome.Reason != "hold" {
		t.Fatalf("unexpected outcome: %+v", outcome)
	}
	if outcome.ResumeToken.ThreadID != "wait-result-th" || outcome.ResumeToken.SnapshotRevision == 0 {
		t.Fatalf("expected persisted resume token, got %+v", outcome.ResumeToken)
	}
}

func TestStreamWaitResultDoesNotRequireEventDrain(t *testing.T) {
	t.Parallel()

	type state struct {
		N int
	}

	b := NewGraph[state, NoEffect](func(_ state, u state) state { return u })
	b.AddNode("loop", func(_ context.Context, s state) (state, Directive, error) {
		s.N++
		if s.N > streamEventBufferSize*2 {
			return s, End(), nil
		}
		return s, Completed(), nil
	})
	b.AddEdge("loop", "loop")
	b.SetEntryPoint("loop")
	g, err := b.Compile(WithMaxSteps(streamEventBufferSize * 4))
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	handle, err := g.NewRunner(newMemoryCP[state, NoEffect]()).Stream(
		context.Background(),
		"wait-result-no-drain-th",
		state{},
	)
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
	outcome, waitErr := handle.WaitResult()
	if waitErr != nil {
		t.Fatalf("WaitResult: %v", waitErr)
	}
	if outcome == nil || outcome.Status != RunStatusCompleted || outcome.State.N <= streamEventBufferSize {
		t.Fatalf("unexpected outcome: %+v", outcome)
	}
}

func TestStartAndStreamRejectEmptyThreadID(t *testing.T) {
	t.Parallel()

	type state struct{}

	b := NewGraph[state, NoEffect](func(_ state, u state) state { return u })
	b.AddNode("work", func(_ context.Context, s state) (state, Directive, error) {
		return s, End(), nil
	})
	b.AllowNoOutgoingRoute("work")
	b.SetEntryPoint("work")
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	if _, err := g.NewRunner(nil).Start(context.Background(), "", state{}); !errors.Is(err, ErrInvalidResumeToken) {
		t.Fatalf("Start expected ErrInvalidResumeToken, got %v", err)
	}
	if _, err := g.NewRunner(nil).Stream(context.Background(), "", state{}); !errors.Is(err, ErrInvalidResumeToken) {
		t.Fatalf("Stream expected ErrInvalidResumeToken, got %v", err)
	}
}

func TestWaitResultIncludesResumeTokenAfterContextCancelSave(t *testing.T) {
	t.Parallel()

	type state struct{}

	ready := make(chan struct{})
	cp := newMemoryCP[state, NoEffect]()
	b := NewGraph[state, NoEffect](func(_ state, u state) state { return u })
	b.AddNode("work", func(ctx context.Context, s state) (state, Directive, error) {
		close(ready)
		<-ctx.Done()
		return s, Completed(), nil
	})
	b.AllowNoOutgoingRoute("work")
	b.SetEntryPoint("work")
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	handle, err := g.NewRunner(cp).Stream(ctx, "wait-result-cancel-th", state{})
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
	<-ready
	cancel()

	outcome, waitErr := handle.WaitResult()
	if !errors.Is(waitErr, context.Canceled) {
		t.Fatalf("expected context.Canceled, got outcome=%+v err=%v", outcome, waitErr)
	}
	if outcome == nil || outcome.Status != RunStatusContextCanceled {
		t.Fatalf("unexpected outcome: %+v", outcome)
	}
	if outcome.ResumeToken.ThreadID != "wait-result-cancel-th" ||
		outcome.ResumeToken.SnapshotRevision == 0 {
		t.Fatalf("expected persisted resume token, got %+v", outcome.ResumeToken)
	}
}

type loadErrorCP[T, E any] struct {
	snapshot Snapshot[T, E]
	revision uint64
	err      error
}

func (c *loadErrorCP[T, E]) Save(
	context.Context,
	uint64,
	Snapshot[T, E],
) (uint64, error) {
	return 0, errors.New("unexpected save")
}

func (c *loadErrorCP[T, E]) Load(context.Context, string) (Snapshot[T, E], uint64, error) {
	if c.snapshot.ThreadID != "" || c.revision != 0 {
		return c.snapshot, c.revision, c.err
	}
	return Snapshot[T, E]{
		ThreadID:         "",
		ExecutionPointer: "",
		Revision:         0,
		State:            *new(T),
		RunMeta:          newRunMetadata(),
		Effects:          nil,
	}, 0, c.err
}

func (c *loadErrorCP[T, E]) GetHistory(context.Context, string, int) ([]Snapshot[T, E], error) {
	return nil, nil
}

func (c *loadErrorCP[T, E]) Prune(context.Context, string, int) error { return nil }

func (c *loadErrorCP[T, E]) Delete(context.Context, string) error { return nil }

func (c *loadErrorCP[T, E]) DeleteIfIdle(context.Context, string) error { return nil }
