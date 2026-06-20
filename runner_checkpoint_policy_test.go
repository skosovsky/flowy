package flowy

import (
	"context"
	"errors"
	"strings"
	"testing"
)

func TestCheckpointSkipOnSaveErrorTerminalSave(t *testing.T) {
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
	handle, err := runner.Stream(context.Background(), "skip-on-save-stream-th", state{},
		WithCheckpointErrorPolicy[state, NoEffect](CheckpointPolicySkipOnSaveError),
	)
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
	events, waitErr := CollectEventsAndWait(context.Background(), handle)
	if waitErr != nil {
		t.Fatalf("Wait: %v", waitErr)
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

	assertSkipOnSaveErrorSuspendStreamSyncReasonMatch(t, runner, state{}, events)
}

type failingMemoryCP[T, E any] struct {
	*memoryCP[T, E]

	failSave  bool
	failPrune bool
}

func (f *failingMemoryCP[T, E]) ensureMemoryCP() {
	if f.memoryCP == nil {
		f.memoryCP = newMemoryCP[T, E]()
	}
}

func (f *failingMemoryCP[T, E]) Save(
	_ context.Context,
	expectedRevision uint64,
	snapshot Snapshot[T, E],
) (uint64, error) {
	if f.failSave {
		return 0, errors.New("save failed")
	}
	f.ensureMemoryCP()
	return f.memoryCP.Save(context.Background(), expectedRevision, snapshot)
}

func (f *failingMemoryCP[T, E]) Load(ctx context.Context, threadID string) (Snapshot[T, E], uint64, error) {
	f.ensureMemoryCP()
	return f.memoryCP.Load(ctx, threadID)
}

func (f *failingMemoryCP[T, E]) Prune(_ context.Context, threadID string, retainCount int) error {
	if f.failPrune {
		return errors.New("prune failed")
	}
	f.ensureMemoryCP()
	return f.memoryCP.Prune(context.Background(), threadID, retainCount)
}

func TestSkipOnSaveErrorDoesNotEmitResumeToken(t *testing.T) {
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
		WithCheckpointErrorPolicy[state, NoEffect](CheckpointPolicySkipOnSaveError),
	)
	if err != nil {
		t.Fatalf("suspend start: %v", err)
	}
	if suspended.ResumeToken.ThreadID != "" || suspended.ResumeToken.SnapshotRevision != 0 {
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

	outbox := &stubHandoffOutbox{}
	handoffRes, err := handoffG.NewRunner(cp).Start(context.Background(), "soft-no-token-handoff", state{},
		WithCheckpointErrorPolicy[state, NoEffect](CheckpointPolicySkipOnSaveError),
		WithHandoffOutbox[state, NoEffect](outbox),
	)
	if err != nil {
		t.Fatalf("handoff start: %v", err)
	}
	if handoffRes.ResumeToken.ThreadID != "" || handoffRes.ResumeToken.SnapshotRevision != 0 {
		t.Fatalf("expected zero ResumeToken on soft warn handoff, got %+v", handoffRes.ResumeToken)
	}
	if handoffRes.Reason != "handoff_checkpoint_skipped" {
		t.Fatalf("expected handoff_checkpoint_skipped reason, got %q", handoffRes.Reason)
	}
	if len(outbox.calls) != 0 {
		t.Fatalf("outbox must not run without persisted handoff save, calls=%d", len(outbox.calls))
	}
	if handoffRes.RunMeta.HandoffStatus != HandoffStatusNone {
		t.Fatalf("expected none handoff status on skip-on-save, got %q", handoffRes.RunMeta.HandoffStatus)
	}
	if !handoffRes.RunMeta.HandoffPendingAt.IsZero() {
		t.Fatalf("expected zero HandoffPendingAt on skip-on-save, got %v", handoffRes.RunMeta.HandoffPendingAt)
	}
}

func TestCheckpointSkipOnSaveErrorWithoutStream(t *testing.T) {
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
		WithCheckpointErrorPolicy[state, NoEffect](CheckpointPolicySkipOnSaveError),
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
	if res.Reason != ReasonSuspendedCheckpointSkipped {
		t.Fatalf("expected %q reason, got %q", ReasonSuspendedCheckpointSkipped, res.Reason)
	}
}

func TestCheckpointSkipOnSaveErrorHandoffSave(t *testing.T) {
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

	outbox := &stubHandoffOutbox{}
	handle, err := g.NewRunner(cp).Stream(context.Background(), "soft-handoff-th", state{},
		WithCheckpointErrorPolicy[state, NoEffect](CheckpointPolicySkipOnSaveError),
		WithHandoffOutbox[state, NoEffect](outbox),
	)
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
	events, waitErr := CollectEventsAndWait(context.Background(), handle)
	if waitErr != nil {
		t.Fatalf("Wait: %v", waitErr)
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
	if len(outbox.calls) != 0 {
		t.Fatal("outbox must not run when handoff save was skipped on save error")
	}
	syncRes, syncErr := g.NewRunner(cp).Start(context.Background(), "soft-handoff-sync-th", state{},
		WithCheckpointErrorPolicy[state, NoEffect](CheckpointPolicySkipOnSaveError),
		WithHandoffOutbox[state, NoEffect](outbox),
	)
	if syncErr != nil {
		t.Fatalf("sync handoff start: %v", syncErr)
	}
	if syncRes.RunMeta.HandoffStatus != HandoffStatusNone {
		t.Fatalf("expected none handoff status on skip-on-save sync, got %q", syncRes.RunMeta.HandoffStatus)
	}
	if !syncRes.RunMeta.HandoffPendingAt.IsZero() {
		t.Fatalf("expected zero HandoffPendingAt on skip-on-save sync, got %v", syncRes.RunMeta.HandoffPendingAt)
	}
}

func TestCheckpointSkipOnSaveErrorContextCancel(t *testing.T) {
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
		WithCheckpointErrorPolicy[state, NoEffect](CheckpointPolicySkipOnSaveError),
	)
	if err == nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context canceled, got res=%+v err=%v", res, err)
	}
	if res.Reason != "context_canceled_checkpoint_skipped" {
		t.Fatalf("expected checkpoint_skipped reason, got %q", res.Reason)
	}
	if _, _, loadErr := cp.Load(context.Background(), "soft-cancel-th"); !errors.Is(loadErr, ErrThreadNotFound) {
		t.Fatalf("expected no persisted snapshot on soft warn cancel, got %v", loadErr)
	}
}

func TestSkipOnSaveErrorEventOrderingCheckpointBeforeTerminal(t *testing.T) {
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

	handle, err := g.NewRunner(cp).Stream(context.Background(), "skip-on-save-order-th", state{},
		WithCheckpointErrorPolicy[state, NoEffect](CheckpointPolicySkipOnSaveError),
	)
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
	events, waitErr := CollectEventsAndWait(context.Background(), handle)
	_ = waitErr

	checkpointIdx, terminalIdx := -1, -1
	for i, ev := range events {
		if ev.Type == EventCheckpointFailed {
			checkpointIdx = i
		}
		if ev.Type == EventSuspended {
			terminalIdx = i
		}
	}
	if checkpointIdx < 0 || terminalIdx < 0 {
		t.Fatalf("expected checkpoint and suspended events, got %+v", events)
	}
	if checkpointIdx >= terminalIdx {
		t.Fatalf("checkpoint_failed must precede suspended: checkpoint=%d suspended=%d", checkpointIdx, terminalIdx)
	}
}

func TestSkipOnSaveErrorCheckpointSkippedWithRetentionFailed(t *testing.T) {
	t.Parallel()

	type state struct{}

	cp := &failingMemoryCP[state, NoEffect]{
		memoryCP:  newMemoryCP[state, NoEffect](),
		failSave:  true,
		failPrune: true,
	}
	b := NewGraph[state, NoEffect](func(_ state, u state) state { return u })
	b.AddNode("wait", func(_ context.Context, s state) (state, Directive, error) {
		return s, Suspend("hold"), nil
	})
	b.AllowNoOutgoingRoute("wait")
	b.SetEntryPoint("wait")
	g, err := b.Compile(WithRetentionLimit(2))
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	res, err := g.NewRunner(cp).Start(context.Background(), "skip-on-save-retention-th", state{},
		WithCheckpointErrorPolicy[state, NoEffect](CheckpointPolicySkipOnSaveError),
	)
	if err != nil {
		t.Fatalf("unexpected error on skip-on-save without persisted save: %v", err)
	}
	if res.Reason != ReasonSuspendedCheckpointSkipped {
		t.Fatalf("expected %q without retention suffix, got %q", ReasonSuspendedCheckpointSkipped, res.Reason)
	}
}

func TestCheckpointSkipOnSaveErrorOnResumeStream(t *testing.T) {
	t.Parallel()

	type state struct{}

	cp := &countingFailingMemoryCP[state, NoEffect]{memoryCP: newMemoryCP[state, NoEffect]()}
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

	handle, err := runner.ResumeStream(context.Background(), first.ResumeToken,
		WithCheckpointErrorPolicy[state, NoEffect](CheckpointPolicySkipOnSaveError),
	)
	if err != nil {
		t.Fatalf("stream resume: %v", err)
	}
	events, waitErr := CollectEventsAndWait(context.Background(), handle)
	if waitErr != nil {
		t.Fatalf("Wait: %v", waitErr)
	}

	foundCheckpoint := false
	for _, ev := range events {
		if ev.Type == EventCheckpointFailed {
			foundCheckpoint = true
		}
	}
	if !foundCheckpoint {
		t.Fatalf("expected EventCheckpointFailed on ResumeStream SkipOnSaveError, got %+v", events)
	}
}

func TestCheckpointSkipOnSaveErrorOnResumeStreamReason(t *testing.T) {
	t.Parallel()

	type state struct{}

	cp := &countingFailingMemoryCP[state, NoEffect]{memoryCP: newMemoryCP[state, NoEffect]()}
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
	first, err := runner.Start(context.Background(), "stream-resume-reason-th", state{})
	if err != nil {
		t.Fatalf("start: %v", err)
	}

	handle, err := runner.ResumeStream(context.Background(), first.ResumeToken,
		WithCheckpointErrorPolicy[state, NoEffect](CheckpointPolicySkipOnSaveError),
	)
	if err != nil {
		t.Fatalf("stream resume: %v", err)
	}
	events, waitErr := CollectEventsAndWait(context.Background(), handle)
	if waitErr != nil {
		t.Fatalf("Wait: %v", waitErr)
	}

	for _, ev := range events {
		if ev.Type == EventSuspended && ev.Reason != ReasonSuspendedCheckpointSkipped {
			t.Fatalf("expected %q on EventSuspended, got %q", ReasonSuspendedCheckpointSkipped, ev.Reason)
		}
	}
}

func TestStreamSkipOnSaveErrorContextCancel(t *testing.T) {
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
	g, err := b.Compile(WithMaxSteps(50))
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	handle, err := g.NewRunner(cp).Stream(ctx, "soft-cancel-th", state{},
		WithCheckpointErrorPolicy[state, NoEffect](CheckpointPolicySkipOnSaveError),
	)
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
	cancel()

	events, waitErr := CollectEventsAndWait(context.Background(), handle)
	if waitErr == nil {
		t.Fatal("expected context canceled error")
	}

	foundCheckpoint := false
	foundCtxCanceled := false
	for _, ev := range events {
		if ev.Type == EventCheckpointFailed {
			foundCheckpoint = true
		}
		if ev.Type == EventContextCanceled {
			foundCtxCanceled = true
			if ev.Reason != ReasonContextCanceledCheckpointSkipped {
				t.Fatalf("expected %q, got %q", ReasonContextCanceledCheckpointSkipped, ev.Reason)
			}
		}
	}
	if !foundCheckpoint || !foundCtxCanceled {
		t.Fatalf("expected checkpoint_failed and context_canceled events, got %+v", events)
	}
	if _, _, loadErr := cp.Load(context.Background(), "soft-cancel-th"); !errors.Is(loadErr, ErrThreadNotFound) {
		t.Fatalf("snapshot must not be saved on ctx cancel soft warn, load err=%v", loadErr)
	}
}

func TestSyncResumeSkipOnSaveErrorReason(t *testing.T) {
	t.Parallel()

	type state struct{}

	cp := &countingFailingMemoryCP[state, NoEffect]{memoryCP: newMemoryCP[state, NoEffect]()}
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
	first, err := runner.Start(context.Background(), "sync-resume-soft-th", state{})
	if err != nil {
		t.Fatalf("start: %v", err)
	}

	second, err := runner.Resume(context.Background(), first.ResumeToken,
		WithCheckpointErrorPolicy[state, NoEffect](CheckpointPolicySkipOnSaveError),
	)
	if err != nil {
		t.Fatalf("resume: %v", err)
	}
	if second.Reason != ReasonSuspendedCheckpointSkipped {
		t.Fatalf("expected %q, got %q", ReasonSuspendedCheckpointSkipped, second.Reason)
	}
	if second.ResumeToken.ThreadID != "" {
		t.Fatalf("expected zero ResumeToken on soft warn, got %+v", second.ResumeToken)
	}
}

func TestSkipOnSaveErrorCheckpointFailedUsesResolvedPointer(t *testing.T) {
	t.Parallel()

	type state struct{}

	cp := &failingMemoryCP[state, NoEffect]{failSave: true}
	b := NewGraph[state, NoEffect](func(_ state, u state) state { return u })
	b.AddNode("wait", func(_ context.Context, s state) (state, Directive, error) {
		return s, Suspend("hold", ResumeAt("router")), nil
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

	skipOnSave := WithCheckpointErrorPolicy[state, NoEffect](CheckpointPolicySkipOnSaveError)

	handle, err := g.NewRunner(cp).Stream(
		context.Background(), "soft-ptr-th", state{}, skipOnSave,
	)
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
	events, waitErr := CollectEventsAndWait(context.Background(), handle)
	if waitErr != nil {
		t.Fatalf("Wait: %v", waitErr)
	}

	var checkpointPtr, suspendedPtr string
	for _, ev := range events {
		if ev.Type == EventCheckpointFailed {
			checkpointPtr = string(ev.ExecutionPointer)
		}
		if ev.Type == EventSuspended {
			suspendedPtr = string(ev.ExecutionPointer)
		}
	}
	if checkpointPtr != "router" {
		t.Fatalf("checkpoint event pointer: want router, got %q events=%+v", checkpointPtr, events)
	}
	if suspendedPtr != "router" {
		t.Fatalf("suspended event pointer: want router, got %q events=%+v", suspendedPtr, events)
	}
}

func TestSkipOnSaveErrorPersistedThenRetentionFailed(t *testing.T) {
	t.Parallel()

	type state struct{}

	cp := &failingMemoryCP[state, NoEffect]{
		memoryCP:  newMemoryCP[state, NoEffect](),
		failPrune: true,
	}
	b := NewGraph[state, NoEffect](func(_ state, u state) state { return u })
	b.AddNode("wait", func(_ context.Context, s state) (state, Directive, error) {
		return s, Suspend("hold"), nil
	})
	b.AllowNoOutgoingRoute("wait")
	b.SetEntryPoint("wait")
	g, err := b.Compile(WithRetentionLimit(2))
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	wantReason := "hold_retention_failed"
	res, err := g.NewRunner(cp).Start(context.Background(), "skip-on-save-persist-ret-th", state{})
	if err == nil {
		t.Fatalf("expected retention error, got %+v", res)
	}
	if res.Reason != wantReason {
		t.Fatalf("expected %q, got %q", wantReason, res.Reason)
	}
}

func TestSkipOnSaveErrorHandoffSkipRetentionReason(t *testing.T) {
	t.Parallel()

	type state struct{}

	cp := &failingMemoryCP[state, NoEffect]{
		memoryCP:  newMemoryCP[state, NoEffect](),
		failSave:  true,
		failPrune: true,
	}
	b := NewGraph[state, NoEffect](func(_ state, u state) state { return u })
	b.AddNode("work", func(_ context.Context, s state) (state, Directive, error) {
		return s, Handoff("bg"), nil
	})
	b.AllowNoOutgoingRoute("work")
	b.SetEntryPoint("work")
	g, err := b.Compile(WithRetentionLimit(2))
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	res, err := g.NewRunner(cp).Start(context.Background(), "handoff-skip-ret-th", state{},
		WithCheckpointErrorPolicy[state, NoEffect](CheckpointPolicySkipOnSaveError),
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.Reason != ReasonHandoffCheckpointSkipped {
		t.Fatalf("expected %q without retention suffix, got %q", ReasonHandoffCheckpointSkipped, res.Reason)
	}
}

func TestDefaultCheckpointPolicyIsHardFail(t *testing.T) {
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

	_, err = g.NewRunner(cp).Start(context.Background(), "default-policy-th", state{})
	if err == nil {
		t.Fatal("expected hard fail without explicit policy")
	}
	if strings.Contains(err.Error(), "checkpoint_skipped") {
		t.Fatalf("default policy must be hard fail, got %v", err)
	}
}

func TestInvalidCheckpointPolicy(t *testing.T) {
	t.Parallel()

	type state struct{}

	b := NewGraph[state, NoEffect](func(_ state, u state) state { return u })
	b.AddNode("wait", func(_ context.Context, s state) (state, Directive, error) {
		return s, Suspend("hold"), nil
	})
	b.AddNode("n", func(_ context.Context, s state) (state, Directive, error) {
		return s, End(), nil
	})
	b.AddEdge("wait", "n")
	b.AllowNoOutgoingRoute("n")
	b.SetEntryPoint("wait")
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	cp := newMemoryCP[state, NoEffect]()
	runner := g.NewRunner(cp)
	badPolicy := WithCheckpointErrorPolicy[state, NoEffect]("bogus")

	suspended, err := runner.Start(context.Background(), "bad-policy-suspended", state{})
	if err != nil {
		t.Fatalf("setup suspend: %v", err)
	}
	if suspended.Status != RunStatusSuspended {
		t.Fatalf("expected suspended setup, got %s", suspended.Status)
	}

	tests := []struct {
		name string
		run  func() error
	}{
		{
			name: "start",
			run: func() error {
				_, err := runner.Start(context.Background(), "bad-policy-start", state{}, badPolicy)
				return err
			},
		},
		{
			name: "resume",
			run: func() error {
				_, err := runner.Resume(context.Background(), suspended.ResumeToken, badPolicy)
				return err
			},
		},
		{
			name: "stream",
			run: func() error {
				handle, err := runner.Stream(context.Background(), "bad-policy-stream", state{}, badPolicy)
				if err != nil {
					return err
				}
				return handle.Wait()
			},
		},
		{
			name: "stream_resume",
			run: func() error {
				handle, err := runner.ResumeStream(context.Background(), suspended.ResumeToken, badPolicy)
				if err != nil {
					return err
				}
				return handle.Wait()
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := tc.run()
			if !errors.Is(err, ErrInvalidCheckpointPolicy) {
				t.Fatalf("expected ErrInvalidCheckpointPolicy, got %v", err)
			}
		})
	}
}
