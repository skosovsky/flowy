package flowy

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestRecoverStaleHandoffFromOrphaned(t *testing.T) {
	t.Parallel()

	type state struct{}
	outbox := &stubHandoffOutbox{}
	cp := newMemoryCP[state, NoEffect]()
	now := time.Now().UTC().Add(-time.Hour)
	seedRecoverSnapshot(t, cp, "orphan-th", state{}, RunMetadata{
		HandoffStatus:    HandoffStatusOrphaned,
		HandoffPendingAt: now,
	})

	runner := newRecoverWorkRunner(t, cp, outbox)
	result, recoverErr := runner.RecoverStaleHandoff(context.Background(), "orphan-th")
	if recoverErr != nil {
		t.Fatalf("recover: %v", recoverErr)
	}
	if !result.Recovered ||
		result.Decision.Status != ResumeDecisionHandoffRecoverable ||
		result.HandoffStatus != HandoffStatusEnqueued ||
		result.ResumeToken.ThreadID != "orphan-th" ||
		result.ResumeToken.SnapshotRevision == 0 {
		t.Fatalf("unexpected recovery result: %+v", result)
	}
	if len(outbox.calls) != 1 {
		t.Fatalf("expected one enqueue, got %d", len(outbox.calls))
	}
	assertEnqueuedHandoffSnapshot(t, cp, "orphan-th")
}

func TestRecoverStaleHandoffFromOrphanedLifecycleObserver(t *testing.T) {
	type state struct{}
	obs := &spyLifecycleObserver{}
	SetLifecycleObserver(obs)
	t.Cleanup(func() { SetLifecycleObserver(nil) })

	outbox := &stubHandoffOutbox{}
	cp := newMemoryCP[state, NoEffect]()
	now := time.Now().UTC().Add(-time.Hour)
	if _, err := cp.Save(context.Background(), 0, Snapshot[state, NoEffect]{
		ThreadID:         "recover-obs-orphan-th",
		ExecutionPointer: "work",
		State:            state{},
		RunMeta: RunMetadata{
			HandoffStatus:    HandoffStatusOrphaned,
			HandoffPendingAt: now,
		},
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

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

	runner := g.NewRunnerWithOptions(cp, []RunnerOption[state, NoEffect]{
		WithRunnerHandoffOutbox[state, NoEffect](outbox),
	})
	if _, recoverErr := runner.RecoverStaleHandoff(context.Background(), "recover-obs-orphan-th"); recoverErr != nil {
		t.Fatalf("recover: %v", recoverErr)
	}
	obs.mu.Lock()
	defer obs.mu.Unlock()
	if len(obs.enqueued) != 1 || obs.enqueued[0] != "success" {
		t.Fatalf("expected success metric, got %+v", obs.enqueued)
	}
}

func TestRecoverStaleHandoffFreshPendingRejected(t *testing.T) {
	t.Parallel()

	type state struct{}
	cp := newMemoryCP[state, NoEffect]()
	if _, err := cp.Save(context.Background(), 0, Snapshot[state, NoEffect]{
		ThreadID:         "fresh-pending-th",
		ExecutionPointer: "work",
		State:            state{},
		RunMeta: RunMetadata{
			HandoffStatus:    HandoffStatusPending,
			HandoffPendingAt: time.Now().UTC(),
		},
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

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

	runner := g.NewRunnerWithOptions(cp, []RunnerOption[state, NoEffect]{
		WithRunnerHandoffOutbox[state, NoEffect](&stubHandoffOutbox{}),
		WithHandoffStaleAfter[state, NoEffect](5 * time.Minute),
	})
	_, err = runner.RecoverStaleHandoff(context.Background(), "fresh-pending-th")
	if !errors.Is(err, ErrHandoffPending) {
		t.Fatalf("expected ErrHandoffPending, got %v", err)
	}
}

func TestRecoverStaleHandoffFreshPendingRejectedNoObserverMetric(t *testing.T) {
	type state struct{}
	obs := &spyLifecycleObserver{}
	SetLifecycleObserver(obs)
	t.Cleanup(func() { SetLifecycleObserver(nil) })

	cp := newMemoryCP[state, NoEffect]()
	if _, err := cp.Save(context.Background(), 0, Snapshot[state, NoEffect]{
		ThreadID:         "fresh-pending-obs-th",
		ExecutionPointer: "work",
		State:            state{},
		RunMeta: RunMetadata{
			HandoffStatus:    HandoffStatusPending,
			HandoffPendingAt: time.Now().UTC(),
		},
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

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

	runner := g.NewRunnerWithOptions(cp, []RunnerOption[state, NoEffect]{
		WithRunnerHandoffOutbox[state, NoEffect](&stubHandoffOutbox{}),
		WithHandoffStaleAfter[state, NoEffect](5 * time.Minute),
	})
	_, err = runner.RecoverStaleHandoff(context.Background(), "fresh-pending-obs-th")
	if !errors.Is(err, ErrHandoffPending) {
		t.Fatalf("expected ErrHandoffPending, got %v", err)
	}
	obs.mu.Lock()
	defer obs.mu.Unlock()
	if len(obs.enqueued) != 0 {
		t.Fatalf("expected no handoff metric on fresh pending reject, got %+v", obs.enqueued)
	}
}

func TestRecoverStaleHandoffStalePendingEnqueues(t *testing.T) {
	obs := &spyLifecycleObserver{}
	SetLifecycleObserver(obs)
	t.Cleanup(func() { SetLifecycleObserver(nil) })

	type state struct{}
	outbox := &stubHandoffOutbox{}
	cp := newMemoryCP[state, NoEffect]()
	staleAt := time.Now().UTC().Add(-10 * time.Minute)
	if _, err := cp.Save(context.Background(), 0, Snapshot[state, NoEffect]{
		ThreadID:         "stale-pending-th",
		ExecutionPointer: "work",
		State:            state{},
		RunMeta: RunMetadata{
			HandoffStatus:    HandoffStatusPending,
			HandoffPendingAt: staleAt,
		},
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

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

	runner := g.NewRunnerWithOptions(cp, []RunnerOption[state, NoEffect]{
		WithRunnerHandoffOutbox[state, NoEffect](outbox),
		WithHandoffStaleAfter[state, NoEffect](time.Minute),
	})
	if _, recoverErr := runner.RecoverStaleHandoff(context.Background(), "stale-pending-th"); recoverErr != nil {
		t.Fatalf("recover: %v", recoverErr)
	}
	if len(outbox.calls) != 1 {
		t.Fatalf("expected enqueue after stale pending, got %d calls", len(outbox.calls))
	}
	_, rev, err := cp.Load(context.Background(), "stale-pending-th")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	assertEnqueuedHandoffSnapshot(t, cp, "stale-pending-th")
	if outbox.calls[0].SnapshotRevision != rev ||
		outbox.calls[0].CommittedSnapshotRevision != rev ||
		outbox.calls[0].ResumeToken.SnapshotRevision != rev {
		t.Fatalf("enqueue intent revision should match committed snapshot revision %d, got %+v",
			rev, outbox.calls[0])
	}
	obs.mu.Lock()
	defer obs.mu.Unlock()
	if len(obs.enqueued) == 0 {
		t.Fatal("expected handoff metric on stale pending recovery")
	}
	last := obs.enqueued[len(obs.enqueued)-1]
	if last != "success" {
		t.Fatalf("expected handoff success metric on stale pending recovery, got %+v", obs.enqueued)
	}
}

func TestHandoffCrashBetweenPendingAndPatchRecoverable(t *testing.T) {
	t.Parallel()

	type state struct{}
	outbox := &stubHandoffOutbox{}
	cp := newMemoryCP[state, NoEffect]()
	crashAt := time.Now().UTC().Add(-2 * DefaultHandoffStaleAfter)
	if _, err := cp.Save(context.Background(), 0, Snapshot[state, NoEffect]{
		ThreadID:         "crash-window-th",
		ExecutionPointer: "work",
		State:            state{},
		RunMeta: RunMetadata{
			HandoffStatus:    HandoffStatusPending,
			HandoffPendingAt: crashAt,
		},
	}); err != nil {
		t.Fatalf("seed pending-only snapshot: %v", err)
	}

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

	runner := g.NewRunnerWithOptions(cp, []RunnerOption[state, NoEffect]{
		WithRunnerHandoffOutbox[state, NoEffect](outbox),
	})
	if _, recoverErr := runner.RecoverStaleHandoff(context.Background(), "crash-window-th"); recoverErr != nil {
		t.Fatalf("recover after crash window: %v", recoverErr)
	}
	assertEnqueuedHandoffSnapshot(t, cp, "crash-window-th")
}

func TestRecoverStaleHandoffZeroPendingTimestamp(t *testing.T) {
	t.Parallel()

	type state struct{}
	outbox := &stubHandoffOutbox{}
	cp := newMemoryCP[state, NoEffect]()
	if _, err := cp.Save(context.Background(), 0, Snapshot[state, NoEffect]{
		ThreadID:         "zero-pending-th",
		ExecutionPointer: "work",
		State:            state{},
		RunMeta: RunMetadata{
			HandoffStatus: HandoffStatusPending,
			// HandoffPendingAt zero has no freshness evidence and is stale immediately.
		},
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

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

	runner := g.NewRunnerWithOptions(cp, []RunnerOption[state, NoEffect]{
		WithRunnerHandoffOutbox[state, NoEffect](outbox),
	})
	if _, recoverErr := runner.RecoverStaleHandoff(context.Background(), "zero-pending-th"); recoverErr != nil {
		t.Fatalf("recover zero pending timestamp: %v", recoverErr)
	}
	assertEnqueuedHandoffSnapshot(t, cp, "zero-pending-th")
}

func TestRecoverStaleHandoffAlreadyEnqueued(t *testing.T) {
	t.Parallel()

	type state struct{}
	cp := newMemoryCP[state, NoEffect]()
	if _, err := cp.Save(context.Background(), 0, Snapshot[state, NoEffect]{
		ThreadID:         "already-enqueued-th",
		ExecutionPointer: "work",
		State:            state{},
		RunMeta: RunMetadata{
			HandoffStatus: HandoffStatusEnqueued,
		},
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

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

	runner := g.NewRunnerWithOptions(cp, []RunnerOption[state, NoEffect]{
		WithRunnerHandoffOutbox[state, NoEffect](&stubHandoffOutbox{}),
	})
	_, err = runner.RecoverStaleHandoff(context.Background(), "already-enqueued-th")
	if !errors.Is(err, ErrHandoffAlreadyEnqueued) {
		t.Fatalf("expected ErrHandoffAlreadyEnqueued, got %v", err)
	}
}

func TestRecoverStaleHandoffWithoutOutbox(t *testing.T) {
	t.Parallel()

	type state struct{}
	cp := newMemoryCP[state, NoEffect]()
	seedRecoverSnapshot(t, cp, "no-outbox-recover-th", state{}, RunMetadata{
		HandoffStatus: HandoffStatusOrphaned,
	})

	runner := newRecoverWorkRunner(t, cp, nil)
	result, err := runner.RecoverStaleHandoff(context.Background(), "no-outbox-recover-th")
	if !errors.Is(err, ErrHandoffOutboxRequired) {
		t.Fatalf("expected ErrHandoffOutboxRequired, got %v", err)
	}
	if result.Decision.Status != ResumeDecisionHandoffRecoverable ||
		result.Decision.Reason != "handoff_outbox_required" ||
		result.Decision.ThreadID != "no-outbox-recover-th" ||
		result.Recovered {
		t.Fatalf("unexpected recovery result: %+v", result)
	}
}

func TestRecoverStaleHandoffEmptyThreadID(t *testing.T) {
	t.Parallel()

	type state struct{}
	runner := newRecoverWorkRunner(t, newMemoryCP[state, NoEffect](), &stubHandoffOutbox{})
	_, err := runner.RecoverStaleHandoff(context.Background(), "")
	if err == nil {
		t.Fatal("expected error for empty threadID")
	}
	if !errors.Is(err, ErrInvalidResumeToken) {
		t.Fatalf("expected ErrInvalidResumeToken, got %v", err)
	}
}

func TestRecoverStaleHandoffOnNoneStatus(t *testing.T) {
	t.Parallel()

	type state struct{}
	cp := newMemoryCP[state, NoEffect]()
	seedRecoverSnapshot(t, cp, "none-status-th", state{}, RunMetadata{HandoffStatus: HandoffStatusNone})

	runner := newRecoverWorkRunner(t, cp, &stubHandoffOutbox{})
	_, err := runner.RecoverStaleHandoff(context.Background(), "none-status-th")
	if !errors.Is(err, ErrHandoffNotRecoverable) {
		t.Fatalf("expected ErrHandoffNotRecoverable, got %v", err)
	}
}

func TestRecoverHandoffEnqueueSaveFailsAfterEnqueue(t *testing.T) {
	obs := &spyLifecycleObserver{}
	SetLifecycleObserver(obs)
	t.Cleanup(func() { SetLifecycleObserver(nil) })

	type state struct{}
	outbox := &stubHandoffOutbox{}
	cp := &handoffPatchFailCP[state, NoEffect]{failOnStatus: HandoffStatusEnqueued}
	now := time.Now().UTC().Add(-time.Hour)
	seedRecoverSnapshot(t, cp, "recover-patch-fail-th", state{}, RunMetadata{
		HandoffStatus:    HandoffStatusOrphaned,
		HandoffPendingAt: now,
	})

	runner := newRecoverWorkRunner(t, cp, outbox)
	result, recoverErr := runner.RecoverStaleHandoff(context.Background(), "recover-patch-fail-th")
	if recoverErr == nil {
		t.Fatal("expected recovery patch failure")
	}
	if !errors.Is(recoverErr, ErrHandoffPatchFailed) {
		t.Fatalf("expected ErrHandoffPatchFailed, got %v", recoverErr)
	}
	if result.Recovered ||
		result.HandoffStatus != HandoffStatusOrphaned ||
		result.Decision.HandoffStatus != HandoffStatusOrphaned ||
		result.ResumeToken.ThreadID != "recover-patch-fail-th" ||
		result.ResumeToken.SnapshotRevision == 0 {
		t.Fatalf("unexpected partial recovery result: %+v", result)
	}
	if len(outbox.calls) != 0 {
		t.Fatalf(
			"outbox must not receive recovery intent before enqueued patch succeeds, got %d calls",
			len(outbox.calls),
		)
	}
	snap, _, loadErr := cp.Load(context.Background(), "recover-patch-fail-th")
	if loadErr != nil {
		t.Fatalf("load: %v", loadErr)
	}
	if snap.RunMeta.HandoffStatus != HandoffStatusOrphaned {
		t.Fatalf("expected unchanged orphaned status, got %q", snap.RunMeta.HandoffStatus)
	}
	if snap.RunMeta.HandoffPendingAt.IsZero() {
		t.Fatal("expected original HandoffPendingAt retained when recovery patch fails")
	}
	obs.mu.Lock()
	defer obs.mu.Unlock()
	last := obs.enqueued[len(obs.enqueued)-1]
	if last != "patch_enqueued_failed" {
		t.Fatalf("expected patch_enqueued_failed metric, got %+v", obs.enqueued)
	}
}

func TestRecoverHandoffEnqueueOrphanClearsHandoffPendingAt(t *testing.T) {
	t.Parallel()

	type state struct{}
	outbox := &stubHandoffOutbox{err: errors.New("broker down")}
	cp := newMemoryCP[state, NoEffect]()
	staleAt := time.Now().UTC().Add(-time.Hour)
	if _, err := cp.Save(context.Background(), 0, Snapshot[state, NoEffect]{
		ThreadID:         "recover-orphan-ts-th",
		ExecutionPointer: "work",
		State:            state{},
		RunMeta: RunMetadata{
			HandoffStatus:    HandoffStatusOrphaned,
			HandoffPendingAt: staleAt,
		},
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

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

	runner := g.NewRunnerWithOptions(cp, []RunnerOption[state, NoEffect]{
		WithRunnerHandoffOutbox[state, NoEffect](outbox),
	})
	result, recoverErr := runner.RecoverStaleHandoff(context.Background(), "recover-orphan-ts-th")
	if !errors.Is(recoverErr, ErrHandoffEnqueueFailed) {
		t.Fatalf("expected ErrHandoffEnqueueFailed, got %v", recoverErr)
	}
	if result.Recovered ||
		result.HandoffStatus != HandoffStatusOrphaned ||
		result.Decision.HandoffStatus != HandoffStatusOrphaned ||
		!result.Decision.RunMeta.HandoffPendingAt.IsZero() ||
		result.ResumeToken.ThreadID != "recover-orphan-ts-th" ||
		result.ResumeToken.SnapshotRevision == 0 {
		t.Fatalf("unexpected partial recovery result: %+v", result)
	}
	assertOrphanedHandoffSnapshot(t, cp, "recover-orphan-ts-th", nil, "")
}

func TestHandoffFalseEnqueuedWithoutMessage(t *testing.T) {
	t.Parallel()

	type state struct{}
	outbox := &stubHandoffOutbox{}
	cp := newMemoryCP[state, NoEffect]()
	if _, err := cp.Save(context.Background(), 0, Snapshot[state, NoEffect]{
		ThreadID:         "false-enqueued-th",
		ExecutionPointer: "work",
		State:            state{},
		RunMeta: RunMetadata{
			HandoffStatus: HandoffStatusEnqueued,
		},
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

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

	runner := g.NewRunnerWithOptions(cp, []RunnerOption[state, NoEffect]{
		WithRunnerHandoffOutbox[state, NoEffect](outbox),
	})
	_, err = runner.RecoverStaleHandoff(context.Background(), "false-enqueued-th")
	if !errors.Is(err, ErrHandoffAlreadyEnqueued) {
		t.Fatalf("expected ErrHandoffAlreadyEnqueued, got %v", err)
	}
	if len(outbox.calls) != 0 {
		t.Fatalf("outbox must not enqueue when status already enqueued, calls=%d", len(outbox.calls))
	}
}

func TestHandoffForceReenqueueFalseEnqueued(t *testing.T) {
	t.Parallel()

	type state struct{}
	outbox := &stubHandoffOutbox{}
	cp := newMemoryCP[state, NoEffect]()
	if _, err := cp.Save(context.Background(), 0, Snapshot[state, NoEffect]{
		ThreadID:         "force-reenqueue-th",
		ExecutionPointer: "work",
		State:            state{},
		RunMeta: RunMetadata{
			HandoffStatus: HandoffStatusEnqueued,
		},
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

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

	runner := g.NewRunnerWithOptions(cp, []RunnerOption[state, NoEffect]{
		WithRunnerHandoffOutbox[state, NoEffect](outbox),
	})
	_, err = runner.RecoverStaleHandoff(context.Background(), "force-reenqueue-th",
		WithRecoverForceReenqueue(true),
	)
	if err != nil {
		t.Fatalf("recover force re-enqueue: %v", err)
	}
	if len(outbox.calls) != 1 {
		t.Fatalf("expected one outbox enqueue on force re-enqueue, got %d", len(outbox.calls))
	}
	assertEnqueuedHandoffSnapshot(t, cp, "force-reenqueue-th")
}

func TestRecoverStaleHandoffConcurrent(t *testing.T) {
	t.Parallel()

	type state struct{}
	outbox := &stubHandoffOutbox{}
	cp := newMemoryCP[state, NoEffect]()
	if _, err := cp.Save(context.Background(), 0, Snapshot[state, NoEffect]{
		ThreadID:         "recover-concurrent-th",
		ExecutionPointer: "work",
		State:            state{},
		RunMeta: RunMetadata{
			HandoffStatus: HandoffStatusOrphaned,
		},
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

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

	runner := g.NewRunnerWithOptions(cp, []RunnerOption[state, NoEffect]{
		WithRunnerHandoffOutbox[state, NoEffect](outbox),
	})

	const workers = 8
	errCh := make(chan error, workers)
	for range workers {
		go func() {
			_, err := runner.RecoverStaleHandoff(context.Background(), "recover-concurrent-th")
			errCh <- err
		}()
	}
	var success int
	for range workers {
		if err := <-errCh; err == nil {
			success++
		}
	}
	if success < 1 {
		t.Fatal("expected at least one successful recovery")
	}
	if len(outbox.calls) < 1 {
		t.Fatalf("expected at least one enqueue, got %d", len(outbox.calls))
	}
	assertEnqueuedHandoffSnapshot(t, cp, "recover-concurrent-th")
}

func TestRecoverStaleHandoffBothPatchesFail(t *testing.T) {
	obs := &spyLifecycleObserver{}
	SetLifecycleObserver(obs)
	t.Cleanup(func() { SetLifecycleObserver(nil) })

	type state struct{}
	outbox := &stubHandoffOutbox{}
	cp := &handoffPatchFailCP[state, NoEffect]{
		failOnStatuses: map[HandoffStatus]struct{}{ //nolint:exhaustive // patch statuses only
			HandoffStatusEnqueued: {},
			HandoffStatusOrphaned: {},
		},
	}
	cp.ensureMemoryCP()
	if _, err := cp.memoryCP.Save(context.Background(), 0, Snapshot[state, NoEffect]{
		ThreadID:         "recover-both-fail-th",
		ExecutionPointer: "work",
		State:            state{},
		RunMeta: RunMetadata{
			HandoffStatus: HandoffStatusOrphaned,
		},
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

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

	runner := g.NewRunnerWithOptions(cp, []RunnerOption[state, NoEffect]{
		WithRunnerHandoffOutbox[state, NoEffect](outbox),
	})
	result, err := runner.RecoverStaleHandoff(context.Background(), "recover-both-fail-th")
	if err == nil {
		t.Fatal("expected recovery patch failure")
	}
	if !errors.Is(err, ErrHandoffPatchFailed) {
		t.Fatalf("expected ErrHandoffPatchFailed, got %v", err)
	}
	if result.Recovered ||
		result.HandoffStatus != HandoffStatusOrphaned ||
		result.Decision.HandoffStatus != HandoffStatusOrphaned ||
		result.ResumeToken.ThreadID != "recover-both-fail-th" ||
		result.ResumeToken.SnapshotRevision == 0 {
		t.Fatalf("unexpected partial recovery result: %+v", result)
	}
	if len(outbox.calls) != 0 {
		t.Fatalf(
			"outbox must not receive recovery intent before enqueued patch succeeds, got %d calls",
			len(outbox.calls),
		)
	}
	snap, _, loadErr := cp.Load(context.Background(), "recover-both-fail-th")
	if loadErr != nil {
		t.Fatalf("load: %v", loadErr)
	}
	if snap.RunMeta.HandoffStatus != HandoffStatusOrphaned {
		t.Fatalf("expected unchanged orphaned status, got %q", snap.RunMeta.HandoffStatus)
	}
	obs.mu.Lock()
	defer obs.mu.Unlock()
	last := obs.enqueued[len(obs.enqueued)-1]
	if last != "patch_enqueued_failed" {
		t.Fatalf("expected patch_enqueued_failed metric, got %+v", obs.enqueued)
	}
}
