package flowy

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestHandoffOutboxFailurePreservesSnapshot(t *testing.T) {
	t.Parallel()

	type state struct{ N int }

	outbox := &stubHandoffOutbox{err: errors.New("broker down")}
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
	res, err := runner.Start(context.Background(), "outbox-fail-th", state{},
		WithHandoffOutbox[state, NoEffect](outbox),
	)
	if !errors.Is(err, ErrHandoffEnqueueFailed) {
		t.Fatalf("expected ErrHandoffEnqueueFailed, got %v res=%+v", err, res)
	}
	if res == nil || res.ResumeToken.ThreadID != "outbox-fail-th" {
		t.Fatalf("expected populated ResumeToken for retry, got %+v", res)
	}
	if res.Status != RunStatusHandoff {
		t.Fatalf("expected handoff status, got %s", res.Status)
	}
	if res.Reason != ReasonHandoffOrphaned {
		t.Fatalf("expected reason %q, got %q", ReasonHandoffOrphaned, res.Reason)
	}
	assertOrphanedHandoffSnapshot(t, cp, "outbox-fail-th", res, "bg")
	assertRunMetaHandoffStatusMatchesSnapshot(t, res, cp, "outbox-fail-th")
	snap, _, loadErr := cp.Load(context.Background(), "outbox-fail-th")
	if loadErr != nil {
		t.Fatalf("load: %v", loadErr)
	}
	if res.ResumeToken.SnapshotRevision != snap.Revision {
		t.Fatalf("snapshot revision %d != snapshot revision %d", res.ResumeToken.SnapshotRevision, snap.Revision)
	}
}

func TestHandoffOutboxSuccess(t *testing.T) {
	t.Parallel()

	type state struct{}

	outbox := &stubHandoffOutbox{}
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
	res, err := runner.Start(context.Background(), "outbox-ok-th", state{},
		WithHandoffOutbox[state, NoEffect](outbox),
	)
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	token := outbox.lastToken()
	if token.ThreadID != "outbox-ok-th" {
		t.Fatalf("outbox token thread mismatch: got %+v", token)
	}
	snap, _, err := cp.Load(context.Background(), "outbox-ok-th")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if res.ResumeToken.SnapshotRevision != snap.Revision {
		t.Fatalf("result token revision %d != snapshot revision %d", res.ResumeToken.SnapshotRevision, snap.Revision)
	}
	if token.SnapshotRevision != snap.Revision-1 {
		t.Fatalf("outbox token revision %d != pending revision %d", token.SnapshotRevision, snap.Revision-1)
	}
	if token.SnapshotRevision == res.ResumeToken.SnapshotRevision {
		t.Fatalf("outbox token must differ from result token when enqueued: %+v", token)
	}
	if snap.RunMeta.HandoffStatus != HandoffStatusEnqueued {
		t.Fatalf("expected enqueued handoff status, got %q", snap.RunMeta.HandoffStatus)
	}
	assertRunMetaHandoffStatusMatchesSnapshot(t, res, cp, "outbox-ok-th")
	if !res.RunMeta.HandoffPendingAt.IsZero() {
		t.Fatalf("expected HandoffPendingAt cleared on enqueued, got %v", res.RunMeta.HandoffPendingAt)
	}
}

func TestHandoffEnqueueWhileStatusPending(t *testing.T) {
	t.Parallel()

	type state struct{}

	cp := newMemoryCP[state, NoEffect]()
	outbox := &stubHandoffOutbox{
		onEnqueue: func(token ResumeToken) error {
			snap, _, loadErr := cp.Load(context.Background(), token.ThreadID)
			if loadErr != nil {
				return loadErr
			}
			if snap.RunMeta.HandoffStatus != HandoffStatusPending {
				return fmt.Errorf("expected pending during enqueue, got %q", snap.RunMeta.HandoffStatus)
			}
			return nil
		},
	}
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

	_, err = g.NewRunner(cp).Start(context.Background(), "enqueue-pending-th", state{},
		WithHandoffOutbox[state, NoEffect](outbox),
	)
	if err != nil {
		t.Fatalf("start: %v", err)
	}
}

type spyLifecycleObserver struct {
	mu              sync.Mutex
	enqueued        []string
	rejected        []string
	checkpointSofts int
}

func (s *spyLifecycleObserver) HandoffEnqueued(_ context.Context, _ string, _ ExecutionPointer, status string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.enqueued = append(s.enqueued, status)
}

func (s *spyLifecycleObserver) ResumeRejected(_ context.Context, _ string, _ ExecutionPointer, reason string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.rejected = append(s.rejected, reason)
}

func (s *spyLifecycleObserver) CheckpointSoftError(context.Context, string, ExecutionPointer) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.checkpointSofts++
}

func TestHandoffLifecycleObserverEmitted(t *testing.T) {
	type state struct{}
	obs := &spyLifecycleObserver{}
	SetLifecycleObserver(obs)
	t.Cleanup(func() { SetLifecycleObserver(nil) })

	outbox := &stubHandoffOutbox{err: errors.New("down")}
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
	res, err := g.NewRunner(cp).Start(context.Background(), "obs-th", state{},
		WithHandoffOutbox[state, NoEffect](outbox),
	)
	if !errors.Is(err, ErrHandoffEnqueueFailed) {
		t.Fatalf("expected ErrHandoffEnqueueFailed, got %v res=%+v", err, res)
	}
	if res == nil || res.Reason != ReasonHandoffOrphaned {
		t.Fatalf("expected reason %q, got res=%+v", ReasonHandoffOrphaned, res)
	}
	assertOrphanedHandoffSnapshot(t, cp, "obs-th", res, "bg")
	assertRunMetaHandoffStatusMatchesSnapshot(t, res, cp, "obs-th")

	obs.mu.Lock()
	defer obs.mu.Unlock()
	if len(obs.enqueued) != 1 || obs.enqueued[0] != "enqueue_failed" {
		t.Fatalf("expected handoff enqueue_failed metric, got %+v", obs.enqueued)
	}
}

func TestHandoffOutboxDoesNotDeleteOnEnqueueFailure(t *testing.T) {
	t.Parallel()

	type state struct{}

	baseCP := newMemoryCP[state, NoEffect]()
	cp := &deleteSpyCP[state, NoEffect]{memoryCP: baseCP}
	outbox := &stubHandoffOutbox{err: errors.New("broker down")}
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
		WithHandoffOutbox[state, NoEffect](outbox),
	)
	if !errors.Is(err, ErrHandoffEnqueueFailed) {
		t.Fatalf("expected enqueue failure, got %v", err)
	}
	if cp.deleteCalls != 0 {
		t.Fatalf("Delete must not run on enqueue failure, calls=%d", cp.deleteCalls)
	}
}

func TestRequestLocalHandoffContextCancelDuringSave(t *testing.T) {
	type state struct{ N int }

	ready := make(chan struct{})
	b := NewGraph[state, NoEffect](func(_ state, u state) state { return u })
	b.AddNode("work", func(ctx context.Context, s state) (state, Directive, error) {
		s.N++
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

	cp := newBlockingSaveCP[state, NoEffect]()
	runner := g.NewRunner(cp)
	runCtx, runCancel := context.WithCancel(context.Background())
	defer runCancel()

	startDone := make(chan error, 1)
	go func() {
		_, startErr := runner.Start(runCtx, "handoff-cancel-th", state{})
		startDone <- startErr
	}()
	<-ready

	handoffCtx, handoffCancel := context.WithCancel(context.Background())
	handoffDone := make(chan error, 1)
	go func() {
		handoffDone <- runner.RequestLocalHandoff(handoffCtx, "handoff-cancel-th")
	}()

	select {
	case <-cp.saveEntered:
	case <-time.After(2 * time.Second):
		t.Fatal("Save was not entered during handoff")
	}
	handoffCancel()
	cp.unblockSave()

	select {
	case handoffErr := <-handoffDone:
		if !errors.Is(handoffErr, ErrHandoffNotCompleted) {
			t.Fatalf("expected ErrHandoffNotCompleted when caller ctx canceled during save, got %v", handoffErr)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("RequestLocalHandoff did not complete within 2s")
	}
	runCancel()
	select {
	case startErr := <-startDone:
		if startErr != nil && !errors.Is(startErr, ErrHandoffRequested) &&
			!errors.Is(startErr, context.Canceled) {
			t.Fatalf("unexpected start error after handoff: %v", startErr)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("start did not complete")
	}
}

func TestRequestLocalHandoffDuringSessionOverwriteRegression(t *testing.T) {
	t.Parallel()

	type state struct{ N int }

	ready := make(chan struct{})
	secondReady := make(chan struct{})
	var readyOnce sync.Once
	var enterMu sync.Mutex
	var enters int
	b := NewGraph[state, NoEffect](func(_ state, u state) state { return u })
	b.AddNode("work", func(ctx context.Context, s state) (state, Directive, error) {
		s.N++
		enterMu.Lock()
		enters++
		n := enters
		enterMu.Unlock()
		if n == 1 {
			readyOnce.Do(func() { close(ready) })
		} else {
			select {
			case <-secondReady:
			default:
				close(secondReady)
			}
		}
		<-ctx.Done()
		return s, Completed(), nil
	})
	b.AllowNoOutgoingRoute("work")
	b.SetEntryPoint("work")
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	cp := newMemoryCP[state, NoEffect]()
	runner := g.NewRunner(cp)

	startDone := make(chan error, 1)
	go func() {
		_, runErr := runner.Start(context.Background(), "htb-overwrite-th", state{})
		startDone <- runErr
	}()
	<-ready

	_, dupErr := runner.Start(context.Background(), "htb-overwrite-th", state{})
	if !errors.Is(dupErr, ErrThreadAlreadyRunning) {
		t.Fatalf("duplicate Start while session active: expected ErrThreadAlreadyRunning, got %v", dupErr)
	}
	if htbErr := runner.RequestLocalHandoff(context.Background(), "htb-overwrite-th"); htbErr != nil {
		t.Fatalf("handoff: %v", htbErr)
	}
	if startErr := <-startDone; startErr != nil {
		t.Fatalf("first run after handoff: %v", startErr)
	}

	ctxB, cancelB := context.WithCancel(context.Background())
	defer cancelB()
	startBDone := make(chan struct{})
	var resB *RunResult[state, NoEffect]
	var errB error
	go func() {
		resB, errB = runner.Start(ctxB, "htb-overwrite-th", state{N: 99})
		close(startBDone)
	}()
	<-secondReady
	cancelB()
	<-startBDone
	if errors.Is(errB, ErrThreadAlreadyRunning) {
		t.Fatalf("second start must not see stale session: %v", errB)
	}
	if errors.Is(errB, ErrHandoffRequested) {
		t.Fatal("second start must not inherit ErrHandoffRequested")
	}
	if resB == nil || resB.State.N != 100 {
		t.Fatalf("second run must start fresh state and execute node once, got res=%+v err=%v", resB, errB)
	}
}

func TestRequestLocalHandoffReturnsHandoffNotCompletedOnShortCallerCtx(t *testing.T) {
	t.Parallel()

	type state struct{}

	ready := make(chan struct{})
	blockForever := make(chan struct{})
	b := NewGraph[state, NoEffect](func(_ state, u state) state { return u })
	b.AddNode("block", func(_ context.Context, s state) (state, Directive, error) {
		close(ready)
		<-blockForever
		return s, Completed(), nil
	})
	b.AllowNoOutgoingRoute("block")
	b.SetEntryPoint("block")
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	runner := g.NewRunner(newMemoryCP[state, NoEffect]())
	startDone := make(chan struct{})
	go func() {
		_, _ = runner.Start(context.Background(), "htb-timeout-th", state{})
		close(startDone)
	}()
	<-ready

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	err = runner.RequestLocalHandoff(ctx, "htb-timeout-th")
	if !errors.Is(err, ErrHandoffNotCompleted) {
		t.Fatalf("expected ErrHandoffNotCompleted, got %v", err)
	}
	close(blockForever)
	<-startDone
}

func TestRequestLocalHandoffSkipOnSaveErrorSkipReturnsCheckpointSkipped(t *testing.T) {
	t.Parallel()

	type state struct{ N int }

	g, ready := blockingHandoffWorkGraph[state, NoEffect](t)
	cp := &failingMemoryCP[state, NoEffect]{failSave: true}
	runner := g.NewRunner(cp)

	startDone := make(chan error, 1)
	go func() {
		_, runErr := runner.Start(context.Background(), "htb-skip-on-save-th", state{},
			WithCheckpointErrorPolicy[state, NoEffect](CheckpointPolicySkipOnSaveError),
		)
		startDone <- runErr
	}()

	<-ready
	handoffErr := runner.RequestLocalHandoff(context.Background(), "htb-skip-on-save-th")
	if !errors.Is(handoffErr, ErrCheckpointSkipped) {
		t.Fatalf("expected ErrCheckpointSkipped, got %v", handoffErr)
	}
	if runErr := <-startDone; !errors.Is(runErr, ErrCheckpointSkipped) {
		t.Fatalf("expected execute ErrCheckpointSkipped, got %v", runErr)
	}
	if _, _, loadErr := cp.Load(context.Background(), "htb-skip-on-save-th"); loadErr == nil {
		t.Fatal("expected no snapshot on skip-on-save handoff skip")
	}
}

func TestRequestLocalHandoffSkipOnSaveErrorWithOutbox(t *testing.T) {
	t.Parallel()

	type state struct{ N int }

	outbox := &stubHandoffOutbox{}
	g, ready := blockingHandoffWorkGraph[state, NoEffect](t)
	cp := &failingMemoryCP[state, NoEffect]{failSave: true}
	runner := g.NewRunner(cp)

	startDone := make(chan *RunResult[state, NoEffect], 1)
	go func() {
		res, runErr := runner.Start(context.Background(), "htb-skip-outbox-th", state{},
			WithCheckpointErrorPolicy[state, NoEffect](CheckpointPolicySkipOnSaveError),
			WithHandoffOutbox[state, NoEffect](outbox),
		)
		if !errors.Is(runErr, ErrCheckpointSkipped) {
			t.Errorf("expected execute ErrCheckpointSkipped, got %v", runErr)
		}
		startDone <- res
	}()

	<-ready
	handoffErr := runner.RequestLocalHandoff(context.Background(), "htb-skip-outbox-th")
	if !errors.Is(handoffErr, ErrCheckpointSkipped) {
		t.Fatalf("expected ErrCheckpointSkipped, got %v", handoffErr)
	}
	res := <-startDone
	if res == nil {
		t.Fatal("expected non-nil RunResult on skip-on-save HTB")
	}
	if res.RunMeta.HandoffStatus != HandoffStatusNone {
		t.Fatalf("expected none handoff status on skip-on-save HTB, got %q", res.RunMeta.HandoffStatus)
	}
	if !res.RunMeta.HandoffPendingAt.IsZero() {
		t.Fatalf("expected zero HandoffPendingAt on skip-on-save HTB, got %v", res.RunMeta.HandoffPendingAt)
	}
	if _, _, loadErr := cp.Load(context.Background(), "htb-skip-outbox-th"); loadErr == nil {
		t.Fatal("expected no snapshot on skip-on-save HTB with outbox")
	}
	if len(outbox.calls) != 0 {
		t.Fatalf("outbox must not run without persisted handoff save, calls=%d", len(outbox.calls))
	}
}

func TestRequestLocalHandoffEnqueueFailAfterPersist(t *testing.T) {
	t.Parallel()

	type state struct{}

	outbox := &stubHandoffOutbox{err: errors.New("broker down")}
	g, ready := blockingHandoffWorkGraph[state, NoEffect](t)
	cp := newMemoryCP[state, NoEffect]()
	runner := g.NewRunner(cp)

	startDone := make(chan *RunResult[state, NoEffect], 1)
	go func() {
		res, _ := runner.Start(context.Background(), "htb-enqueue-th", state{},
			WithHandoffOutbox[state, NoEffect](outbox),
		)
		startDone <- res
	}()

	<-ready
	handoffErr := runner.RequestLocalHandoff(context.Background(), "htb-enqueue-th")
	if !errors.Is(handoffErr, ErrHandoffEnqueueFailed) {
		t.Fatalf("expected ErrHandoffEnqueueFailed, got %v", handoffErr)
	}
	res := <-startDone
	if res == nil || res.ResumeToken.ThreadID == "" {
		t.Fatalf("expected ResumeToken after enqueue fail, got %+v", res)
	}
	assertHandoffFailureTokenMatchesSnapshot(t, res, cp, "htb-enqueue-th")
	assertHandoffReasonMatchesStatus(t, res, cp, "htb-enqueue-th", "background_handoff")
	assertRunMetaHandoffStatusMatchesSnapshot(t, res, cp, "htb-enqueue-th")
}

func TestRequestLocalHandoffPropagatesSaveError(t *testing.T) {
	t.Parallel()

	type state struct{}

	g, ready := blockingHandoffWorkGraph[state, NoEffect](t)
	cp := &failingMemoryCP[state, NoEffect]{failSave: true}
	runner := g.NewRunner(cp)

	startDone := make(chan error, 1)
	go func() {
		_, runErr := runner.Start(context.Background(), "htb-savefail-th", state{})
		startDone <- runErr
	}()

	<-ready
	handoffErr := runner.RequestLocalHandoff(context.Background(), "htb-savefail-th")
	if handoffErr == nil {
		t.Fatal("expected handoff save error")
	}
	if errors.Is(handoffErr, ErrCheckpointSkipped) {
		t.Fatalf("hard fail must not return ErrCheckpointSkipped, got %v", handoffErr)
	}
	if !strings.Contains(handoffErr.Error(), "save failed") {
		t.Fatalf("expected save failed in error, got %v", handoffErr)
	}
}

func TestRequestLocalHandoffRetentionFailAfterPersist(t *testing.T) {
	t.Parallel()

	type state struct{}

	cp := &failingMemoryCP[state, NoEffect]{
		memoryCP:  newMemoryCP[state, NoEffect](),
		failPrune: true,
	}
	b := NewGraph[state, NoEffect](func(_ state, u state) state { return u })
	ready := make(chan struct{})
	b.AddNode("work", func(ctx context.Context, s state) (state, Directive, error) {
		close(ready)
		<-ctx.Done()
		return s, Completed(), nil
	})
	b.AllowNoOutgoingRoute("work")
	b.SetEntryPoint("work")
	g, err := b.Compile(WithRetentionLimit(2))
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	runner := g.NewRunner(cp)

	startDone := make(chan *RunResult[state, NoEffect], 1)
	go func() {
		res, runErr := runner.Start(context.Background(), "htb-retention-th", state{})
		if runErr == nil {
			t.Errorf("expected retention error from Start")
		}
		startDone <- res
	}()

	<-ready
	handoffErr := runner.RequestLocalHandoff(context.Background(), "htb-retention-th")
	if handoffErr == nil {
		t.Fatal("expected retention error on handoff")
	}
	if !strings.Contains(handoffErr.Error(), "retention") {
		t.Fatalf("expected retention in error, got %v", handoffErr)
	}
	if _, _, loadErr := cp.Load(context.Background(), "htb-retention-th"); loadErr != nil {
		t.Fatalf("snapshot must exist despite retention failure: %v", loadErr)
	}
	syncRes := <-startDone
	wantReason := retentionFailedReason("background_handoff")
	if syncRes != nil && syncRes.Reason != wantReason {
		t.Fatalf("expected sync reason %q, got %q", wantReason, syncRes.Reason)
	}
	assertRunMetaHandoffStatusMatchesSnapshot(t, syncRes, cp, "htb-retention-th")
}

func TestRequestLocalHandoffLeaseLostClosesSession(t *testing.T) {
	t.Parallel()

	type state struct{ N int }

	leaseOpts := []RunOption[state, NoEffect]{
		WithRunLease[state, NoEffect]("worker-a", 50*time.Millisecond),
	}

	t.Run("lease_lost_before_handoff", func(t *testing.T) {
		t.Parallel()
		// RequestLocalHandoff before forceLeaseTakeover is intentional: deterministic ordering vs concurrent race.

		lease := NewMemoryLeaseManager()
		g, ready := blockingHandoffWorkGraph[state, NoEffect](t)
		runner := g.NewRunnerWithOptions(newMemoryCP[state, NoEffect](), []RunnerOption[state, NoEffect]{
			WithLeaseManager[state, NoEffect](lease),
		})

		startDone := make(chan error, 1)
		go func() {
			_, runErr := runner.Start(context.Background(), "htb-lease-before-th", state{}, leaseOpts...)
			startDone <- runErr
		}()

		<-ready
		handoffErr := runner.RequestLocalHandoff(context.Background(), "htb-lease-before-th")
		if errors.Is(handoffErr, ErrNoActiveExecution) {
			t.Fatalf(
				"handoff must not return ErrNoActiveExecution while session is active, got %v",
				handoffErr,
			)
		}
		forceLeaseTakeover(t, lease, "htb-lease-before-th")
		waitForLeaseTTLExpiry()

		startErr := <-startDone
		if handoffErr != nil && !errors.Is(handoffErr, ErrLeaseLost) {
			t.Fatalf("expected nil or ErrLeaseLost on handoff, got %v", handoffErr)
		}
		if startErr != nil && !errors.Is(startErr, ErrLeaseLost) {
			t.Fatalf("expected nil or ErrLeaseLost on execute after handoff race, got %v", startErr)
		}
	})

	t.Run("session_closed_after_lease_lost", func(t *testing.T) {
		t.Parallel()

		lease := NewMemoryLeaseManager()
		g, ready := blockingHandoffWorkGraph[state, NoEffect](t)
		runner := g.NewRunnerWithOptions(newMemoryCP[state, NoEffect](), []RunnerOption[state, NoEffect]{
			WithLeaseManager[state, NoEffect](lease),
		})

		startDone := make(chan error, 1)
		go func() {
			_, runErr := runner.Start(context.Background(), "htb-lease-after-th", state{}, leaseOpts...)
			startDone <- runErr
		}()

		<-ready
		forceLeaseTakeover(t, lease, "htb-lease-after-th")
		waitForLeaseTTLExpiry()

		startErr := <-startDone
		if !errors.Is(startErr, ErrLeaseLost) {
			t.Fatalf("expected ErrLeaseLost on execute, got %v", startErr)
		}

		handoffErr := runner.RequestLocalHandoff(context.Background(), "htb-lease-after-th")
		if !errors.Is(handoffErr, ErrNoActiveExecution) {
			t.Fatalf("expected ErrNoActiveExecution after session closed, got %v", handoffErr)
		}
	})
}

func TestHandoffPointerResolveFailReasonOnBackgroundHandoff(t *testing.T) {
	t.Parallel()

	type state struct{}

	g, ready := blockingHandoffWorkGraph[state, NoEffect](t)
	cp := newMemoryCP[state, NoEffect]()
	runner := g.NewRunner(cp)

	startDone := make(chan *RunResult[state, NoEffect], 1)
	go func() {
		res, _ := runner.Start(context.Background(), "htb-resolve-th", state{},
			WithSuspendPointerResolver[state, NoEffect](func(_ state, _ ExecutionPointer) (ExecutionPointer, error) {
				return "", errors.New("bad pointer")
			}),
		)
		startDone <- res
	}()

	<-ready
	handoffErr := runner.RequestLocalHandoff(context.Background(), "htb-resolve-th")
	if handoffErr == nil {
		t.Fatal("expected resolve error")
	}
	if _, _, loadErr := cp.Load(context.Background(), "htb-resolve-th"); !errors.Is(loadErr, ErrThreadNotFound) {
		t.Fatalf("snapshot must not be saved on resolve fail, load err=%v", loadErr)
	}
	res := <-startDone
	if res == nil || res.Reason != ReasonHandoffPointerResolveFailed {
		t.Fatalf("expected reason %q, got %+v", ReasonHandoffPointerResolveFailed, res)
	}
}

func TestRequestLocalHandoffNoActiveSession(t *testing.T) {
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

	cp := newMemoryCP[state, NoEffect]()
	runner := g.NewRunner(cp)

	if handoffErr := runner.RequestLocalHandoff(
		context.Background(),
		"never-started-th",
	); !errors.Is(
		handoffErr,
		ErrNoActiveExecution,
	) {
		t.Fatalf("never started: expected ErrNoActiveExecution, got %v", handoffErr)
	}

	res, err := runner.Start(context.Background(), "post-done-th", state{})
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	if res.Status != RunStatusCompleted {
		t.Fatalf("expected completed, got %s", res.Status)
	}
	if err := runner.RequestLocalHandoff(context.Background(), "post-done-th"); !errors.Is(
		err,
		ErrNoActiveExecution,
	) {
		t.Fatalf("after Wait on same runner: expected ErrNoActiveExecution, got %v", err)
	}
}

func TestRequestLocalHandoffEmptyThreadID(t *testing.T) {
	t.Parallel()

	type state struct{}

	g, _ := blockingHandoffWorkGraph[state, NoEffect](t)
	runner := g.NewRunner(newMemoryCP[state, NoEffect]())
	if err := runner.RequestLocalHandoff(context.Background(), ""); err == nil {
		t.Fatal("expected error for empty threadID")
	} else if errors.Is(err, ErrNoActiveExecution) {
		t.Fatalf("empty threadID must not return ErrNoActiveExecution, got %v", err)
	} else if !errors.Is(err, ErrInvalidResumeToken) {
		t.Fatalf("expected ErrInvalidResumeToken, got %v", err)
	}
}

func TestRequestLocalHandoffEnqueueAndRetentionJoin(t *testing.T) {
	t.Parallel()

	type state struct{}

	outbox := &stubHandoffOutbox{err: errors.New("broker down")}
	cp := &failingMemoryCP[state, NoEffect]{
		memoryCP:  newMemoryCP[state, NoEffect](),
		failPrune: true,
	}
	b := NewGraph[state, NoEffect](func(_ state, u state) state { return u })
	ready := make(chan struct{})
	b.AddNode("work", func(ctx context.Context, s state) (state, Directive, error) {
		close(ready)
		<-ctx.Done()
		return s, Completed(), nil
	})
	b.AllowNoOutgoingRoute("work")
	b.SetEntryPoint("work")
	g, err := b.Compile(WithRetentionLimit(2))
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	runner := g.NewRunner(cp)

	startDone := make(chan *RunResult[state, NoEffect], 1)
	go func() {
		res, runErr := runner.Start(context.Background(), "htb-enqueue-retention-th", state{},
			WithHandoffOutbox[state, NoEffect](outbox),
		)
		if runErr == nil {
			t.Errorf("expected combined enqueue+retention error from Start")
		}
		startDone <- res
	}()

	<-ready
	handoffErr := runner.RequestLocalHandoff(context.Background(), "htb-enqueue-retention-th")
	if !errors.Is(handoffErr, ErrHandoffEnqueueFailed) {
		t.Fatalf("expected enqueue failure, got %v", handoffErr)
	}
	if !strings.Contains(handoffErr.Error(), "retention") {
		t.Fatalf("expected retention error in join chain, got %v", handoffErr)
	}
	res := <-startDone
	wantReason := retentionFailedReason(ReasonHandoffOrphaned)
	if res == nil || res.Reason != wantReason {
		t.Fatalf("expected reason %q, got %+v", wantReason, res)
	}
	assertOrphanedHandoffSnapshot(t, cp, "htb-enqueue-retention-th", nil, "")
	assertRunMetaHandoffStatusMatchesSnapshot(t, res, cp, "htb-enqueue-retention-th")
}

func TestHandoffWithoutOutboxPopulatesResumeToken(t *testing.T) {
	t.Parallel()

	type state struct{}

	b := NewGraph[state, NoEffect](func(_ state, u state) state { return u })
	b.AddNode("n", func(_ context.Context, s state) (state, Directive, error) {
		return s, Handoff("job"), nil
	})
	b.AllowNoOutgoingRoute("n")
	b.SetEntryPoint("n")
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	cp := newMemoryCP[state, NoEffect]()
	res, err := g.NewRunner(cp).Start(context.Background(), "no-outbox-th", state{})
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	snap, _, loadErr := cp.Load(context.Background(), "no-outbox-th")
	if loadErr != nil {
		t.Fatalf("load: %v", loadErr)
	}
	if res.ResumeToken.ThreadID != "no-outbox-th" {
		t.Fatalf("expected thread no-outbox-th, got %+v", res.ResumeToken)
	}
	if res.ResumeToken.SnapshotRevision != snap.Revision {
		t.Fatalf("snapshot revision %d != revision %d", res.ResumeToken.SnapshotRevision, snap.Revision)
	}
	if snap.RunMeta.HandoffStatus != HandoffStatusNone {
		t.Fatalf("expected none handoff status without outbox, got %q", snap.RunMeta.HandoffStatus)
	}
	assertRunMetaHandoffStatusMatchesSnapshot(t, res, cp, "no-outbox-th")
}

func TestHandoffEnqueueFailWithRetentionFail(t *testing.T) {
	t.Parallel()

	type state struct{}

	outbox := &stubHandoffOutbox{err: errors.New("broker down")}
	cp := &failingMemoryCP[state, NoEffect]{
		memoryCP:  newMemoryCP[state, NoEffect](),
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

	handle, err := g.NewRunner(cp).Stream(context.Background(), "enqueue-retention-th", state{},
		WithHandoffOutbox[state, NoEffect](outbox),
	)
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
	events, waitErr := CollectEventsAndWait(context.Background(), handle)
	if !errors.Is(waitErr, ErrHandoffEnqueueFailed) {
		t.Fatalf("expected enqueue failure, got %v", waitErr)
	}
	if waitErr == nil {
		t.Fatal("expected combined enqueue+retention error")
	}
	if !strings.Contains(waitErr.Error(), "retention") {
		t.Fatalf("expected retention error in join chain, got %v", waitErr)
	}

	wantReason := retentionFailedReason(ReasonHandoffOrphaned)
	foundHandoff := false
	for _, ev := range events {
		if ev.Type == EventHandoff {
			foundHandoff = true
			if ev.Reason != wantReason {
				t.Fatalf("expected handoff reason %q, got %q", wantReason, ev.Reason)
			}
		}
	}
	if !foundHandoff {
		t.Fatalf("expected EventHandoff, events=%+v", events)
	}

	syncRes, syncErr := g.NewRunner(cp).Start(context.Background(), "enqueue-retention-sync-th", state{},
		WithHandoffOutbox[state, NoEffect](outbox),
	)
	if syncErr == nil {
		t.Fatalf("expected sync enqueue+retention error, got %+v", syncRes)
	}
	if syncRes.Reason != wantReason {
		t.Fatalf("sync reason: want %q, got %q", wantReason, syncRes.Reason)
	}
	assertTerminalEventReasonMatchesSync(t, events, EventHandoff, syncRes.Reason)

	assertOrphanedHandoffSnapshot(t, cp, "enqueue-retention-th", nil, "")
	assertRunMetaHandoffStatusMatchesSnapshot(t, syncRes, cp, "enqueue-retention-sync-th")
}

func TestHandoffEnqueueOkPatchEnqueuedFails(t *testing.T) {
	t.Parallel()

	type state struct{}

	outbox := &stubHandoffOutbox{}
	cp := &handoffPatchFailCP[state, NoEffect]{failOnStatus: HandoffStatusEnqueued}
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

	res, err := g.NewRunner(cp).Start(context.Background(), "patch-enqueued-fail-th", state{},
		WithHandoffOutbox[state, NoEffect](outbox),
	)
	if err == nil {
		t.Fatalf("expected patch failure error, got %+v", res)
	}
	if !errors.Is(err, ErrHandoffPatchFailed) {
		t.Fatalf("expected ErrHandoffPatchFailed, got %v", err)
	}
	if res == nil || res.Status != RunStatusHandoff {
		t.Fatalf("expected RunStatusHandoff on patch fail after enqueue, got %+v", res)
	}
	if len(outbox.calls) != 1 {
		t.Fatalf("expected one successful enqueue before patch fail, got %d", len(outbox.calls))
	}
	if res.ResumeToken.ThreadID == "" {
		t.Fatalf("expected populated ResumeToken, got %+v", res.ResumeToken)
	}
	assertOrphanedHandoffSnapshot(t, cp, "patch-enqueued-fail-th", res, "bg")
	assertRunMetaHandoffStatusMatchesSnapshot(t, res, cp, "patch-enqueued-fail-th")
	assertHandoffReasonMatchesStatus(t, res, cp, "patch-enqueued-fail-th", "bg")
}

func TestHandoffEnqueueFailPatchOrphanFails(t *testing.T) {
	t.Parallel()

	type state struct{}

	outbox := &stubHandoffOutbox{err: errors.New("broker down")}
	cp := &handoffPatchFailCP[state, NoEffect]{failOnStatus: HandoffStatusOrphaned}
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

	res, err := g.NewRunner(cp).Start(context.Background(), "patch-orphan-fail-th", state{},
		WithHandoffOutbox[state, NoEffect](outbox),
	)
	if !errors.Is(err, ErrHandoffEnqueueFailed) {
		t.Fatalf("expected ErrHandoffEnqueueFailed, got %v res=%+v", err, res)
	}
	if !errors.Is(err, ErrHandoffPatchFailed) {
		t.Fatalf("expected ErrHandoffPatchFailed in join, got %v", err)
	}
	if res == nil || res.Status != RunStatusHandoff {
		t.Fatalf("expected RunStatusHandoff, got %+v", res)
	}
	snap, rev, loadErr := cp.Load(context.Background(), "patch-orphan-fail-th")
	if loadErr != nil {
		t.Fatalf("load: %v", loadErr)
	}
	if snap.RunMeta.HandoffStatus != HandoffStatusPending {
		t.Fatalf("expected pending status when orphan patch fails, got %q", snap.RunMeta.HandoffStatus)
	}
	if res.ResumeToken.SnapshotRevision != rev {
		t.Fatalf("token revision %d != snapshot revision %d", res.ResumeToken.SnapshotRevision, rev)
	}
	assertRunMetaHandoffStatusMatchesSnapshot(t, res, cp, "patch-orphan-fail-th")
	assertHandoffReasonMatchesStatus(t, res, cp, "patch-orphan-fail-th", "bg")
}

func TestHandoffEnqueueOkBothPatchesFail(t *testing.T) {
	t.Parallel()

	type state struct{}

	outbox := &stubHandoffOutbox{}
	cp := &handoffPatchFailCP[state, NoEffect]{
		failOnStatuses: map[HandoffStatus]struct{}{ //nolint:exhaustive // only patch statuses under test
			HandoffStatusEnqueued: {},
			HandoffStatusOrphaned: {},
		},
	}
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

	res, err := g.NewRunner(cp).Start(context.Background(), "both-patch-fail-th", state{},
		WithHandoffOutbox[state, NoEffect](outbox),
	)
	if err == nil {
		t.Fatalf("expected patch failure error, got %+v", res)
	}
	if !errors.Is(err, ErrHandoffPatchFailed) {
		t.Fatalf("expected ErrHandoffPatchFailed, got %v", err)
	}
	if res == nil || res.Status != RunStatusHandoff {
		t.Fatalf("expected RunStatusHandoff, got %+v", res)
	}
	if len(outbox.calls) != 1 {
		t.Fatalf("expected one enqueue before patch failures, got %d", len(outbox.calls))
	}
	snap, _, loadErr := cp.Load(context.Background(), "both-patch-fail-th")
	if loadErr != nil {
		t.Fatalf("load: %v", loadErr)
	}
	if snap.RunMeta.HandoffStatus != HandoffStatusPending {
		t.Fatalf("expected pending when both patches fail, got %q", snap.RunMeta.HandoffStatus)
	}
	assertRunMetaHandoffStatusMatchesSnapshot(t, res, cp, "both-patch-fail-th")
	assertHandoffReasonMatchesStatus(t, res, cp, "both-patch-fail-th", "bg")
}

func TestHandoffRunMetaMatchesSnapshotOnEnqueueFail(t *testing.T) {
	t.Parallel()

	type state struct{}

	outbox := &stubHandoffOutbox{err: errors.New("broker down")}
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
	res, err := g.NewRunner(cp).Start(context.Background(), "runmeta-orphan-th", state{},
		WithHandoffOutbox[state, NoEffect](outbox),
	)
	if !errors.Is(err, ErrHandoffEnqueueFailed) {
		t.Fatalf("expected ErrHandoffEnqueueFailed, got %v", err)
	}
	assertRunMetaHandoffStatusMatchesSnapshot(t, res, cp, "runmeta-orphan-th")
}

// syncMetaLoadFailCP simulates transient Load failure after orphan patch persisted.
type syncMetaLoadFailCP[T, E any] struct {
	*memoryCP[T, E]
}

func (c *syncMetaLoadFailCP[T, E]) ensureMemoryCP() {
	if c.memoryCP == nil {
		c.memoryCP = newMemoryCP[T, E]()
	}
}

func (c *syncMetaLoadFailCP[T, E]) Save(
	ctx context.Context,
	expectedRevision uint64,
	snapshot Snapshot[T, E],
) (uint64, error) {
	c.ensureMemoryCP()
	return c.memoryCP.Save(ctx, expectedRevision, snapshot)
}

func (c *syncMetaLoadFailCP[T, E]) Load(
	ctx context.Context,
	threadID string,
) (Snapshot[T, E], uint64, error) {
	c.ensureMemoryCP()
	snap, rev, err := c.memoryCP.Load(ctx, threadID)
	if err != nil {
		return snap, rev, err
	}
	if snap.RunMeta.HandoffStatus == HandoffStatusOrphaned {
		return Snapshot[T, E]{}, 0, errors.New("transient load fail")
	}
	return snap, rev, nil
}

func (c *syncMetaLoadFailCP[T, E]) GetHistory(
	ctx context.Context,
	threadID string,
	limit int,
) ([]Snapshot[T, E], error) {
	c.ensureMemoryCP()
	return c.memoryCP.GetHistory(ctx, threadID, limit)
}

func (c *syncMetaLoadFailCP[T, E]) Prune(ctx context.Context, threadID string, limit int) error {
	c.ensureMemoryCP()
	return c.memoryCP.Prune(ctx, threadID, limit)
}

func (c *syncMetaLoadFailCP[T, E]) Delete(ctx context.Context, threadID string) error {
	c.ensureMemoryCP()
	return c.memoryCP.Delete(ctx, threadID)
}

func (c *syncMetaLoadFailCP[T, E]) DeleteIfIdle(ctx context.Context, threadID string) error {
	c.ensureMemoryCP()
	return c.memoryCP.DeleteIfIdle(ctx, threadID)
}

func TestSyncHandoffRunMetaUsesFSMStatusWhenLoadFails(t *testing.T) {
	t.Parallel()

	type state struct{}

	outbox := &stubHandoffOutbox{err: errors.New("broker down")}
	cp := &syncMetaLoadFailCP[state, NoEffect]{}
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

	res, err := g.NewRunner(cp).Start(context.Background(), "sync-meta-load-fail-th", state{},
		WithHandoffOutbox[state, NoEffect](outbox),
	)
	if !errors.Is(err, ErrHandoffEnqueueFailed) {
		t.Fatalf("expected ErrHandoffEnqueueFailed, got %v", err)
	}
	snap, _, loadErr := cp.memoryCP.Load(context.Background(), "sync-meta-load-fail-th")
	if loadErr != nil {
		t.Fatalf("direct load: %v", loadErr)
	}
	if snap.RunMeta.HandoffStatus != HandoffStatusOrphaned {
		t.Fatalf("expected orphaned in store, got %q", snap.RunMeta.HandoffStatus)
	}
	if res.Reason != ReasonHandoffOrphaned {
		t.Fatalf("expected orphaned reason from FSM when sync Load fails, got %q", res.Reason)
	}
	if res.RunMeta.HandoffStatus != HandoffStatusOrphaned {
		t.Fatalf("expected orphaned RunMeta from FSM, got %q", res.RunMeta.HandoffStatus)
	}
	if !res.RunMeta.HandoffPendingAt.IsZero() {
		t.Fatalf("expected HandoffPendingAt cleared on orphaned RunMeta, got %v", res.RunMeta.HandoffPendingAt)
	}
}

func TestResumeClearsHandoffEnqueuedStatus(t *testing.T) {
	t.Parallel()

	type state struct{ N int }

	outbox := &stubHandoffOutbox{}
	b := NewGraph[state, NoEffect](func(_ state, u state) state { return u })
	b.AddNode("work", func(_ context.Context, s state) (state, Directive, error) {
		s.N++
		if s.N == 1 {
			return s, Handoff("bg"), nil
		}
		if s.N == 2 {
			return s, Suspend("hold"), nil
		}
		return s, End(), nil
	})
	b.AllowNoOutgoingRoute("work")
	b.SetEntryPoint("work")
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	cp := newMemoryCP[state, NoEffect]()
	runner := g.NewRunner(cp)
	res, err := runner.Start(context.Background(), "resume-clear-enqueued-th", state{},
		WithHandoffOutbox[state, NoEffect](outbox),
	)
	if err != nil {
		t.Fatalf("handoff: %v", err)
	}
	snap, _, loadErr := cp.Load(context.Background(), "resume-clear-enqueued-th")
	if loadErr != nil {
		t.Fatalf("load after handoff: %v", loadErr)
	}
	if snap.RunMeta.HandoffStatus != HandoffStatusEnqueued {
		t.Fatalf("expected enqueued before resume, got %q", snap.RunMeta.HandoffStatus)
	}

	suspended, err := runner.Resume(context.Background(), res.ResumeToken)
	if err != nil {
		t.Fatalf("resume: %v", err)
	}
	if suspended.Status != RunStatusSuspended {
		t.Fatalf("expected suspended after first resume, got %s", suspended.Status)
	}
	snap, _, loadErr = cp.Load(context.Background(), "resume-clear-enqueued-th")
	if loadErr != nil {
		t.Fatalf("load after resume save: %v", loadErr)
	}
	if snap.RunMeta.HandoffStatus != HandoffStatusNone {
		t.Fatalf("expected HandoffStatusNone after resume save, got %q", snap.RunMeta.HandoffStatus)
	}
}

func TestHandoffSaveFailUsesResolvedPointer(t *testing.T) {
	t.Parallel()

	type state struct{}

	cp := &failingMemoryCP[state, NoEffect]{failSave: true}
	b := NewGraph[state, NoEffect](func(_ state, u state) state { return u })
	b.AddNode("wait", func(_ context.Context, s state) (state, Directive, error) {
		return s, Handoff("bg"), nil
	})
	b.AddNode("router", func(_ context.Context, s state) (state, Directive, error) {
		return s, Handoff("bg"), nil
	})
	b.AllowNoOutgoingRoute("wait")
	b.AllowNoOutgoingRoute("router")
	b.SetEntryPoint("wait")
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	res, err := g.NewRunner(cp).Start(context.Background(), "handoff-ptr-th", state{},
		WithSuspendPointerResolver[state, NoEffect](func(_ state, _ ExecutionPointer) (ExecutionPointer, error) {
			return "router", nil
		}),
	)
	if err == nil {
		t.Fatalf("expected save failure, got %+v", res)
	}
	if string(res.ExecutionPointer) != "router" {
		t.Fatalf("expected failed result pointer router, got %q", res.ExecutionPointer)
	}
}

func TestTransactionalHandoffNoSnapshotOnEnqueueFail(t *testing.T) {
	t.Parallel()

	type state struct{}

	outbox := &stubHandoffOutbox{err: errors.New("broker down")}
	cp := &transactionalMemoryCP[state, NoEffect]{memoryCP: newMemoryCP[state, NoEffect]()}
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

	_, err = g.NewRunner(cp).Start(context.Background(), "tx-handoff-fail-th", state{},
		WithHandoffOutbox[state, NoEffect](outbox),
	)
	if err == nil {
		t.Fatal("expected transactional handoff failure")
	}
	requireSnapshotMissing(t, cp, "tx-handoff-fail-th")
}

func TestTransactionalHandoffSuccess(t *testing.T) {
	t.Parallel()

	type state struct{}

	outbox := &stubHandoffOutbox{}
	cp := &transactionalMemoryCP[state, NoEffect]{memoryCP: newMemoryCP[state, NoEffect]()}
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

	res, err := g.NewRunner(cp).Start(context.Background(), "tx-handoff-ok-th", state{},
		WithHandoffOutbox[state, NoEffect](outbox),
	)
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	if res.Status != RunStatusHandoff {
		t.Fatalf("expected handoff, got %s", res.Status)
	}
	snap, rev, loadErr := cp.Load(context.Background(), "tx-handoff-ok-th")
	if loadErr != nil {
		t.Fatalf("load: %v", loadErr)
	}
	if snap.RunMeta.HandoffStatus != HandoffStatusEnqueued {
		t.Fatalf("expected enqueued, got %q", snap.RunMeta.HandoffStatus)
	}
	if len(outbox.calls) != 1 {
		t.Fatalf("expected one enqueue, got %d", len(outbox.calls))
	}
	if outbox.calls[0].SnapshotRevision != rev {
		t.Fatalf("outbox token rev %d != snapshot rev %d", outbox.calls[0].SnapshotRevision, rev)
	}
	assertRunMetaHandoffStatusMatchesSnapshot(t, res, cp, "tx-handoff-ok-th")
}

func TestTransactionalHandoffFailureRunMeta(t *testing.T) {
	t.Parallel()

	type state struct{}

	outbox := &stubHandoffOutbox{err: errors.New("broker down")}
	cp := &transactionalMemoryCP[state, NoEffect]{memoryCP: newMemoryCP[state, NoEffect]()}
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

	res, err := g.NewRunner(cp).Start(context.Background(), "tx-fail-runmeta-th", state{},
		WithHandoffOutbox[state, NoEffect](outbox),
	)
	if err == nil {
		t.Fatal("expected transactional handoff failure")
	}
	if res == nil {
		t.Fatal("expected failed RunResult")
	}
	if res.RunMeta.HandoffStatus != HandoffStatusNone {
		t.Fatalf("expected none handoff status after TX rollback, got %q", res.RunMeta.HandoffStatus)
	}
	requireSnapshotMissing(t, cp, "tx-fail-runmeta-th")
}

func TestHandoffWithRunLeaseAndMemoryCPUsesThreePhaseFSM(t *testing.T) {
	t.Parallel()

	type state struct{}

	outbox := &stubHandoffOutbox{}
	lease := NewMemoryLeaseManager()
	cp := newMemoryCP[state, NoEffect]()
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

	runner := g.NewRunnerWithOptions(cp, []RunnerOption[state, NoEffect]{
		WithLeaseManager[state, NoEffect](lease),
	})
	res, err := runner.Start(context.Background(), "lease-handoff-th", state{},
		WithHandoffOutbox[state, NoEffect](outbox),
		WithRunLease[state, NoEffect]("worker-1", time.Minute),
	)
	if err != nil {
		t.Fatalf("handoff with lease+memory: %v", err)
	}
	if res == nil || res.Status != RunStatusHandoff {
		t.Fatalf("expected handoff, got %+v err=%v", res, err)
	}
	if len(outbox.calls) != 1 {
		t.Fatalf("expected one outbox enqueue via 3-phase FSM, got %d", len(outbox.calls))
	}
	assertHandoffTokenRevisionContract(t, outbox, res, cp, "lease-handoff-th")
}
