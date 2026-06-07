package flowy

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"
)

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
	if res.Status != RunStatusHandoff {
		t.Fatalf("expected handoff status, got %s", res.Status)
	}
	snap, loadErr := cp.Load(context.Background(), "schedule-fail-th")
	if loadErr != nil {
		t.Fatalf("snapshot must be preserved after schedule failure: %v", loadErr)
	}
	if res.ResumeToken.SnapshotRevision != snap.Revision {
		t.Fatalf("snapshot revision %d != snapshot revision %d", res.ResumeToken.SnapshotRevision, snap.Revision)
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
	if token.ThreadID != "schedule-ok-th" || token.SnapshotRevision != res.ResumeToken.SnapshotRevision {
		t.Fatalf("scheduler token mismatch: got %+v result token %+v", token, res.ResumeToken)
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

func TestRequestLocalHandoffContextCancelDuringSave(t *testing.T) {
	t.Parallel()

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
	if _, loadErr := cp.Load(context.Background(), "htb-skip-on-save-th"); loadErr == nil {
		t.Fatal("expected no snapshot on skip-on-save handoff skip")
	}
}

func TestRequestLocalHandoffScheduleFailAfterPersist(t *testing.T) {
	t.Parallel()

	type state struct{}

	scheduler := &stubHandoffScheduler{err: errors.New("broker down")}
	g, ready := blockingHandoffWorkGraph[state, NoEffect](t)
	cp := newMemoryCP[state, NoEffect]()
	runner := g.NewRunner(cp)

	startDone := make(chan *RunResult[state, NoEffect], 1)
	go func() {
		res, _ := runner.Start(context.Background(), "htb-sched-th", state{},
			WithHandoffScheduler[state, NoEffect](scheduler),
		)
		startDone <- res
	}()

	<-ready
	handoffErr := runner.RequestLocalHandoff(context.Background(), "htb-sched-th")
	if !errors.Is(handoffErr, ErrHandoffScheduleFailed) {
		t.Fatalf("expected ErrHandoffScheduleFailed, got %v", handoffErr)
	}
	res := <-startDone
	if res == nil || res.ResumeToken.ThreadID == "" {
		t.Fatalf("expected ResumeToken after schedule fail, got %+v", res)
	}
	if _, loadErr := cp.Load(context.Background(), "htb-sched-th"); loadErr != nil {
		t.Fatalf("snapshot must exist after schedule fail: %v", loadErr)
	}
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
		memoryCP:  *newMemoryCP[state, NoEffect](),
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

	startDone := make(chan error, 1)
	go func() {
		_, runErr := runner.Start(context.Background(), "htb-retention-th", state{})
		startDone <- runErr
	}()

	<-ready
	handoffErr := runner.RequestLocalHandoff(context.Background(), "htb-retention-th")
	if handoffErr == nil {
		t.Fatal("expected retention error on handoff")
	}
	if !strings.Contains(handoffErr.Error(), "retention") {
		t.Fatalf("expected retention in error, got %v", handoffErr)
	}
	if _, loadErr := cp.Load(context.Background(), "htb-retention-th"); loadErr != nil {
		t.Fatalf("snapshot must exist despite retention failure: %v", loadErr)
	}
	<-startDone
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
	if _, loadErr := cp.Load(context.Background(), "htb-resolve-th"); !errors.Is(loadErr, ErrThreadNotFound) {
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
	}
}

func TestRequestLocalHandoffScheduleAndRetentionJoin(t *testing.T) {
	t.Parallel()

	type state struct{}

	scheduler := &stubHandoffScheduler{err: errors.New("broker down")}
	cp := &failingMemoryCP[state, NoEffect]{
		memoryCP:  *newMemoryCP[state, NoEffect](),
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

	startDone := make(chan error, 1)
	go func() {
		_, runErr := runner.Start(context.Background(), "htb-sched-retention-th", state{},
			WithHandoffScheduler[state, NoEffect](scheduler),
		)
		startDone <- runErr
	}()

	<-ready
	handoffErr := runner.RequestLocalHandoff(context.Background(), "htb-sched-retention-th")
	if !errors.Is(handoffErr, ErrHandoffScheduleFailed) {
		t.Fatalf("expected schedule failure, got %v", handoffErr)
	}
	if !strings.Contains(handoffErr.Error(), "retention") {
		t.Fatalf("expected retention error in join chain, got %v", handoffErr)
	}
	if _, loadErr := cp.Load(context.Background(), "htb-sched-retention-th"); loadErr != nil {
		t.Fatalf("snapshot must exist after schedule+retention fail: %v", loadErr)
	}
	if runErr := <-startDone; runErr == nil {
		t.Fatal("expected combined schedule+retention error from Start")
	}
}

func TestHandoffWithoutSchedulerPopulatesResumeToken(t *testing.T) {
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
	res, err := g.NewRunner(cp).Start(context.Background(), "no-sched-th", state{})
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	snap, loadErr := cp.Load(context.Background(), "no-sched-th")
	if loadErr != nil {
		t.Fatalf("load: %v", loadErr)
	}
	if res.ResumeToken.ThreadID != "no-sched-th" {
		t.Fatalf("expected thread no-sched-th, got %+v", res.ResumeToken)
	}
	if res.ResumeToken.SnapshotRevision != snap.Revision {
		t.Fatalf("snapshot revision %d != revision %d", res.ResumeToken.SnapshotRevision, snap.Revision)
	}
}

func TestHandoffScheduleFailWithRetentionFail(t *testing.T) {
	t.Parallel()

	type state struct{}

	scheduler := &stubHandoffScheduler{err: errors.New("broker down")}
	cp := &failingMemoryCP[state, NoEffect]{
		memoryCP:  *newMemoryCP[state, NoEffect](),
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

	handle, err := g.NewRunner(cp).Stream(context.Background(), "sched-retention-th", state{},
		WithHandoffScheduler[state, NoEffect](scheduler),
	)
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
	events, waitErr := CollectEventsAndWait(context.Background(), handle)
	if !errors.Is(waitErr, ErrHandoffScheduleFailed) {
		t.Fatalf("expected schedule failure, got %v", waitErr)
	}
	if waitErr == nil {
		t.Fatal("expected combined schedule+retention error")
	}
	if !strings.Contains(waitErr.Error(), "retention") {
		t.Fatalf("expected retention error in join chain, got %v", waitErr)
	}

	wantReason := "bg_retention_failed"
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

	syncRes, syncErr := g.NewRunner(cp).Start(context.Background(), "sched-retention-sync-th", state{},
		WithHandoffScheduler[state, NoEffect](scheduler),
	)
	if syncErr == nil {
		t.Fatalf("expected sync schedule+retention error, got %+v", syncRes)
	}
	if syncRes.Reason != wantReason {
		t.Fatalf("sync reason: want %q, got %q", wantReason, syncRes.Reason)
	}
	assertTerminalEventReasonMatchesSync(t, events, EventHandoff, syncRes.Reason)
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
