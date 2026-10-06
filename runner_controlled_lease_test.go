package flowy

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

// controlledLossLease keeps storage time fixed and releases Renew only on an
// explicit ownership change or run cancellation. No scheduler/TTL race selects loss.
type controlledLossLease struct {
	*MemoryLeaseManager

	lost chan struct{}
}

func newControlledLossLease() *controlledLossLease {
	memory := NewMemoryLeaseManager()
	now := time.Now().UTC()
	memory.nowFunc = func() time.Time { return now }
	return &controlledLossLease{MemoryLeaseManager: memory, lost: make(chan struct{})}
}

func (l *controlledLossLease) Renew(
	ctx context.Context,
	lease ExecutionLease,
	ttl time.Duration,
) (ExecutionLease, error) {
	select {
	case <-l.lost:
		return l.MemoryLeaseManager.Renew(ctx, lease, ttl)
	case <-ctx.Done():
		return ExecutionLease{}, ctx.Err()
	}
}

func (l *controlledLossLease) trigger(t *testing.T, id string) {
	t.Helper()
	forceLeaseTakeover(t, l.MemoryLeaseManager, id)
	close(l.lost)
}

//nolint:nestif // Assertions distinguish active handoff from an already closed session.
func assertControlledHandoffLeaseLoss(t *testing.T, streaming, requestFirst bool) {
	t.Helper()
	// Arrange: keep the node/session active until the test allows its return.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ready := make(chan struct{})
	canceled := make(chan struct{})
	exit := make(chan struct{})
	var exitOnce sync.Once
	t.Cleanup(func() { exitOnce.Do(func() { close(exit) }) })
	lease := newControlledLossLease()
	graph := ownershipGraph(t, func(nodeCtx context.Context, state int) (int, Directive, error) {
		close(ready)
		<-nodeCtx.Done()
		close(canceled)
		<-exit
		return state, Completed(), nil
	})
	cp := newMemoryCP[int, NoEffect]()
	runner := graph.NewRunnerWithOptions(cp, []RunnerOption[int, NoEffect]{WithLeaseManager[int, NoEffect](lease)})
	opts := []RunOption[int, NoEffect]{WithRunLease[int, NoEffect]("worker-a", 50*time.Millisecond)}
	type outcome struct {
		result *RunResult[int, NoEffect]
		err    error
	}
	done := make(chan outcome, 1)
	if streaming {
		handle, err := runner.Stream(ctx, "controlled-loss", 42, opts...)
		if err != nil {
			t.Fatal(err)
		}
		go func() { result, err := handle.WaitResult(); done <- outcome{result, err} }()
	} else {
		go func() { result, err := runner.Start(ctx, "controlled-loss", 42, opts...); done <- outcome{result, err} }()
	}
	<-ready
	// Act: trigger loss either while a requested handoff is blocked, or before any request.
	var handoff <-chan error
	if requestFirst {
		requested := make(chan error, 1)
		handoff = requested
		go func() { requested <- runner.RequestLocalHandoff(ctx, "controlled-loss") }()
		<-canceled
		lease.trigger(t, "controlled-loss")
	} else {
		lease.trigger(t, "controlled-loss")
		<-canceled
	}
	exitOnce.Do(func() { close(exit) })
	finished := <-done
	// Assert: loss is explicitly addressed; another owner's lease is never released.
	if requestFirst {
		if err := <-handoff; err != nil {
			t.Fatalf("active handoff rejected: %v", err)
		}
		if finished.result.Status != RunStatusHandoff || !errors.Is(finished.err, ErrRunCleanup) ||
			!errors.Is(finished.err, ErrLeaseLost) {
			t.Fatalf("result=%+v", finished.result)
		}
	} else {
		if !errors.Is(finished.err, ErrLeaseLost) || finished.result.Status != RunStatusFailed {
			t.Fatalf("result=%+v err=%v", finished.result, finished.err)
		}
		if err := runner.RequestLocalHandoff(ctx, "controlled-loss"); !errors.Is(err, ErrNoActiveExecution) {
			t.Fatalf("closed session handoff=%v", err)
		}
		if len(cp.hist) != 0 {
			t.Fatal("lost lease published a checkpoint")
		}
	}
	owner, held, err := lease.Holder(ctx, "controlled-loss")
	if err != nil || !held || owner != "worker-b" {
		t.Fatalf("new owner lease changed: %q held=%v err=%v", owner, held, err)
	}
}
