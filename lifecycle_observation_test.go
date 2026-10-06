package flowy

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

type observationCallback func(context.Context, LifecycleObservation)

func (callback observationCallback) ObserveLifecycle(ctx context.Context, event LifecycleObservation) {
	callback(ctx, event)
}

func TestObservationPanicIsContainedAndNilDisablesCallbacks(t *testing.T) {
	// Arrange: a misbehaving observer cannot turn diagnostics into execution authority.
	var calls atomic.Int32
	SetLifecycleObserver(
		observationCallback(func(context.Context, LifecycleObservation) { calls.Add(1); panic("observer failed") }),
	)
	t.Cleanup(func() { SetLifecycleObserver(nil) })
	// Act: both panic callbacks return to their caller; disabled observation is inert.
	event := lifecycleObservation(LifecycleCheckpoint, LifecycleCommitted, "run", "node")
	observeLifecycle(context.Background(), event)
	observeLifecycle(context.Background(), event)
	SetLifecycleObserver(nil)
	observeLifecycle(context.Background(), event)
	// Assert.
	if calls.Load() != 2 {
		t.Fatalf("panic/disable policy: calls=%d", calls.Load())
	}
}

func TestBlockingObserverIsSynchronousAndDoesNotHoldInstallationLock(t *testing.T) {
	// Arrange: exactly one test-owned calling goroutine; core must add no queue/worker.
	entered, release, returned := make(chan struct{}), make(chan struct{}), make(chan struct{})
	SetLifecycleObserver(observationCallback(func(context.Context, LifecycleObservation) { close(entered); <-release }))
	t.Cleanup(func() { SetLifecycleObserver(nil) })
	go func() {
		observeLifecycle(context.Background(), lifecycleObservation(LifecycleActivity, LifecycleStarted, "run", "node"))
		close(returned)
	}()
	// Act: while the callback is blocked, installation remains independent.
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("callback never entered")
	}
	SetLifecycleObserver(nil)
	select {
	case <-returned:
		t.Fatal("observer was secretly queued instead of synchronous")
	default:
	}
	close(release)
	// Assert: release unblocks that same caller, with no external acknowledgement.
	select {
	case <-returned:
	case <-time.After(time.Second):
		t.Fatal("caller did not return")
	}
}
