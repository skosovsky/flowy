package flowy

import (
	"context"
	"errors"
	"slices"
	"sync"
	"testing"
)

type collectionProbeHandle struct {
	events   chan RunEvent[int, NoEffect]
	stopped  chan struct{}
	waited   chan struct{}
	stopOnce sync.Once
	waitErr  error
}

func (h *collectionProbeHandle) Events() <-chan RunEvent[int, NoEffect] { return h.events }

func (h *collectionProbeHandle) RequestStop()                                   { h.stopOnce.Do(func() { close(h.stopped) }) }
func (h *collectionProbeHandle) Wait() error                                    { close(h.waited); return h.waitErr }
func (h *collectionProbeHandle) WaitResult() (*RunResult[int, NoEffect], error) { return nil, h.Wait() }
func collectionEvent(state int) RunEvent[int, NoEffect] {
	return RunEvent[int, NoEffect]{
		Type:             EventNodeCompleted,
		ExecutionPointer: "node",
		State:            state,
		Effect:           NoEffect{},
		HasEffect:        false,
		Error:            nil,
		Duration:         0,
		Reason:           "",
	}
}
func newCollectionProbe(buffer int, waitErr error) *collectionProbeHandle {
	return &collectionProbeHandle{
		events:   make(chan RunEvent[int, NoEffect], buffer),
		stopped:  make(chan struct{}),
		waited:   make(chan struct{}),
		stopOnce: sync.Once{},
		waitErr:  waitErr,
	}
}
func TestCollectEventsCanceledClosedBufferOwnsReturnedSlice(t *testing.T) {
	t.Parallel()
	// Arrange: a legal closed buffered source can keep draining after early return.
	const count = 100000
	handle := newCollectionProbe(count, nil)
	for i := range count {
		handle.events <- collectionEvent(i)
	}
	close(handle.events)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	// Act.
	events, err := CollectEventsAndWait(ctx, handle)
	// Assert: the caller owns returned storage even while the background drain finishes.
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error=%v", err)
	}
	snapshot := slices.Clone(events)
	// Touch and extend caller storage while the background callback may append.
	for i := range events {
		events[i].Reason = "caller-owned"
	}
	for range count {
		events = append(events, collectionEvent(-1))
	}
	<-handle.waited
	for i, original := range snapshot {
		if events[i].State != original.State || events[i].Reason != "caller-owned" {
			t.Fatal("late drain mutated returned events")
		}
	}
}
func TestCollectEventsCanceledOpenProducerReturnsBeforeDrain(t *testing.T) {
	t.Parallel()
	// Arrange: producer ignores stop and stays open until explicitly released.
	handle := newCollectionProbe(0, nil)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	// Act.
	events, err := CollectEventsAndWait(ctx, handle)
	// Assert: return does not wait for an uncooperative producer; drain still owns late events.
	if !errors.Is(err, context.Canceled) || len(events) != 0 {
		t.Fatalf("events=%v err=%v", events, err)
	}
	<-handle.stopped
	select {
	case <-handle.waited:
		t.Fatal("Wait called before source closure")
	default:
	}
	handle.events <- collectionEvent(42)
	close(handle.events)
	<-handle.waited
	if len(events) != 0 {
		t.Fatal("returned slice changed after late delivery")
	}
}
func TestCollectEventsSuccessfulDrainKeepsAllEventsAndWaitError(t *testing.T) {
	t.Parallel()
	// Arrange.
	waitErr := errors.New("terminal error")
	handle := newCollectionProbe(3, waitErr)
	for i := range 3 {
		handle.events <- collectionEvent(i)
	}
	close(handle.events)
	// Act.
	events, err := CollectEventsAndWait(t.Context(), handle)
	// Assert.
	if !errors.Is(err, waitErr) || len(events) != 3 {
		t.Fatalf("events=%v err=%v", events, err)
	}
	for i, event := range events {
		if event.State != i {
			t.Fatalf("order=%v", events)
		}
	}
}

func TestConsumeEventsCancellationDoesNotJoinBlockedCallback(t *testing.T) {
	t.Parallel()
	// Arrange: hold one callback while cancellation returns to its caller.
	handle := newCollectionProbe(1, nil)
	handle.events <- collectionEvent(42)
	close(handle.events)
	entered, release := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	finished := make(chan error, 1)
	go func() {
		finished <- ConsumeEventsAndWait(ctx, handle, func(_ RunEvent[int, NoEffect]) bool {
			close(entered)
			<-release
			return true
		})
	}()
	<-entered
	// Act.
	cancel()
	consumeErr := <-finished
	// Assert: callback lifetime continues independently of the canceled call.
	if !errors.Is(consumeErr, context.Canceled) {
		t.Fatalf("error=%v", consumeErr)
	}
	select {
	case <-handle.waited:
		t.Fatal("Wait called before callback returned")
	default:
	}
	releaseOnce.Do(func() { close(release) })
	<-handle.waited
}
