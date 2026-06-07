package flowy

import (
	"context"
	"errors"
)

// StreamCollectResult holds the outcome of BeginStreamCollect after AwaitStreamCollect.
// For terminal reason/state when a terminal event may be dropped (persist-vs-event),
// prefer Snapshot over TerminalEvent.
type StreamCollectResult[T, E any] struct {
	Events        []RunEvent[T, E]
	WaitErr       error
	TerminalEvent *RunEvent[T, E]
	Snapshot      *Snapshot[T, E]
	ResumeToken   ResumeToken
}

func isTerminalEventType(t EventType) bool {
	switch t {
	case EventCompleted, EventSuspended, EventFailed, EventHandoff, EventContextCanceled:
		return true
	default:
		return false
	}
}

// terminalEventFromEvents returns the last terminal event in the slice, if any.
func terminalEventFromEvents[T, E any](events []RunEvent[T, E]) (RunEvent[T, E], bool) {
	for i := len(events) - 1; i >= 0; i-- {
		if isTerminalEventType(events[i].Type) {
			return events[i], true
		}
	}
	return RunEvent[T, E]{}, false
}

func terminalEventPtr[T, E any](events []RunEvent[T, E]) *RunEvent[T, E] {
	if ev, ok := terminalEventFromEvents(events); ok {
		ev := ev
		return &ev
	}
	return nil
}

// ConsumeEventsAndWait drains Events on a background goroutine, invokes onEvent for each
// delivered event, then calls Wait once after the channel closes.
//
// Returning false from onEvent calls RequestStop and silently drains remaining events
// without invoking the callback again. The function still waits for terminal persistence.
//
// onEvent runs on the drain goroutine; it must not block indefinitely and must not panic.
// Safe concurrent use: RequestStop, parent ctx cancel, or RequestLocalHandoff from
// another goroutine while this blocks.
func ConsumeEventsAndWait[T, E any](
	ctx context.Context,
	h StreamHandle[T, E],
	onEvent func(RunEvent[T, E]) bool,
) error {
	if onEvent == nil {
		onEvent = func(RunEvent[T, E]) bool { return true }
	}

	errCh := make(chan error, 1)
	go func() {
		stopRequested := false
		for ev := range h.Events() {
			if stopRequested {
				continue
			}
			if !onEvent(ev) {
				stopRequested = true
				h.RequestStop()
			}
		}
		errCh <- h.Wait()
	}()

	select {
	case waitErr := <-errCh:
		return waitErr
	case <-ctx.Done():
		h.RequestStop()
		select {
		case waitErr := <-errCh:
			return errors.Join(ctx.Err(), waitErr)
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// CollectEventsAndWait is a convenience wrapper that collects all events then Wait().
func CollectEventsAndWait[T, E any](ctx context.Context, h StreamHandle[T, E]) ([]RunEvent[T, E], error) {
	var events []RunEvent[T, E]
	err := ConsumeEventsAndWait(ctx, h, func(ev RunEvent[T, E]) bool {
		events = append(events, ev)
		return true
	})
	return events, err
}

// BeginStreamCollect starts a background drain of Events followed by Wait.
// The caller triggers termination (RequestStop, ctx cancel, RequestLocalHandoff),
// then receives the result via AwaitStreamCollect.
//
// Pair with AwaitStreamCollect on the same StreamHandle; do not drain Events on the
// caller goroutine while BeginStreamCollect is active.
func BeginStreamCollect[T, E any](h StreamHandle[T, E]) <-chan StreamCollectResult[T, E] {
	out := make(chan StreamCollectResult[T, E], 1)
	go func() {
		var events []RunEvent[T, E]
		for ev := range h.Events() {
			events = append(events, ev)
		}
		waitErr := h.Wait()
		out <- StreamCollectResult[T, E]{
			Events:        events,
			WaitErr:       waitErr,
			TerminalEvent: terminalEventPtr(events),
			Snapshot:      nil,
			ResumeToken:   ResumeToken{ThreadID: "", SnapshotRevision: 0},
		}
	}()
	return out
}

// AwaitStreamCollect waits for BeginStreamCollect to finish or until ctx is canceled.
// On ctx cancel it calls RequestStop on h (symmetry with ConsumeEventsAndWait) and joins
// ctx.Err() with the drain goroutine's Wait error when available.
func AwaitStreamCollect[T, E any](
	ctx context.Context,
	h StreamHandle[T, E],
	out <-chan StreamCollectResult[T, E],
) (StreamCollectResult[T, E], error) {
	select {
	case result := <-out:
		return result, result.WaitErr
	case <-ctx.Done():
		h.RequestStop()
		select {
		case result := <-out:
			return result, errors.Join(ctx.Err(), result.WaitErr)
		case <-ctx.Done():
			return StreamCollectResult[T, E]{
				Events:        nil,
				WaitErr:       ctx.Err(),
				TerminalEvent: nil,
				Snapshot:      nil,
				ResumeToken:   ResumeToken{ThreadID: "", SnapshotRevision: 0},
			}, ctx.Err()
		}
	}
}

// AwaitStreamCollectWithSnapshot awaits collection and loads the persisted snapshot.
// ResumeToken is built via ResumeTokenFromSnapshot for Handoff/HITL flows.
func AwaitStreamCollectWithSnapshot[T, E any](
	ctx context.Context,
	h StreamHandle[T, E],
	out <-chan StreamCollectResult[T, E],
	cp Checkpointer[T, E],
	threadID string,
) (StreamCollectResult[T, E], error) {
	result, err := AwaitStreamCollect(ctx, h, out)
	if err != nil && len(result.Events) == 0 {
		return result, err
	}
	snap, loadErr := cp.Load(ctx, threadID)
	if loadErr != nil {
		return result, loadErr
	}
	result.Snapshot = &snap
	result.ResumeToken = ResumeTokenFromSnapshot(snap)
	return result, result.WaitErr
}
