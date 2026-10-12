package flowy

import (
	"context"
	"errors"
	"sync"
)

// streamEventCanceled observes cancellation without blocking the producer.
func streamEventCanceled(ctx context.Context, stop <-chan struct{}) bool {
	select {
	case <-stop:
		return true
	case <-ctx.Done():
		return true
	default:
		return false
	}
}

func streamConsumerClosed(ctx context.Context) bool {
	type stopChecker interface {
		stopped() bool
	}
	sc, ok := ctx.Value(streamCloseKey{}).(stopChecker)
	return ok && sc.stopped()
}

func (s *streamHandle[T, E]) Events() <-chan RunEvent[T, E] {
	return s.events
}

func (s *streamHandle[T, E]) RequestStop() {
	s.once.Do(func() {
		close(s.stop)
		if s.onStop != nil {
			s.onStop()
		}
	})
}

func (s *streamHandle[T, E]) Wait() error {
	<-s.done
	return s.err
}

func (s *streamHandle[T, E]) WaitResult() (*RunResult[T, E], error) {
	<-s.done
	return s.result, s.err
}

func (s *streamHandle[T, E]) stopped() bool {
	select {
	case <-s.stop:
		return true
	default:
		return false
	}
}

func (s *streamHandle[T, E]) publishEvent(
	eventCtx context.Context, event RunEvent[T, E], inv runInvocationOptions[T, E],
) bool {
	if streamEventCanceled(eventCtx, s.stop) {
		return false
	}
	// This sink has one serialized producer. Concurrent consumers can only
	// free capacity, so a free slot remains available during detachment.
	// A full-buffer observation drops immediately without cloning.
	if len(s.events) == cap(s.events) {
		return true
	}
	if inv.cloneState != nil {
		event.State = inv.cloneState(event.State)
	}
	if event.HasEffect && inv.cloneEffect != nil {
		event.Effect = inv.cloneEffect(event.Effect)
	}
	// Prefer cancellation already observed during a host clone to publication.
	if streamEventCanceled(eventCtx, s.stop) {
		return false
	}
	select {
	case <-s.stop:
		return false
	case <-eventCtx.Done():
		return false
	case s.events <- event:
		return true
	default:
		return true
	}
}

func (r *graphRunner[T, E]) startStream(
	ctx context.Context,
	inv runInvocationOptions[T, E],
	runFn func(context.Context, eventSink[T, E]) (*RunResult[T, E], error),
) StreamHandle[T, E] {
	streamCtx, cancelStream := context.WithCancelCause(ctx)
	stream := &streamHandle[T, E]{
		events: make(chan RunEvent[T, E], streamEventBufferSize),
		stop:   make(chan struct{}),
		done:   make(chan struct{}),
		once:   sync.Once{},
		err:    nil,
		result: nil,
		onStop: func() {
			cancelStream(context.Canceled)
		},
	}
	go func() {
		defer cancelStream(context.Canceled)
		defer close(stream.events)
		defer close(stream.done)
		sink := func(eventCtx context.Context, event RunEvent[T, E]) bool {
			return stream.publishEvent(eventCtx, event, inv)
		}
		runCtx := context.WithValue(streamCtx, streamCloseKey{}, stream)
		result, err := runFn(runCtx, sink)
		if errors.Is(err, context.Canceled) && stream.stopped() && ctx.Err() == nil &&
			!errors.Is(err, ErrCheckpointSkipped) && !errors.Is(err, ErrRunCleanup) {
			err = nil
		}
		stream.result = result
		stream.err = err
	}()
	return stream
}
