package flowy

import (
	"context"
	"errors"
	"sync"
)

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
			select {
			case <-stream.stop:
				return false
			case <-eventCtx.Done():
				return false
			default:
			}
			if inv.cloneState != nil {
				event.State = inv.cloneState(event.State)
			}
			if event.HasEffect && inv.cloneEffect != nil {
				event.Effect = inv.cloneEffect(event.Effect)
			}
			select {
			case <-stream.stop:
				return false
			case <-eventCtx.Done():
				return false
			case stream.events <- event:
				return true
			default:
				return true
			}
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
