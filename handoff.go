package flowy

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

// ErrHandoffRequested is returned when execution ends for background handoff.
var ErrHandoffRequested = errors.New("flowy: handoff to background requested")

// ErrHandoffNotCompleted is returned when HandoffToBackground times out waiting for checkpoint save.
var ErrHandoffNotCompleted = errors.New("flowy: handoff did not complete before deadline")

const handoffCompletionTimeout = 30 * time.Second

type runSession struct {
	cancel context.CancelCauseFunc
	done   chan struct{}
	once   sync.Once
	err    atomic.Pointer[error]
}

func newRunSession(cancel context.CancelCauseFunc) *runSession {
	return &runSession{
		cancel: cancel,
		done:   make(chan struct{}),
		once:   sync.Once{},
		err:    atomic.Pointer[error]{},
	}
}

func (s *runSession) finish(err error) {
	s.once.Do(func() {
		if err != nil {
			s.err.Store(&err)
		}
		close(s.done)
	})
}

func (s *runSession) completionError() error {
	if p := s.err.Load(); p != nil {
		return *p
	}
	return nil
}
