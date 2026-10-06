package flowy

import (
	"context"
	"errors"
)

func makeExecutionTerminal(status RunStatus, reason string, err error) *ExecutionTerminal {
	terminal := &ExecutionTerminal{Status: status, Reason: reason, Failure: nil}
	if status != RunStatusFailed {
		return terminal
	}
	message := reason
	if err != nil {
		message = err.Error()
	}
	terminal.Failure = &ExecutionFailure{Message: message}
	return terminal
}

func (c *executionCheckpointer[T, E]) finalizeRunOutcome(
	ctx context.Context,
	result *RunResult[T, E],
	runErr error,
	sink eventSink[T, E],
	terminal *RunEvent[T, E],
) error {
	if result != nil && result.Status == RunStatusFailed && runErr != nil {
		if commitErr := c.commitRunFailure(ctx, result, runErr); commitErr != nil {
			runErr = errors.Join(runErr, commitErr)
		}
	}
	c.mu.Lock()
	if result != nil {
		result.ResumeToken = ResumeToken{ThreadID: c.envelope.ExecutionID, SnapshotRevision: c.envelope.Revision}
	}
	committedFailure := c.envelope.Terminal != nil && c.envelope.Terminal.Status == RunStatusFailed
	c.mu.Unlock()
	if terminal != nil && (runErr == nil || committedFailure) {
		sink(ctx, *terminal)
	}
	return runErr
}

// Node failures outside directive handling also need a terminal commit. A
// recoverable journal/storage/ownership failure is not a definitive node result.
func (c *executionCheckpointer[T, E]) commitRunFailure(
	ctx context.Context,
	result *RunResult[T, E],
	runErr error,
) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if ctx.Err() != nil || c.persistenceFailed || c.envelope.Terminal != nil || recoverableExecutionFailure(runErr) {
		return nil
	}
	if c.resolvedActivitiesLocked() != nil {
		return nil
	}
	target := cloneExecutionEnvelope(c.envelope)
	target.Terminal = &ExecutionTerminal{
		Status:  RunStatusFailed,
		Reason:  result.Reason,
		Failure: &ExecutionFailure{Message: runErr.Error()},
	}
	meta := result.RunMeta
	markSegmentFailed(&meta)
	_, err := c.persistSnapshotLocked(ctx, target, Snapshot[T, E]{
		ThreadID: c.lease.ExecutionID, Revision: 0, ExecutionPointer: result.ExecutionPointer,
		State: result.State, Effects: result.Effects, RunMeta: meta,
	})
	if err == nil {
		result.RunMeta = meta
	}
	return err
}

func recoverableExecutionFailure(err error) bool {
	for _, recoverable := range []error{
		ErrActivityConflict, ErrActivityUnknown, ErrActivityBusy, ErrActivityRetryPending,
		ErrActivityFailed, ErrActivityAttemptsExhausted, ErrActivityScheduleInvalid, ErrLeaseLost, ErrConcurrencyConflict,
		ErrExecutionCapability, context.Canceled, context.DeadlineExceeded,
		ErrWaitInvalid, ErrWaitConflict, ErrWaitStale, ErrWaitUnresolved, ErrWaitRegistration,
	} {
		if errors.Is(err, recoverable) {
			return true
		}
	}
	return false
}
