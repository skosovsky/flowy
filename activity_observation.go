package flowy

import (
	"bytes"
	"context"
	"errors"
)

// activityObservationLocked copies only runtime addresses while the caller owns
// the mutex. The returned value may be observed only after releasing the mutex.
func (c *executionCheckpointer[T, E]) activityObservationLocked(
	operation LifecycleOperation,
	stage LifecycleStage,
	identity string,
) LifecycleObservation {
	event := lifecycleObservation(operation, stage, c.envelope.ExecutionID, c.envelope.Progress.ExecutionPointer)
	event.ActivityID = identity
	event.SegmentID = c.envelope.RunMeta.Segment.SegmentID
	event.SourceRevision = c.envelope.Revision
	return event
}

func (c *executionCheckpointer[T, E]) finishActivity(
	ctx context.Context,
	identity string,
	payload []byte,
	dispatchErr error,
	origin ActivityOrigin,
	classification ActivityFailureDecision,
) (ActivityOutcome, error) {
	operation := LifecycleActivity
	if origin == ActivityReconciled {
		operation = LifecycleReconcile
	}
	c.mu.Lock()
	event := c.activityObservationLocked(operation, LifecycleFailed, identity)
	outcome, err := c.finishActivityLocked(ctx, identity, payload, dispatchErr, origin, classification)
	var outcomeCode string
	if journal, readErr := c.readJournal(); readErr == nil {
		event.Attempt = len(journal[identity].Attempts)
		outcomeCode = activityObservationCode(journal[identity].State)
	}
	if c.envelope.Revision > event.SourceRevision {
		event.Stage, event.Revision = LifecycleCommitted, c.envelope.Revision
		event.Code = outcomeCode
	} else if outcome.Origin == ActivityReplayed {
		event.Stage, event.Revision = LifecycleReplayed, outcome.Revision
	}
	c.mu.Unlock()
	observeLifecycle(ctx, event)
	observeActivityRetry(ctx, event, err)
	return outcome, err
}

func observeActivityRetry(ctx context.Context, event LifecycleObservation, err error) {
	// Only the acknowledged retry publication has a revision; a pending error
	// from an uncommitted outcome must not manufacture scheduling success.
	if event.Stage != LifecycleCommitted {
		return
	}
	if pending, ok := errors.AsType[*ActivityRetryPendingError](err); ok {
		event.Operation, event.Code = LifecycleRetry, "retry_pending"
		event.Revision = pending.Revision
		observeLifecycle(ctx, event)
	}
}

func activityObservationCode(state ActivityState) string {
	switch state {
	case ActivityCompleted:
		return lifecycleOutcomeCompleted
	case ActivityFailed:
		return lifecycleOutcomeFailed
	case ActivityUnknown:
		return "outcome_unknown"
	case ActivityPrepared:
		return "retry_pending"
	case ActivityRunning:
		return "outcome_running"
	default:
		return "invalid_state"
	}
}

// replayActivityLocked consumes the mutex held by callActivity and observes the
// cached result after unlocking. No new attempt or commit is manufactured.
func (c *executionCheckpointer[T, E]) replayActivityLocked(
	ctx context.Context,
	record ActivityRecord,
) ActivityOutcome {
	result := ActivityOutcome{
		Payload: bytes.Clone(record.Outcome), Origin: ActivityReplayed,
		Revision: c.envelope.Revision, Identity: record.Identity,
	}
	event := c.activityObservationLocked(LifecycleActivity, LifecycleReplayed, record.Identity)
	event.Revision, event.Attempt = result.Revision, len(record.Attempts)
	c.mu.Unlock()
	observeLifecycle(ctx, event)
	return result
}
