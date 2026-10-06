package flowy

import (
	"errors"
	"fmt"
	"time"
)

// ActivityFailureClass preserves the host's classification of a returned error.
// Unknown includes ambiguous remote delivery; timeout alone cannot rule it out.
type ActivityFailureClass string

const (
	ActivityRetryable    ActivityFailureClass = "retryable"
	ActivityNonRetryable ActivityFailureClass = "non_retryable"
	ActivityAmbiguous    ActivityFailureClass = "unknown"
)

// ActivityRetryPolicy is persisted and compared on recovery. MaxAttempts counts
// dispatches, including the initial dispatch. SafeRetryContract is a host label
// attesting downstream idempotency or replay safety, not a runtime guarantee.
// An all-zero policy never retries; adapter-internal retries must be disabled.
// Active policies require an explicit Schedule; legacy Delay-only data is rejected.
type ActivityRetryPolicy struct {
	Label             string                `json:"label"`
	MaxAttempts       int                   `json:"max_attempts"`
	Schedule          ActivityRetrySchedule `json:"schedule"`
	SafeRetryContract string                `json:"safe_retry_contract"`
}

var (
	ErrActivityRetryPending      = errors.New("flowy: activity retry pending")
	ErrActivityAttemptsExhausted = errors.New("flowy: activity attempts exhausted")
	ErrActivityFailed            = errors.New("flowy: activity failed without retry")
)

// ActivityRetryPendingError reports the committed retry deadline and revision.
// It never sleeps or holds a worker until the deadline.
type ActivityRetryPendingError struct {
	Identity string
	Deadline time.Time
	Revision uint64
}

func (e *ActivityRetryPendingError) Error() string {
	return fmt.Sprintf("%s: %s at %s", ErrActivityRetryPending, e.Identity, e.Deadline.UTC().Format(time.RFC3339Nano))
}

func (*ActivityRetryPendingError) Unwrap() error { return ErrActivityRetryPending }

func activityRecordError(record ActivityRecord, revision uint64) error {
	switch record.State {
	case ActivityPrepared:
		return &ActivityRetryPendingError{Identity: record.Identity, Deadline: record.NextAttemptAt, Revision: revision}
	case ActivityFailed:
		if len(record.Attempts) > 0 && record.Attempts[len(record.Attempts)-1].RetrySchedule != nil &&
			record.Attempts[len(record.Attempts)-1].RetrySchedule.Rejection != "" {
			return ErrActivityScheduleInvalid
		}
		if record.Classification == ActivityRetryable {
			return ErrActivityAttemptsExhausted
		}
		return ErrActivityFailed
	default:
		return ErrActivityUnknown
	}
}

func preparedActivityError(record ActivityRecord, revision uint64, now time.Time) error {
	if record.State == ActivityFailed {
		return activityRecordError(record, revision)
	}
	if record.State != ActivityPrepared {
		return ErrActivityConflict
	}
	if len(record.Attempts) == 0 {
		return nil
	}
	if len(record.Attempts) >= record.Retry.MaxAttempts || record.Retry.SafeRetryContract == "" ||
		record.NextAttemptAt.IsZero() {
		return ErrActivityConflict
	}
	if now.Before(record.NextAttemptAt) {
		return activityRecordError(record, revision)
	}
	return nil
}

func classifyActivityFailure(request ActivityRequest, dispatchErr error) (ActivityFailureDecision, bool) {
	if request.Classify == nil {
		return ActivityFailureDecision{Class: ActivityAmbiguous, NotBefore: time.Time{}}, false
	}
	decision := request.Classify(dispatchErr)
	switch decision.Class {
	case ActivityRetryable, ActivityNonRetryable, ActivityAmbiguous:
		return decision, false
	default:
		return ActivityFailureDecision{Class: ActivityAmbiguous, NotBefore: time.Time{}}, true
	}
}

func validateActivityRetry(policy ActivityRetryPolicy) error {
	var noRetry ActivityRetryPolicy
	if policy == noRetry {
		return nil
	}
	if policy.Label == "" || !validRuntimeText(policy.Label, policy.SafeRetryContract) || policy.MaxAttempts < 1 ||
		(policy.MaxAttempts > 1 && policy.SafeRetryContract == "") {
		return ErrExecutionCapability
	}
	if err := validateActivitySchedule(policy.Schedule); err != nil {
		return errors.Join(ErrExecutionCapability, err)
	}
	return nil
}

func (c *executionCheckpointer[T, E]) recordActivityFailure(
	record *ActivityRecord,
	dispatchErr error,
	decision ActivityFailureDecision,
) {
	last := &record.Attempts[len(record.Attempts)-1]
	record.Classification, last.Classification = decision.Class, decision.Class
	last.Error, last.FinishedAt = dispatchErr.Error(), c.clock.Now().UTC()
	record.NextAttemptAt = time.Time{}
	switch decision.Class {
	case ActivityRetryable:
		last.State, record.State = ActivityFailed, ActivityFailed
		if len(record.Attempts) < record.Retry.MaxAttempts {
			chosen := chooseActivityRetrySchedule(
				record.Retry,
				len(record.Attempts),
				last.FinishedAt,
				decision.NotBefore,
				c.retryRandom,
			)
			last.RetrySchedule = &chosen
			if chosen.Rejection == "" {
				record.State = ActivityPrepared
				record.NextAttemptAt = chosen.Deadline
			}
		}
	case ActivityNonRetryable:
		last.State, record.State = ActivityFailed, ActivityFailed
	default:
		last.State, record.State = ActivityUnknown, ActivityUnknown
	}
}
