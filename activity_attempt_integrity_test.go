package flowy

import (
	"errors"
	"testing"
	"time"
)

func TestActivityAttemptHistoryRequiresSafeTransition(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name          string
		state         ActivityState
		class         ActivityFailureClass
		manual, valid bool
	}{
		{name: "completed then dispatch", state: ActivityCompleted},
		{name: "nonretryable then dispatch", state: ActivityFailed, class: ActivityNonRetryable},
		{name: "unknown then blind dispatch", state: ActivityUnknown, class: ActivityAmbiguous},
		{name: "safe classified retry", state: ActivityFailed, class: ActivityRetryable, valid: true},
		{name: "addressed unknown retry", state: ActivityUnknown, class: ActivityAmbiguous, manual: true, valid: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			// Arrange.
			record := validRunningActivityForTest()
			second := record.Attempts[0]
			second.Number, second.Incarnation = 2, 3
			record.Attempts[0].State, record.Attempts[0].Classification = test.state, test.class
			record.Attempts[0].FinishedAt = time.Now().UTC()
			record.Attempts = append(record.Attempts, second)
			record.Retry = ActivityRetryPolicy{
				Schedule:          ActivityRetrySchedule{Kind: ActivityRetryFixed},
				Label:             "safe",
				MaxAttempts:       2,
				SafeRetryContract: "idempotent",
			}
			if test.state == ActivityFailed && test.class == ActivityRetryable {
				choice := chooseActivityRetrySchedule(record.Retry, 1, record.Attempts[0].FinishedAt, time.Time{}, nil)
				record.Attempts[0].RetrySchedule = &choice
				record.Attempts[1].StartedAt = choice.Deadline
			}
			if test.manual {
				record.Resolutions = []ActivityResolutionRecord{
					{
						DecisionID:        "operator-retry",
						Attempt:           1,
						Action:            ActivityResolveRetry,
						PriorState:        ActivityUnknown,
						SourceRevision:    1,
						Incarnation:       2,
						Reason:            "safe",
						Evidence:          "receipt",
						At:                time.Now().UTC(),
						SafeRetryContract: "idempotent",
					},
				}
				choice := chooseActivityRetrySchedule(record.Retry, 1, record.Resolutions[0].At, time.Time{}, nil)
				record.Resolutions[0].RetrySchedule = &choice
				record.Attempts[1].StartedAt = choice.Deadline
			}
			// Act.
			err := validateActivityRecord(record)
			// Assert.
			if test.valid {
				if err != nil {
					t.Fatalf("safe history rejected: %v", err)
				}
			} else if !errors.Is(err, ErrExecutionCorrupt) {
				t.Fatalf("unsafe history accepted: %v", err)
			}
		})
	}
}
