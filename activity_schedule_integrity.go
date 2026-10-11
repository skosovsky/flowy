package flowy

func validateActivityRecordSchedules(record ActivityRecord) error {
	for index, attempt := range record.Attempts {
		choice := attempt.RetrySchedule
		if choice == nil {
			if attempt.State == ActivityFailed && attempt.Classification == ActivityRetryable &&
				attempt.Number < record.Retry.MaxAttempts {
				return ErrExecutionCorrupt
			}
			continue
		}
		if attempt.State != ActivityFailed || attempt.Classification != ActivityRetryable ||
			choice.Attempt != attempt.Number ||
			!choice.PreparedAt.Equal(attempt.FinishedAt) ||
			!validActivityRetryScheduleDecision(record.Retry, choice) {
			return ErrExecutionCorrupt
		}
		if index+1 < len(record.Attempts) &&
			(choice.Rejection != "" || record.Attempts[index+1].StartedAt.Before(choice.Deadline)) {
			return ErrExecutionCorrupt
		}
	}
	return nil
}

func validPreparedActivitySchedule(record ActivityRecord, last ActivityAttempt) bool {
	choice := last.RetrySchedule
	if record.Origin == ActivityManual {
		if len(record.Resolutions) == 0 {
			return false
		}
		choice = record.Resolutions[len(record.Resolutions)-1].RetrySchedule
	}
	return choice != nil && choice.Rejection == "" && choice.Deadline.Equal(record.NextAttemptAt)
}
