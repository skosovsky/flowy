package flowy

func validateActivityAttemptHistory(record ActivityRecord) error {
	for index := 0; index+1 < len(record.Attempts); index++ {
		attempt := record.Attempts[index]
		if record.Retry.SafeRetryContract == "" {
			return ErrExecutionCorrupt
		}
		switch attempt.State {
		case ActivityFailed:
			if attempt.Classification != ActivityRetryable {
				return ErrExecutionCorrupt
			}
		case ActivityUnknown:
			if !hasActivityRetryDecision(record, attempt.Number) {
				return ErrExecutionCorrupt
			}
		case ActivityPrepared, ActivityRunning, ActivityCompleted:
			return ErrExecutionCorrupt
		default:
			return ErrExecutionCorrupt
		}
	}
	return nil
}

func hasActivityRetryDecision(record ActivityRecord, attempt int) bool {
	for _, decision := range record.Resolutions {
		if decision.Attempt == attempt && decision.Action == ActivityResolveRetry {
			return true
		}
	}
	return false
}
