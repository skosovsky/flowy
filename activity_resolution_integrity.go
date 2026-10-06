package flowy

func validateActivityDecisions(record ActivityRecord) error {
	seen := make(map[string]bool)
	previousAttempt, previousRevision := 0, uint64(0)
	for _, decision := range record.Resolutions {
		if seen[decision.DecisionID] || decision.DecisionID == "" || decision.Reason == "" || decision.Evidence == "" ||
			decision.At.IsZero() || decision.Incarnation == 0 || decision.SourceRevision <= previousRevision ||
			decision.Attempt <= previousAttempt || decision.Attempt > len(record.Attempts) ||
			(decision.PriorState != ActivityUnknown && decision.PriorState != ActivityRunning) {
			return ErrExecutionCorrupt
		}
		if record.Attempts[decision.Attempt-1].State != ActivityUnknown ||
			!validActivityDecisionAction(decision, record.Retry) {
			return ErrExecutionCorrupt
		}
		seen[decision.DecisionID] = true
		if decision.Action != ActivityResolveRetry && decision.Attempt != len(record.Attempts) {
			return ErrExecutionCorrupt
		}
		previousAttempt, previousRevision = decision.Attempt, decision.SourceRevision
	}
	if record.Origin == ActivityManual && !validManualActivityOrigin(record) {
		return ErrExecutionCorrupt
	}
	return nil
}

func validManualActivityOrigin(record ActivityRecord) bool {
	if len(record.Resolutions) == 0 {
		return false
	}
	last := record.Resolutions[len(record.Resolutions)-1]
	switch record.State {
	case ActivityCompleted:
		return last.Action == ActivityResolveComplete && last.Attempt == len(record.Attempts)
	case ActivityFailed:
		return last.Action == ActivityResolveFail && last.Attempt == len(record.Attempts)
	case ActivityPrepared:
		return last.Action == ActivityResolveRetry && last.Attempt == len(record.Attempts)
	case ActivityRunning, ActivityUnknown:
		return last.Action == ActivityResolveRetry && last.Attempt < len(record.Attempts)
	}
	return false
}

func validActivityDecisionAction(decision ActivityResolutionRecord, policy ActivityRetryPolicy) bool {
	switch decision.Action {
	case ActivityResolveComplete, ActivityResolveFail:
		return decision.SafeRetryContract == ""
	case ActivityResolveRetry:
		return decision.SafeRetryContract != "" && decision.SafeRetryContract == policy.SafeRetryContract &&
			decision.Attempt < policy.MaxAttempts
	}
	return false
}
