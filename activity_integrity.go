package flowy

import (
	"encoding/hex"
	"encoding/json"
	"errors"
)

func decodeActivityJournal(payload []byte) (map[string]ActivityRecord, error) {
	journal := make(map[string]ActivityRecord)
	if len(payload) == 0 {
		return journal, nil
	}
	if err := json.Unmarshal(payload, &journal); err != nil {
		return nil, errors.Join(ErrExecutionCorrupt, err)
	}
	if journal == nil {
		return nil, ErrExecutionCorrupt
	}
	for identity, record := range journal {
		if identity != record.Identity || validateActivityRecord(record) != nil {
			return nil, ErrExecutionCorrupt
		}
	}
	return journal, nil
}

func validActivityDigest(value string) bool {
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded) == 32 && hex.EncodeToString(decoded) == value
}

func validateActivityRecord(record ActivityRecord) error {
	if !validActivityAddress(record) || !validActivityDigest(record.InputDigest) || record.Key == "" ||
		record.Implementation == "" || !validRuntimeText(record.Implementation) || validateActivityRetry(record.Retry) != nil {
		return ErrExecutionCorrupt
	}
	for index, attempt := range record.Attempts {
		if attempt.Number != index+1 || attempt.Incarnation == 0 || attempt.StartedAt.IsZero() ||
			validateActivityAttempt(attempt) != nil ||
			(attempt.State == ActivityRunning && index != len(record.Attempts)-1) {
			return ErrExecutionCorrupt
		}
	}
	limit := record.Retry.MaxAttempts
	if limit == 0 {
		limit = 1
	}
	if len(record.Attempts) > limit {
		return ErrExecutionCorrupt
	}
	if decisionErr := validateActivityDecisions(record); decisionErr != nil {
		return decisionErr
	}
	if historyErr := validateActivityAttemptHistory(record); historyErr != nil {
		return historyErr
	}
	return validateActivityRecordState(record)
}

func executionActivityJournal(envelope ExecutionEnvelope) (map[string]ActivityRecord, error) {
	journal, err := decodeActivityJournal(envelope.JournalPayload)
	if err != nil {
		return nil, err
	}
	for _, record := range journal {
		if record.Origin == ActivitySimulated && (envelope.Fork == nil || envelope.Fork.Mode != ForkFake) {
			return nil, ErrExecutionCorrupt
		}
		if record.Origin == ActivityLive && envelope.Fork != nil && envelope.Fork.Mode == ForkFake {
			return nil, ErrExecutionCorrupt
		}
		if record.ExecutionID != envelope.ExecutionID || record.Activation > envelope.Activation {
			return nil, ErrExecutionCorrupt
		}
		for _, decision := range record.Resolutions {
			if decision.SourceRevision >= envelope.Revision {
				return nil, ErrExecutionCorrupt
			}
		}
	}
	if referenceErr := validateActivityReferences(envelope, journal); referenceErr != nil {
		return nil, referenceErr
	}
	return journal, nil
}

func validActivityAddress(record ActivityRecord) bool {
	return record.ExecutionID != "" && record.Node != "" && record.Activation != 0 &&
		validRuntimeText(record.ExecutionID, string(record.Node), record.Key) &&
		record.Identity == activityAddressIdentity(record.ExecutionID, record.Node, record.Activation, record.Key)
}

func validateActivityAttempt(attempt ActivityAttempt) error {
	switch attempt.State {
	case ActivityPrepared:
		return ErrExecutionCorrupt
	case ActivityRunning:
		if attempt.Classification == "" && attempt.FinishedAt.IsZero() && attempt.Error == "" {
			return nil
		}
	case ActivityCompleted:
		if attempt.Classification == "" && !attempt.FinishedAt.IsZero() && attempt.Error == "" {
			return nil
		}
	case ActivityFailed:
		if (attempt.Classification == ActivityRetryable || attempt.Classification == ActivityNonRetryable) &&
			!attempt.FinishedAt.IsZero() {
			return nil
		}
	case ActivityUnknown:
		if attempt.Classification == ActivityAmbiguous {
			return nil
		}
	}
	return ErrExecutionCorrupt
}

func validateActivityRecordState(record ActivityRecord) error {
	if record.State == ActivityPrepared && len(record.Attempts) == 0 {
		if record.Classification == "" && record.NextAttemptAt.IsZero() && record.Origin == "" &&
			len(record.Outcome) == 0 {
			return nil
		}
		return ErrExecutionCorrupt
	}
	if len(record.Attempts) == 0 {
		return ErrExecutionCorrupt
	}
	last := record.Attempts[len(record.Attempts)-1]
	valid := false
	switch record.State {
	case ActivityPrepared:
		valid = validPreparedActivity(record, last)
	case ActivityRunning:
		valid = last.State == ActivityRunning && record.Classification == "" && record.NextAttemptAt.IsZero() &&
			len(record.Outcome) == 0
	case ActivityUnknown:
		valid = last.State == ActivityUnknown && record.Classification == ActivityAmbiguous &&
			record.NextAttemptAt.IsZero() &&
			len(record.Outcome) == 0
	case ActivityCompleted:
		valid = validCompletedActivity(record, last)
	case ActivityFailed:
		valid = validFailedActivity(record, last)
	}
	if valid {
		return nil
	}
	return ErrExecutionCorrupt
}

func validPreparedActivity(record ActivityRecord, last ActivityAttempt) bool {
	return record.Classification == ActivityRetryable && !record.NextAttemptAt.IsZero() &&
		record.Retry.SafeRetryContract != "" && len(record.Attempts) < record.Retry.MaxAttempts &&
		(last.State == ActivityFailed || (record.Origin == ActivityManual && last.State == ActivityUnknown)) && len(record.Outcome) == 0
}

func validCompletedActivity(record ActivityRecord, last ActivityAttempt) bool {
	return record.NextAttemptAt.IsZero() &&
		(((record.Origin == ActivityLive || record.Origin == ActivitySimulated) && last.State == ActivityCompleted) ||
			((record.Origin == ActivityReconciled || record.Origin == ActivityManual) && last.State == ActivityUnknown))
}

func validFailedActivity(record ActivityRecord, last ActivityAttempt) bool {
	return record.NextAttemptAt.IsZero() && len(record.Outcome) == 0 &&
		(record.Classification == ActivityNonRetryable || record.Classification == ActivityRetryable) &&
		(last.State == ActivityFailed || (record.Origin == ActivityManual && last.State == ActivityUnknown))
}
