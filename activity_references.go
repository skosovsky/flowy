package flowy

// boundActivityIdentity preserves a migrated activity's original address.
// References are scoped to the current cursor/activation and cleared on advance.
func boundActivityIdentity(envelope ExecutionEnvelope, key string) string {
	if identity, ok := envelope.Progress.JournalReferences[key]; ok {
		return identity
	}
	return activityIdentity(envelope, key)
}

func activityUnresolved(state ActivityState) bool {
	return state == ActivityPrepared || state == ActivityRunning || state == ActivityUnknown
}

func validateActivityReferences(envelope ExecutionEnvelope, journal map[string]ActivityRecord) error {
	for key, identity := range envelope.Progress.JournalReferences {
		record, exists := journal[identity]
		if !exists || key != record.Key || record.Activation != envelope.Activation {
			return ErrExecutionCorrupt
		}
	}
	for _, record := range journal {
		if !activityUnresolved(record.State) {
			continue
		}
		if record.Activation != envelope.Activation || boundActivityIdentity(envelope, record.Key) != record.Identity {
			return ErrExecutionCorrupt
		}
	}
	return nil
}
