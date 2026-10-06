package flowy

import (
	"slices"
	"time"
)

// PendingActivityRetry is sealed metadata for host-owned wakeup routing. It is
// not an accepted retry or permission to bypass the persisted retry policy.
type PendingActivityRetry struct {
	Identity    string
	Node        ExecutionPointer
	Activation  uint64
	ResumeToken ResumeToken
	Descriptor  ExecutionDescriptor
	Policy      ActivityRetryPolicy
	Attempts    int
	Deadline    time.Time
}

// InspectPendingActivityRetries reads without codecs/nodes/dispatch. Callers
// must route the exact token to the declared recovery owner; Resume rechecks
// revision, lease, deadline and limits before any allowed attempt.
func InspectPendingActivityRetries(envelope ExecutionEnvelope) ([]PendingActivityRetry, error) {
	if err := ValidateExecutionIntegrity(envelope, envelope.ExecutionID, envelope.Revision); err != nil {
		return nil, err
	}
	if err := envelope.Descriptor.Validate(); err != nil {
		return nil, err
	}
	if err := validateExecutionCollections(envelope); err != nil {
		return nil, err
	}
	if err := validateExecutionSourceMetadata(envelope); err != nil {
		return nil, err
	}
	result := make([]PendingActivityRetry, 0)
	if envelope.Terminal != nil {
		return result, nil
	}
	journal, err := executionActivityJournal(envelope)
	if err != nil {
		return nil, err
	}
	for identity, record := range journal {
		if record.Activation != envelope.Activation || record.State != ActivityPrepared ||
			len(record.Attempts) == 0 || record.NextAttemptAt.IsZero() ||
			len(record.Attempts) >= record.Retry.MaxAttempts || record.Retry.SafeRetryContract == "" {
			continue
		}
		result = append(result, PendingActivityRetry{Identity: identity, Node: envelope.Progress.ExecutionPointer,
			Activation: envelope.Activation, ResumeToken: ResumeToken{
				ThreadID: envelope.ExecutionID, SnapshotRevision: envelope.Revision}, Descriptor: envelope.Descriptor,
			Policy: record.Retry, Attempts: len(record.Attempts), Deadline: record.NextAttemptAt})
	}
	slices.SortFunc(result, func(a, b PendingActivityRetry) int { return compareChildIDs(a.Identity, b.Identity) })
	return result, nil
}
