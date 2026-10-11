package flowy

import "context"

// InspectExecutionResume returns a core-issued token for a validated latest head.
// It is read-only and never acquires a lease, decodes host state, dispatches,
// reconciles or authorizes retries. A concurrent commit can make its token stale;
// Resume retains responsibility for compatibility, latest revision and fencing.
func InspectExecutionResume(ctx context.Context, store ExecutionStore, executionID string) (ResumeToken, error) {
	if executionID == "" || !validRuntimeText(executionID) {
		return ResumeToken{}, ErrInvalidResumeToken
	}
	envelope, err := store.LoadExecution(ctx, executionID)
	if err != nil {
		return ResumeToken{}, err
	}
	if err = ValidateExecutionIntegrity(envelope, executionID, envelope.Revision); err != nil {
		return ResumeToken{}, err
	}
	if err = envelope.Descriptor.Validate(); err != nil {
		return ResumeToken{}, err
	}
	if err = validateExecutionCollections(envelope); err != nil {
		return ResumeToken{}, err
	}
	if err = validateExecutionSourceMetadata(envelope); err != nil {
		return ResumeToken{}, err
	}
	return ResumeToken{ThreadID: envelope.ExecutionID, SnapshotRevision: envelope.Revision}, nil
}
