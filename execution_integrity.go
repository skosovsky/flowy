package flowy

import "errors"

// ErrExecutionCorrupt rejects missing integrity metadata or inconsistent raw
// persistence. It never means the execution is absent or safe to recreate.
var ErrExecutionCorrupt = errors.New("flowy: corrupt execution envelope")

func validateExecutionSourceMetadata(envelope ExecutionEnvelope) error {
	if envelope.Rollover != nil &&
		(envelope.Rollover.Validate() != nil || envelope.Rollover.TargetID != envelope.ExecutionID) {
		return ErrExecutionCorrupt
	}
	if envelope.Transfer != nil &&
		(envelope.Transfer.Validate() != nil || envelope.Transfer.Lineage.Source.ExecutionID != envelope.ExecutionID || envelope.Revision != envelope.Transfer.SourceRevision || envelope.Terminal == nil || envelope.Terminal.Status != RunStatusTransferred) {
		return ErrExecutionCorrupt
	}
	if envelope.Terminal != nil && envelope.Terminal.Status == RunStatusTransferred && envelope.Transfer == nil {
		return ErrExecutionCorrupt
	}
	if envelope.Fork != nil && (envelope.Fork.Validate() != nil || envelope.Fork.TargetID != envelope.ExecutionID) {
		return ErrExecutionCorrupt
	}
	if envelope.Import != nil && (envelope.Import.ImporterID == "" || envelope.Import.Source.Validate() != nil) {
		return ErrExecutionImportInvalid
	}
	if errors.Is(terminalFailureError(envelope.Terminal), ErrInvalidSnapshot) {
		return ErrInvalidSnapshot
	}
	return nil
}

func validateExecutionCollections(envelope ExecutionEnvelope) error {
	_, err := validateExecutionCollectionsWithWaits(envelope)
	return err
}

func validateExecutionCollectionsWithWaits(envelope ExecutionEnvelope) (map[string]DurableWaitRecord, error) {
	if !validMigrationText(envelope.Progress) {
		return nil, ErrExecutionCorrupt
	}
	if envelope.RuntimeProfile != nil && envelope.RuntimeProfile.Validate() != nil {
		return nil, ErrExecutionCorrupt
	}
	if _, err := executionActivityJournal(envelope); err != nil {
		return nil, err
	}
	if _, err := executionChildGroups(envelope); err != nil {
		return nil, err
	}
	return executionWaits(envelope)
}

// SealExecutionEnvelope computes integrity after storage assigns its revision.
// Call inside the atomic fenced/OCC write; it does not establish compatibility.
func SealExecutionEnvelope(envelope ExecutionEnvelope) (ExecutionEnvelope, error) {
	if envelope.ExecutionID == "" || envelope.Revision == 0 || envelope.Progress.ExecutionPointer == "" ||
		!validRuntimeText(envelope.ExecutionID) || !validMigrationText(envelope.Progress) {
		return ExecutionEnvelope{}, ErrExecutionCorrupt
	}
	digest, err := EnvelopeDigest(envelope)
	if err != nil {
		return ExecutionEnvelope{}, err
	}
	envelope.Digest = digest
	return envelope, nil
}

// ValidateExecutionIntegrity checks a record against its exact storage address.
// Revision must be nonzero; zero is never a latest fallback.
func ValidateExecutionIntegrity(envelope ExecutionEnvelope, id string, revision uint64) error {
	if id == "" || revision == 0 || envelope.ExecutionID != id || envelope.Revision != revision ||
		!validRuntimeText(id) || !validMigrationText(envelope.Progress) ||
		envelope.Progress.ExecutionPointer == "" ||
		envelope.Digest == "" {
		return ErrExecutionCorrupt
	}
	digest, err := EnvelopeDigest(envelope)
	if err != nil {
		return err
	}
	if digest != envelope.Digest {
		return ErrExecutionCorrupt
	}
	return nil
}
