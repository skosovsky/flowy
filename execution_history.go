package flowy

import (
	"context"
	"errors"
)

var (
	ErrExecutionHistoryUnsupported    = errors.New("flowy: execution history unsupported")
	ErrExecutionCheckpointUnavailable = errors.New("flowy: exact execution checkpoint absent or pruned")
	ErrForkInvalid                    = errors.New("flowy: invalid execution fork contract")
	ErrForkSourceDigest               = errors.New("flowy: fork source digest mismatch")
)

// HistoricalCheckpointReference addresses exactly one immutable raw checkpoint.
// Digest is the expected envelope seal, not a domain-state-only hash.
type HistoricalCheckpointReference struct {
	ExecutionID string `json:"execution_id"`
	Revision    uint64 `json:"revision"`
	Digest      string `json:"digest"`
}

func (s HistoricalCheckpointReference) Validate() error {
	if s.ExecutionID == "" || !validRuntimeText(s.ExecutionID) || s.Revision == 0 {
		return ErrForkInvalid
	}
	if !validActivityDigest(s.Digest) {
		return ErrForkSourceDigest
	}
	return nil
}

// InspectExecutionCheckpoint does not acquire a lease, decode host state,
// migrate, dispatch or fall back to latest. The result is detached raw metadata;
// inspection is not permission to execute or to trust embedded host approvals.
func InspectExecutionCheckpoint(ctx context.Context, store ExecutionStore,
	source HistoricalCheckpointReference,
) (ExecutionEnvelope, error) {
	if err := source.Validate(); err != nil {
		return ExecutionEnvelope{}, err
	}
	history, ok := store.(ExecutionHistoryStore)
	if !ok {
		return ExecutionEnvelope{}, ErrExecutionHistoryUnsupported
	}
	envelope, err := history.LoadCheckpoint(ctx, source.ExecutionID, source.Revision)
	if errors.Is(err, ErrThreadNotFound) {
		return ExecutionEnvelope{}, errors.Join(ErrExecutionCheckpointUnavailable, err)
	}
	if err != nil {
		return ExecutionEnvelope{}, err
	}
	if err = ValidateExecutionIntegrity(envelope, source.ExecutionID, source.Revision); err != nil {
		return ExecutionEnvelope{}, err
	}
	if envelope.Digest != source.Digest {
		return ExecutionEnvelope{}, ErrForkSourceDigest
	}
	if err = envelope.Descriptor.Validate(); err != nil {
		return ExecutionEnvelope{}, err
	}
	if err = validateExecutionCollections(envelope); err != nil {
		return ExecutionEnvelope{}, err
	}
	if err = validateExecutionSourceMetadata(envelope); err != nil {
		return ExecutionEnvelope{}, err
	}
	return cloneExecutionEnvelope(envelope), nil
}
