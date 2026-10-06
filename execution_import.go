package flowy

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
)

// ErrExecutionImportInvalid rejects an unaddressed, corrupt or invalid import.
var ErrExecutionImportInvalid = errors.New("flowy: invalid execution import")

// LegacyExecutionSource is an immutable artifact captured explicitly by the
// host. Digest is SHA-256 of Payload; Format selects the host importer, never a
// runtime JSON heuristic. Host owns source capture, retention and permissions.
type LegacyExecutionSource struct {
	ID       string `json:"id"`
	Revision uint64 `json:"revision"`
	Format   string `json:"format"`
	Digest   string `json:"digest"`
	Payload  []byte `json:"payload"`
}

// Validate checks the explicit source address, format and payload integrity.
func (s LegacyExecutionSource) Validate() error {
	if s.ID == "" || s.Revision == 0 || s.Format == "" || len(s.Payload) == 0 || !validRuntimeText(s.ID, s.Format) {
		return ErrExecutionImportInvalid
	}
	sum := sha256.Sum256(s.Payload)
	if s.Digest != hex.EncodeToString(sum[:]) {
		return ErrExecutionImportInvalid
	}
	return nil
}

// ImportedExecutionState contains target data, not a reconstructed journal.
// Old external effects must not be reclassified as completed activities.
type ImportedExecutionState struct {
	Progress       MigrationState
	EffectsPayload []byte
	RunMeta        RunMetadata
}

// ExecutionImporter is a host-supplied pure transformation for one explicit
// source format. It receives detached bytes and must not perform external I/O.
type ExecutionImporter struct {
	ID        string
	Format    string
	Transform func([]byte) (ImportedExecutionState, error)
}

// ImportProvenance retains the original source artifact and importer identity.
type ImportProvenance struct {
	Source     LegacyExecutionSource `json:"source"`
	ImporterID string                `json:"importer_id"`
}

// Import creates a new target execution without executing nodes or activities.
// It never overwrites an existing execution or guesses an old record's format.
func (r *DurableRunner[T, E]) Import(
	ctx context.Context,
	id string,
	source LegacyExecutionSource,
	importer ExecutionImporter,
) (ResumeToken, error) {
	if importer.ID == "" || importer.Format != source.Format || importer.Transform == nil ||
		!validRuntimeText(importer.ID) {
		return ResumeToken{}, ErrExecutionImportInvalid
	}
	source.Payload = bytes.Clone(source.Payload)
	if sourceErr := source.Validate(); sourceErr != nil {
		return ResumeToken{}, sourceErr
	}
	if profileErr := r.checkRuntimeProfile(r.options.WaitProfile); profileErr != nil {
		return ResumeToken{}, profileErr
	}
	session, err := r.acquireSession(ctx, id)
	if err != nil {
		return ResumeToken{}, err
	}
	defer session.finish()
	ctx = session.ctx
	if _, loadErr := r.store.LoadExecution(ctx, id); !errors.Is(loadErr, ErrThreadNotFound) {
		if loadErr != nil {
			return ResumeToken{}, loadErr
		}
		return ResumeToken{}, ErrConcurrencyConflict
	}
	if ctx.Err() != nil {
		return ResumeToken{}, context.Cause(ctx)
	}
	event := lifecycleObservation(LifecycleImport, LifecycleStarted, id, "")
	event.SourceExecutionID, event.SourceRevision, event.TargetExecutionID = source.ID, source.Revision, id
	observeLifecycle(ctx, event)
	event.Stage = LifecycleFailed
	defer func() { observeLifecycle(ctx, event) }()
	state, err := importer.Transform(bytes.Clone(source.Payload))
	if err != nil {
		return ResumeToken{}, fmt.Errorf("%w: %w", ErrExecutionImportInvalid, err)
	}
	if ctx.Err() != nil {
		return ResumeToken{}, context.Cause(ctx)
	}
	if pointerErr := r.validatePointer(state.Progress.ExecutionPointer); pointerErr != nil {
		return ResumeToken{}, fmt.Errorf("%w: %w", ErrExecutionImportInvalid, pointerErr)
	}
	if _, decodeErr := r.stateCodec.Unmarshal(bytes.Clone(state.Progress.StatePayload)); decodeErr != nil {
		return ResumeToken{}, fmt.Errorf("%w: target state: %w", ErrExecutionImportInvalid, decodeErr)
	}
	if _, decodeErr := r.effectsCodec.Unmarshal(bytes.Clone(state.EffectsPayload)); decodeErr != nil {
		return ResumeToken{}, fmt.Errorf("%w: target effects: %w", ErrExecutionImportInvalid, decodeErr)
	}
	if ctx.Err() != nil {
		return ResumeToken{}, context.Cause(ctx)
	}
	envelope := cloneExecutionEnvelope(ExecutionEnvelope{
		Revision:        0,
		Digest:          "",
		JournalPayload:  nil,
		ChildrenPayload: nil,
		WaitsPayload:    nil,
		Terminal:        nil,
		Migration:       nil,
		ExecutionID:     id,
		Descriptor:      r.descriptor,
		RuntimeProfile:  r.options.WaitProfile,
		Progress:        state.Progress,
		EffectsPayload:  state.EffectsPayload,
		RunMeta:         state.RunMeta,
		Activation:      1,
		Import:          &ImportProvenance{Source: source, ImporterID: importer.ID},
		Fork:            nil, Rollover: nil, Transfer: nil,
	})
	if collectionsErr := validateExecutionCollections(envelope); collectionsErr != nil {
		return ResumeToken{}, errors.Join(ErrExecutionImportInvalid, collectionsErr)
	}
	if metadataErr := validateExecutionSourceMetadata(envelope); metadataErr != nil {
		return ResumeToken{}, errors.Join(ErrExecutionImportInvalid, metadataErr)
	}
	committed, err := commitExecution(ctx, r.store, 0, session.lease, envelope)
	if err != nil {
		return ResumeToken{}, err
	}
	event.Stage, event.Revision, event.TargetRevision = LifecycleCommitted, committed.Revision, committed.Revision
	return ResumeToken{ThreadID: id, SnapshotRevision: committed.Revision}, nil
}
