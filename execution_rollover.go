package flowy

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
)

func rolloverRequestDigest(
	source HistoricalCheckpointReference,
	sourceDescriptor, targetDescriptor ExecutionDescriptor,
	targetID, decisionID string,
	policy RolloverPolicy,
	label string,
) string {
	encoded, _ := json.Marshal(
		[]any{"flowy-rollover-v1", source, sourceDescriptor, targetDescriptor, targetID, decisionID, policy, label},
	)
	digest := sha256.Sum256(encoded)
	return hex.EncodeToString(digest[:])
}

// Rollover explicitly publishes a fresh continuation and a transferred source.
// No nodes, activity dispatch, child dispatch, merge or implicit Resume runs.
// Identical replay addresses the original source token and skips projection.
func (r *DurableRunner[T, E]) Rollover(
	ctx context.Context,
	token ResumeToken,
	request RolloverRequest,
) (ResumeToken, error) {
	store, ok := r.store.(ExecutionRolloverStore)
	if !ok {
		return ResumeToken{}, ErrExecutionLifecycleUnsupported
	}
	if token.ThreadID == "" || token.SnapshotRevision == 0 || request.TargetID == "" ||
		request.TargetID == token.ThreadID ||
		request.DecisionID == "" ||
		request.ProjectionLabel == "" ||
		request.Project == nil ||
		!validRuntimeText(token.ThreadID, request.TargetID, request.DecisionID, request.ProjectionLabel) ||
		request.Policy.Validate() != nil ||
		request.SourceDescriptor.Validate() != nil {
		return ResumeToken{}, ErrExecutionRolloverConflict
	}
	session, err := r.acquireSession(ctx, token.ThreadID)
	if err != nil {
		return ResumeToken{}, err
	}
	defer session.finish()
	event := rolloverOperationObservation(token, request)
	defer func() { observeLifecycle(session.ctx, event) }()
	prior, err := store.LoadRollover(session.ctx, token.ThreadID)
	if err != nil {
		return ResumeToken{}, err
	}
	if prior != nil {
		result, replayErr := r.replayRollover(token, request, *prior)
		event = rolloverReplayObservation(event, *prior, replayErr)
		return result, replayErr
	}
	source, err := r.store.LoadExecution(session.ctx, token.ThreadID)
	if err != nil {
		return ResumeToken{}, err
	}
	if source.Revision != token.SnapshotRevision {
		return ResumeToken{}, ErrConcurrencyConflict
	}
	if err = ValidateExecutionIntegrity(source, token.ThreadID, token.SnapshotRevision); err != nil {
		return ResumeToken{}, err
	}
	if err = source.Descriptor.Check(request.SourceDescriptor); err != nil {
		return ResumeToken{}, err
	}
	if err = validateRolloverSource(source); err != nil {
		return ResumeToken{}, err
	}
	if err = validateRolloverRecordLimit(source, request.Policy); err != nil {
		return ResumeToken{}, err
	}
	session.ctx = restoreExecutionTelemetry(session.ctx, source)
	event.Node, event.SegmentID, event.Stage = source.Progress.ExecutionPointer, source.RunMeta.Segment.SegmentID, LifecycleStarted
	observeLifecycle(session.ctx, event)
	event.Stage = LifecycleFailed
	reference, target, err := r.prepareRolloverTarget(source, request)
	if err != nil {
		return ResumeToken{}, err
	}
	if err = session.ctx.Err(); err != nil {
		return ResumeToken{}, context.Cause(session.ctx)
	}
	receipt, err := store.CommitRollover(session.ctx, session.lease, reference, target)
	session.noteLeaseFailure(err)
	if err != nil {
		return ResumeToken{}, err
	}
	if err = receipt.Validate(); err != nil {
		return ResumeToken{}, err
	}
	event.Stage, event.Revision, event.TargetRevision = LifecycleCommitted, receipt.SourceRevision, receipt.Target.Revision
	return ResumeToken{ThreadID: receipt.Target.ExecutionID, SnapshotRevision: receipt.Target.Revision}, nil
}

func (r *DurableRunner[T, E]) replayRollover(
	token ResumeToken,
	request RolloverRequest,
	prior RolloverReceipt,
) (ResumeToken, error) {
	if prior.Validate() != nil {
		return ResumeToken{}, ErrExecutionCorrupt
	}
	digest := rolloverRequestDigest(
		prior.Lineage.Source,
		request.SourceDescriptor,
		r.descriptor,
		request.TargetID,
		request.DecisionID,
		request.Policy,
		request.ProjectionLabel,
	)
	if prior.Lineage.Source.Revision != token.SnapshotRevision || digest != prior.Lineage.RequestDigest {
		return ResumeToken{}, ErrExecutionRolloverConflict
	}
	return ResumeToken{ThreadID: prior.Target.ExecutionID, SnapshotRevision: prior.Target.Revision}, nil
}

func (r *DurableRunner[T, E]) rolloverTarget(
	source ExecutionEnvelope,
	request RolloverRequest,
	payload RolloverPayload,
	reference HistoricalCheckpointReference,
) ExecutionEnvelope {
	lineage := RolloverLineage{
		Source:           reference,
		SourceDescriptor: source.Descriptor,
		TargetDescriptor: r.descriptor,
		TargetID:         request.TargetID,
		DecisionID:       request.DecisionID,
		Policy:           request.Policy,
		ProjectionLabel:  request.ProjectionLabel,
		RequestDigest: rolloverRequestDigest(
			reference,
			source.Descriptor,
			r.descriptor,
			request.TargetID,
			request.DecisionID,
			request.Policy,
			request.ProjectionLabel,
		),
		CreatedAt: r.options.Clock.Now().UTC(),
	}
	target := ExecutionEnvelope{
		ExecutionID:     request.TargetID,
		Descriptor:      r.descriptor,
		Progress:        payload.Progress,
		EffectsPayload:  payload.EffectsPayload,
		RuntimeProfile:  r.options.WaitProfile,
		Activation:      1,
		RunMeta:         newRunMetadata(),
		Rollover:        &lineage,
		Revision:        0,
		Digest:          "",
		JournalPayload:  nil,
		ChildrenPayload: nil,
		WaitsPayload:    nil,
		Terminal:        nil,
		Migration:       nil,
		Import:          nil,
		Transfer:        nil,
		Fork:            nil,
	}
	// StepCount is segment-local; cumulative accounting maps remain preserved.
	target.RunMeta.StepCount = 0
	target.RunMeta.RetryCounts = source.RunMeta.RetryCounts
	target.RunMeta.BudgetCounts = source.RunMeta.BudgetCounts
	target.RunMeta.TelemetryContext = source.RunMeta.TelemetryContext
	return target
}

func (r *DurableRunner[T, E]) prepareRolloverTarget(
	source ExecutionEnvelope,
	request RolloverRequest,
) (HistoricalCheckpointReference, ExecutionEnvelope, error) {
	payload, err := request.Project(
		RolloverPayload{
			Progress:       cloneMigrationState(source.Progress),
			EffectsPayload: bytes.Clone(source.EffectsPayload),
		},
	)
	if err != nil {
		return HistoricalCheckpointReference{}, ExecutionEnvelope{}, errors.Join(ErrExecutionLifecycleUnsafe, err)
	}
	payload.Progress = cloneMigrationState(payload.Progress)
	payload.EffectsPayload = bytes.Clone(payload.EffectsPayload)
	if len(payload.Progress.ChildCursors) != 0 || len(payload.Progress.JournalReferences) != 0 ||
		len(payload.Progress.ChildGroupReferences) != 0 {
		return HistoricalCheckpointReference{}, ExecutionEnvelope{}, ErrExecutionLifecycleUnsafe
	}
	if err = r.validatePointer(payload.Progress.ExecutionPointer); err != nil {
		return HistoricalCheckpointReference{}, ExecutionEnvelope{}, err
	}
	reference := HistoricalCheckpointReference{
		ExecutionID: source.ExecutionID,
		Revision:    source.Revision,
		Digest:      source.Digest,
	}
	target := r.rolloverTarget(source, request, payload, reference)
	if err = r.validateTargetCodecs(target); err != nil {
		return HistoricalCheckpointReference{}, ExecutionEnvelope{}, errors.Join(ErrExecutionIncompatible, err)
	}
	return reference, target, nil
}
