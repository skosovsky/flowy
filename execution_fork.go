package flowy

import (
	"bytes"
	"context"
	"errors"
)

// Fork creates only a fresh saved boundary. Resume requires the recorded policy;
// source history and latest are never changed or implicitly migrated.
func (r *DurableRunner[T, E]) Fork(ctx context.Context, request ForkRequest) (ResumeToken, error) {
	// Freeze caller-owned registration before any host callback can mutate it.
	if request.Projection != nil {
		projection := *request.Projection
		request.Projection = &projection
	}
	source, err := InspectExecutionCheckpoint(ctx, r.store, request.Source)
	if err != nil {
		return ResumeToken{}, err
	}
	lineage, err := r.prepareForkLineage(ctx, source, request)
	if err != nil {
		return ResumeToken{}, err
	}
	if err = r.checkRuntimeProfile(r.options.WaitProfile); err != nil {
		return ResumeToken{}, err
	}
	if err = r.forkTargetAbsent(ctx, request.TargetID); err != nil {
		return ResumeToken{}, err
	}
	session, err := r.acquireSession(ctx, request.TargetID)
	if err != nil {
		return ResumeToken{}, err
	}
	defer session.finish()
	if err = r.forkTargetAbsent(session.ctx, request.TargetID); err != nil {
		return ResumeToken{}, err
	}
	progress, err := r.transformForkProgress(session.ctx, source, request)
	if err != nil {
		return ResumeToken{}, err
	}
	effects, err := r.effectsCodec.Marshal(nil)
	if err != nil {
		return ResumeToken{}, errors.Join(ErrExecutionIncompatible, err)
	}
	if session.ctx.Err() != nil {
		return ResumeToken{}, context.Cause(session.ctx)
	}
	target := ExecutionEnvelope{ExecutionID: request.TargetID, Descriptor: r.descriptor,
		RuntimeProfile: r.options.WaitProfile, Progress: progress, EffectsPayload: effects,
		Activation: 1, RunMeta: newRunMetadata(), Fork: &lineage,
		Revision: 0, Digest: "", JournalPayload: nil, ChildrenPayload: nil, WaitsPayload: nil,
		Terminal: nil, Migration: nil, Import: nil}
	committed, err := r.store.CommitExecution(session.ctx, 0, session.lease, target)
	if err != nil {
		return ResumeToken{}, err
	}
	return ResumeToken{ThreadID: request.TargetID, SnapshotRevision: committed.Revision}, nil
}

func (r *DurableRunner[T, E]) forkTargetAbsent(ctx context.Context, id string) error {
	_, err := r.store.LoadExecution(ctx, id)
	if errors.Is(err, ErrThreadNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	return ErrForkTargetConflict
}

func (r *DurableRunner[T, E]) prepareForkLineage(ctx context.Context, source ExecutionEnvelope,
	request ForkRequest,
) (ForkLineage, error) {
	mode, label := request.Mode, request.PolicyLabel
	if mode == "" {
		mode = ForkFake
	}
	if label == "" && mode == ForkFake {
		label = defaultForkPolicy
	}
	projectionLabel := ""
	if request.Projection != nil {
		if request.Projection.Label == "" || request.Projection.Project == nil {
			return ForkLineage{}, ErrForkPolicy
		}
		projectionLabel = request.Projection.Label
	}
	lineage := ForkLineage{Source: request.Source, SourceDescriptor: source.Descriptor, TargetDescriptor: r.descriptor,
		TargetID: request.TargetID, Mode: mode, PolicyLabel: label, TransformLabel: request.Transform.Label,
		ProjectionLabel: projectionLabel, CreatedAt: r.options.Clock.Now().UTC()}
	if err := lineage.Validate(); err != nil {
		return ForkLineage{}, err
	}
	if request.Transform.Transform == nil || request.Transform.Source.Check(source.Descriptor) != nil {
		return ForkLineage{}, ErrExecutionIncompatible
	}
	if err := forkSourceSettled(source); err != nil {
		return ForkLineage{}, err
	}
	// A default fake boundary may be created for inspection with no executable
	// policy. It cannot be resumed until a matching fake dispatcher is supplied.
	if mode == ForkLive || r.options.ForkPolicy != nil {
		if err := r.checkForkPolicy(ctx, &lineage); err != nil {
			return ForkLineage{}, err
		}
	}
	return lineage, nil
}

func (r *DurableRunner[T, E]) transformForkProgress(ctx context.Context, source ExecutionEnvelope,
	request ForkRequest,
) (MigrationState, error) {
	if ctx.Err() != nil {
		return MigrationState{}, context.Cause(ctx)
	}
	progress, err := request.Transform.Transform(cloneMigrationState(source.Progress))
	if err != nil {
		return MigrationState{}, errors.Join(ErrForkTransform, err)
	}
	progress = cloneMigrationState(progress)
	if request.Projection != nil {
		progress, err = request.Projection.Project(cloneMigrationState(progress))
		if err != nil {
			return MigrationState{}, errors.Join(ErrForkTransform, err)
		}
		progress = cloneMigrationState(progress)
	}
	progress.ChildCursors, progress.JournalReferences = nil, nil
	progress.ChildGroupReferences = nil
	if err = r.validatePointer(progress.ExecutionPointer); err != nil {
		return MigrationState{}, errors.Join(ErrExecutionIncompatible, err)
	}
	if ctx.Err() != nil {
		return MigrationState{}, context.Cause(ctx)
	}
	if _, err = r.stateCodec.Unmarshal(bytes.Clone(progress.StatePayload)); err != nil {
		return MigrationState{}, errors.Join(ErrExecutionIncompatible, err)
	}
	return progress, nil
}
