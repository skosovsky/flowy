package flowy

import (
	"context"
	"errors"
)

func (r *DurableRunner[T, E]) prepareStart(
	ctx context.Context,
	lease ExecutionLease,
	initial T,
) (ExecutionEnvelope, error) {
	_, loadErr := r.store.LoadExecution(ctx, lease.ExecutionID)
	if loadErr == nil {
		return ExecutionEnvelope{}, ErrConcurrencyConflict
	}
	if !errors.Is(loadErr, ErrThreadNotFound) {
		return ExecutionEnvelope{}, loadErr
	}
	state, err := r.stateCodec.Marshal(initial)
	if err != nil {
		return ExecutionEnvelope{}, err
	}
	effects, err := r.effectsCodec.Marshal(nil)
	if err != nil {
		return ExecutionEnvelope{}, err
	}
	if ctx.Err() != nil {
		return ExecutionEnvelope{}, context.Cause(ctx)
	}
	meta := newRunMetadata()
	meta.TelemetryContext = extractTelemetryContext(ctx)
	return commitExecution(ctx, r.store, 0, lease, ExecutionEnvelope{
		ExecutionID:    lease.ExecutionID,
		Revision:       0,
		Digest:         "",
		Descriptor:     r.descriptor,
		RuntimeProfile: r.options.WaitProfile,
		Progress: ExecutionProgress{
			StatePayload:         state,
			ExecutionPointer:     ExecutionPointer(r.graph.entryPoint),
			ChildCursors:         nil,
			JournalReferences:    nil,
			ChildGroupReferences: nil,
		},
		EffectsPayload:  effects,
		JournalPayload:  nil,
		ChildrenPayload: nil,
		WaitsPayload:    nil,
		RunMeta:         meta,
		Activation:      1,
		Terminal:        nil,
		Migration:       nil,
		Import:          nil,
		Fork:            nil, Rollover: nil, Transfer: nil,
	})
}

// Stream creates a new durable execution and returns a stream after its initial
// checkpoint commits. Terminal events are emitted only after durable commit.
func (r *DurableRunner[T, E]) Stream(
	ctx context.Context,
	id string,
	initial T,
	opts ...RunOption[T, E],
) (StreamHandle[T, E], error) {
	if err := r.validateRunOptions(opts...); err != nil {
		return nil, err
	}
	if profileErr := r.checkRuntimeProfile(r.options.WaitProfile); profileErr != nil {
		return nil, profileErr
	}
	return r.preparedStream(ctx, id, func(ownedCtx context.Context, lease ExecutionLease) (ExecutionEnvelope, error) {
		return r.prepareStart(ownedCtx, lease, initial)
	}, opts...)
}

func (r *DurableRunner[T, E]) preparedStream(
	ctx context.Context, id string,
	prepare func(context.Context, ExecutionLease) (ExecutionEnvelope, error), opts ...RunOption[T, E],
) (handle StreamHandle[T, E], retErr error) { //nolint:nonamedreturns // Admission failures must report cleanup.
	inv, err := applyRunOptions(opts...)
	if err != nil {
		return nil, err
	}
	session, err := r.acquireSession(ctx, id)
	if err != nil {
		return nil, err
	}
	transferred := false
	defer func() {
		if !transferred {
			retErr = errors.Join(retErr, session.finish())
		}
	}()
	envelope, err := prepare(session.ctx, session.lease)
	if err != nil {
		return nil, err
	}
	base, ok := r.graph.NewRunner(nil).(*graphRunner[T, E])
	if !ok {
		return nil, ErrExecutionCapability
	}
	handle = base.startStream(
		session.ctx,
		inv,
		func(streamCtx context.Context, sink eventSink[T, E]) (result *RunResult[T, E], runErr error) { //nolint:nonamedreturns // Join cleanup on all exit paths.
			defer func() { runErr = errors.Join(runErr, session.finish()) }()
			return r.runWithSink(streamCtx, session.lease, envelope, sink, opts...)
		},
	)
	transferred = true
	return handle, nil
}
