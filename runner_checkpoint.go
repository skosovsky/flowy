package flowy

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// prepareSnapshot validates domain state before transforming its storage representation.
func (r *graphRunner[T, E]) prepareSnapshot(
	ctx context.Context,
	snapshot Snapshot[T, E],
	inv runInvocationOptions[T, E],
) (Snapshot[T, E], error) {
	if r.checkpointer == nil {
		return snapshot, errors.New("flowy: checkpointer is required")
	}
	if err := validateInvariant(snapshot.State, inv); err != nil {
		return snapshot, err
	}
	state := snapshot.State
	for _, interceptor := range r.interceptors {
		if err := interceptor.BeforeSave(ctx, &state); err != nil {
			return snapshot, fmt.Errorf("flowy: before_save interceptor: %w", err)
		}
	}
	snapshot.State = state
	return snapshot, nil
}

//nolint:nonamedreturns // persisted flag pairs with newRevision for terminal paths
func (r *graphRunner[T, E]) persistSnapshot(
	ctx context.Context,
	expectedRevision uint64,
	snapshot Snapshot[T, E],
	sink eventSink[T, E],
	current string,
	state T,
	inv runInvocationOptions[T, E],
) (newRevision uint64, persisted bool, err error) {
	_, newRevision, persisted, err = r.persistSnapshotPrepared(
		ctx,
		expectedRevision,
		snapshot,
		sink,
		current,
		state,
		inv,
	)
	return newRevision, persisted, err
}

func (r *graphRunner[T, E]) persistSnapshotPrepared(
	ctx context.Context,
	expectedRevision uint64,
	snapshot Snapshot[T, E],
	sink eventSink[T, E],
	current string,
	state T,
	inv runInvocationOptions[T, E],
) (Snapshot[T, E], uint64, bool, error) {
	saveCtx, cancelSave := context.WithTimeout(context.WithoutCancel(ctx), contextCancelSaveTimeout)
	defer cancelSave()
	prepared, prepErr := r.prepareSnapshot(saveCtx, snapshot, inv)
	if prepErr != nil {
		return snapshot, 0, false, prepErr
	}
	newRevision, saveErr := r.checkpointer.Save(saveCtx, expectedRevision, prepared)
	if saveErr == nil {
		return prepared, newRevision, true, nil
	}
	if inv.checkpointPolicy != CheckpointPolicySkipOnSaveError || checkpointContractError(saveErr) {
		return prepared, 0, false, saveErr
	}
	eventPointer := string(snapshot.ExecutionPointer)
	if eventPointer == "" {
		eventPointer = current
	}
	r.emitCheckpointFailed(ctx, sink, snapshot.ThreadID, eventPointer, state, saveErr)
	return prepared, 0, false, nil
}

func checkpointContractError(err error) bool {
	return errors.Is(err, ErrConcurrencyConflict) || errors.Is(err, ErrInvalidSnapshot) ||
		errors.Is(err, ErrLeaseLost) || errors.Is(err, ErrThreadLeaseBusy) ||
		errors.Is(err, ErrExecutionCapability) || errors.Is(err, ErrInvalidHandoffIntent) ||
		errors.Is(err, ErrTransactionalOutboxUnsupported)
}

func (r *graphRunner[T, E]) enqueueHandoffIntent(
	runCtx context.Context,
	outbox HandoffOutbox,
	intent HandoffIntent,
) error {
	if outbox == nil {
		return nil
	}
	if err := validateHandoffIntent(intent); err != nil {
		return err
	}
	enqueueCtx, cancelEnqueue := context.WithTimeout(
		context.WithoutCancel(runCtx),
		handoffEnqueueTimeout,
	)
	defer cancelEnqueue()
	if err := outbox.EnqueueIntent(enqueueCtx, intent); err != nil {
		return fmt.Errorf("%w: %w", ErrHandoffEnqueueFailed, err)
	}
	return nil
}

func (r *graphRunner[T, E]) resolveHandoffOutbox(inv runInvocationOptions[T, E]) HandoffOutbox {
	if inv.handoffOutbox != nil {
		return inv.handoffOutbox
	}
	return r.handoffOutbox
}

func (r *graphRunner[T, E]) patchHandoffStatus(
	ctx context.Context,
	expectedRevision uint64,
	snapshot Snapshot[T, E],
	meta RunMetadata,
	status HandoffStatus,
	_ runInvocationOptions[T, E],
) (uint64, error) {
	meta.HandoffStatus = status
	if status == HandoffStatusPending {
		meta.HandoffPendingAt = time.Now().UTC()
	} else {
		meta.HandoffPendingAt = time.Time{}
	}
	snapshot.RunMeta = meta
	saveCtx, cancelSave := context.WithTimeout(context.WithoutCancel(ctx), contextCancelSaveTimeout)
	defer cancelSave()
	// State already has its persisted representation; metadata changes must not re-encode it.
	return r.checkpointer.Save(saveCtx, expectedRevision, snapshot)
}
