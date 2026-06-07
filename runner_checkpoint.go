package flowy

import (
	"context"
	"errors"
	"fmt"
	"time"
)

func (r *graphRunner[T, E]) saveSnapshotOCC(
	ctx context.Context,
	expectedRevision uint64,
	snapshot Snapshot[T, E],
	inv runInvocationOptions[T, E],
) (uint64, error) {
	if r.checkpointer == nil {
		return 0, errors.New("flowy: checkpointer is required")
	}
	if err := validateInvariant(snapshot.State, inv); err != nil {
		return 0, err
	}
	state := snapshot.State
	for _, interceptor := range r.interceptors {
		if err := interceptor.BeforeSave(ctx, &state); err != nil {
			return 0, fmt.Errorf("flowy: before_save interceptor: %w", err)
		}
	}
	snapshot.State = state
	return r.checkpointer.Save(ctx, expectedRevision, snapshot)
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
	saveCtx, cancelSave := context.WithTimeout(context.WithoutCancel(ctx), contextCancelSaveTimeout)
	defer cancelSave()
	newRevision, saveErr := r.saveSnapshotOCC(saveCtx, expectedRevision, snapshot, inv)
	if saveErr == nil {
		return newRevision, true, nil
	}
	if inv.checkpointPolicy != CheckpointPolicySkipOnSaveError {
		return 0, false, saveErr
	}
	eventPointer := string(snapshot.ExecutionPointer)
	if eventPointer == "" {
		eventPointer = current
	}
	r.emitCheckpointFailed(ctx, sink, snapshot.ThreadID, eventPointer, state, saveErr)
	return 0, false, nil
}

func (r *graphRunner[T, E]) enqueueHandoffIntent(
	runCtx context.Context,
	outbox HandoffOutbox,
	token ResumeToken,
) error {
	if outbox == nil {
		return nil
	}
	enqueueCtx, cancelEnqueue := context.WithTimeout(
		context.WithoutCancel(runCtx),
		handoffEnqueueTimeout,
	)
	defer cancelEnqueue()
	if err := outbox.EnqueueIntent(enqueueCtx, token); err != nil {
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
	inv runInvocationOptions[T, E],
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
	return r.saveSnapshotOCC(saveCtx, expectedRevision, snapshot, inv)
}
