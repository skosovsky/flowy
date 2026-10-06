package flowy

import (
	"context"
	"errors"
	"fmt"
	"time"
)

//nolint:funlen,gocognit // handoff FSM: save, patch status, enqueue, retention
func (r *graphRunner[T, E]) completeHandoffTerminal(
	runCtx context.Context,
	threadID, current string,
	state T,
	meta RunMetadata,
	effects []E,
	revision uint64,
	reason string,
	resumeAt ExecutionPointer,
	sink eventSink[T, E],
	inv runInvocationOptions[T, E],
	signalSession bool,
) (*RunResult[T, E], error) {
	meta.TelemetryContext = extractTelemetryContext(runCtx)
	meta.Segment.EndTime = time.Now().UTC()
	meta.Segment.EndReason = SegmentEndHandoff
	resumePtr := resumeAt
	if resumePtr == "" {
		resumePtr = ExecutionPointer(current)
	}
	if ptrErr := r.validateExecutionPointer(resumePtr); ptrErr != nil {
		handoffErr := fmt.Errorf("flowy: handoff resume target invalid: %w", ptrErr)
		emitTerminalEvent(
			runCtx,
			sink,
			newRunEventFailed[T, E](current, state, ptrErr, ReasonHandoffResumeTargetInvalid),
		)
		return newRunResultFailed(
			state,
			effects,
			meta,
			current,
			ReasonHandoffResumeTargetInvalid,
		), handoffErr
	}
	savedPointer := string(resumePtr)
	outbox := r.resolveHandoffOutbox(inv)
	if outbox != nil {
		meta.HandoffStatus = HandoffStatusPending
		meta.HandoffPendingAt = time.Now().UTC()
	} else {
		meta.HandoffStatus = HandoffStatusNone
		meta.HandoffPendingAt = time.Time{}
	}
	result := newRunResultHandoff(state, effects, meta, savedPointer, reason)
	snapshot := Snapshot[T, E]{
		ThreadID:         threadID,
		ExecutionPointer: resumePtr,
		Revision:         0,
		State:            state,
		RunMeta:          meta,
		Effects:          append([]E(nil), effects...),
	}
	if outbox != nil {
		if txRev, done, txErr := r.tryTransactionalHandoffSave(
			runCtx, threadID, revision, snapshot, meta, inv, outbox, result,
		); done {
			if txErr != nil {
				handoffErr := fmt.Errorf("flowy: transactional handoff failed: %w", txErr)
				emitTerminalEvent(
					runCtx,
					sink,
					newRunEventFailed[T, E](savedPointer, state, txErr, ReasonHandoffSaveFailed),
				)
				failed := newRunResultFailed(
					state,
					effects,
					meta,
					savedPointer,
					ReasonHandoffSaveFailed,
				)
				clearHandoffRunMeta(&failed.RunMeta)
				return failed, handoffErr
			}
			result.ResumeToken = ResumeToken{ThreadID: threadID, SnapshotRevision: txRev}
			return r.finalizeHandoffTerminal(
				runCtx,
				threadID,
				savedPointer,
				state,
				result.Reason,
				result,
				sink,
				signalSession,
				true,
			)
		}
	}
	snapshot, newRev, persisted, saveErr := r.persistSnapshotPrepared(
		runCtx,
		revision,
		snapshot,
		sink,
		current,
		state,
		inv,
	)
	if saveErr != nil {
		handoffErr := fmt.Errorf("flowy: handoff save failed: %w", saveErr)
		emitTerminalEvent(
			runCtx,
			sink,
			newRunEventFailed[T, E](savedPointer, state, saveErr, ReasonHandoffSaveFailed),
		)
		failed := newRunResultFailed(
			state,
			effects,
			meta,
			savedPointer,
			ReasonHandoffSaveFailed,
		)
		clearHandoffRunMeta(&failed.RunMeta)
		return failed, handoffErr
	}
	if !persisted && inv.checkpointPolicy == CheckpointPolicySkipOnSaveError {
		result.Reason = ReasonHandoffCheckpointSkipped
		if outbox != nil {
			clearHandoffRunMeta(&result.RunMeta)
		}
	}
	if persisted {
		result.ResumeToken = ResumeToken{ThreadID: threadID, SnapshotRevision: newRev}
		if outbox != nil {
			if earlyRes, done, earlyErr := r.dispatchPersistedHandoffOutbox(
				runCtx, threadID, newRev, snapshot, meta, inv, outbox, resumePtr,
				savedPointer, state, effects, result, sink,
			); done {
				return earlyRes, earlyErr
			}
		}
	}
	return r.finalizeHandoffTerminal(
		runCtx,
		threadID,
		savedPointer,
		state,
		result.Reason,
		result,
		sink,
		signalSession,
		persisted,
	)
}

func (r *graphRunner[T, E]) handoffAfterEnqueueFailure(
	runCtx context.Context,
	threadID, savedPointer string,
	state T,
	result *RunResult[T, E],
	sink eventSink[T, E],
	enqueueErr error,
) (*RunResult[T, E], error) {
	policyCtx, cancelPolicy := context.WithTimeout(
		context.WithoutCancel(runCtx),
		contextCancelSaveTimeout,
	)
	policyErr := r.applyRetentionPolicy(policyCtx, threadID)
	cancelPolicy()

	finalReason := result.Reason
	if policyErr != nil {
		finalReason = retentionFailedReason(finalReason)
		result.Reason = finalReason
	}
	r.emitHandoffTerminalEvent(runCtx, sink, threadID, savedPointer, state, finalReason, true)

	retErr := enqueueErr
	if policyErr != nil {
		retentionErr := fmt.Errorf("flowy: handoff retention failed: %w", policyErr)
		retErr = errors.Join(enqueueErr, retentionErr)
	}
	return result, retErr
}

func (r *graphRunner[T, E]) finalizeHandoffTerminal(
	runCtx context.Context,
	threadID, savedPointer string,
	state T,
	reason string,
	result *RunResult[T, E],
	sink eventSink[T, E],
	signalSession bool,
	persisted bool,
) (*RunResult[T, E], error) {
	var policyErr error
	finalReason := reason
	if persisted {
		policyCtx, cancelPolicy := context.WithTimeout(
			context.WithoutCancel(runCtx),
			contextCancelSaveTimeout,
		)
		policyErr = r.applyRetentionPolicy(policyCtx, threadID)
		cancelPolicy()
		if policyErr != nil {
			finalReason = retentionFailedReason(reason)
			result.Reason = finalReason
		}
	}
	r.emitHandoffTerminalEvent(runCtx, sink, threadID, savedPointer, state, finalReason, persisted)
	return r.completeHandoffSession(result, policyErr, persisted, signalSession)
}

func (r *graphRunner[T, E]) emitHandoffTerminalEvent(
	runCtx context.Context,
	sink eventSink[T, E],
	threadID, savedPointer string,
	state T,
	reason string,
	persisted bool,
) {
	if !emitTerminalEvent(runCtx, sink, newRunEventHandoff[T, E](savedPointer, state, reason)) {
		msg := "flowy: handoff terminal event not delivered"
		if persisted {
			msg = "flowy: handoff persisted but terminal event not delivered"
		}
		r.logger.DebugContext(runCtx, msg, "thread_id", threadID)
	}
}

func (r *graphRunner[T, E]) completeHandoffSession(
	result *RunResult[T, E],
	policyErr error,
	persisted bool,
	signalSession bool,
) (*RunResult[T, E], error) {
	if policyErr != nil {
		return result, fmt.Errorf("flowy: handoff retention failed: %w", policyErr)
	}
	if !persisted && signalSession {
		return result, ErrCheckpointSkipped
	}
	return result, nil
}

func (r *graphRunner[T, E]) handleHandoff(
	runCtx context.Context,
	threadID, current string,
	state T,
	meta RunMetadata,
	effects []E,
	revision uint64,
	sink eventSink[T, E],
	inv runInvocationOptions[T, E],
) (*RunResult[T, E], error) {
	snapshot, restoreErr := r.interruptionSnapshot(runCtx, threadID, current, state, meta, effects)
	if restoreErr != nil {
		return failedDiagnosticResult(state, effects, meta, current, context.Cause(runCtx), restoreErr)
	}
	state, meta, effects = snapshot.State, snapshot.RunMeta, snapshot.Effects
	current = string(snapshot.ExecutionPointer)
	reason := "background_handoff"
	if cause := context.Cause(runCtx); cause != nil && !errors.Is(cause, ErrHandoffRequested) {
		reason = cause.Error()
	}
	return r.completeHandoffTerminal(
		runCtx, threadID, current, state, meta, effects, revision, reason, "", sink, inv, true,
	)
}

func (r *graphRunner[T, E]) emitCheckpointFailed(
	ctx context.Context,
	sink eventSink[T, E],
	threadID, current string,
	state T,
	saveErr error,
) {
	emitCheckpointSoftError(ctx, threadID, ExecutionPointer(current))
	if sink == nil {
		r.logger.DebugContext(ctx, "flowy: checkpoint soft warn without event sink",
			"thread_id", threadID, "err", saveErr)
		return
	}
	if !emitTerminalEvent(ctx, sink, newRunEventCheckpointFailed[T, E](current, state, saveErr)) {
		r.logger.DebugContext(ctx, "flowy: checkpoint failed event not delivered",
			"thread_id", threadID)
	}
}

func (r *graphRunner[T, E]) applyRetentionPolicy(ctx context.Context, threadID string) error {
	if r.checkpointer == nil || r.graph.defaults.retentionLimit <= 0 {
		return nil
	}
	return r.checkpointer.Prune(ctx, threadID, r.graph.defaults.retentionLimit)
}

func appliesTerminalPolicies(status RunStatus) bool {
	return status == RunStatusCompleted || status == RunStatusFailed
}

func (r *graphRunner[T, E]) postRunCleanup(
	ctx context.Context,
	threadID string,
	inv runInvocationOptions[T, E],
	result *RunResult[T, E],
) error {
	releaseErr := r.releaseLease(ctx, threadID, inv)
	r.logReleaseLeaseError(ctx, threadID, releaseErr)
	var policyErr error
	if result != nil && threadID != "" && appliesTerminalPolicies(result.Status) {
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), contextCancelSaveTimeout)
		policyErr = r.applyTerminalPolicies(cleanupCtx, threadID, result.Status)
		cancel()
	}
	if err := errors.Join(releaseErr, policyErr); err != nil {
		return fmt.Errorf("%w: %w", ErrRunCleanup, err)
	}
	return nil
}

func (r *graphRunner[T, E]) logReleaseLeaseError(ctx context.Context, threadID string, err error) {
	if err != nil {
		r.logger.WarnContext(ctx, "flowy: release lease failed",
			"thread_id", threadID, "err", err)
	}
}

func (r *graphRunner[T, E]) applyTerminalPolicies(
	ctx context.Context,
	threadID string,
	status RunStatus,
) error {
	if r.checkpointer == nil {
		return nil
	}
	if status == RunStatusCompleted && r.graph.defaults.deleteOnSuccess {
		return r.checkpointer.DeleteIfIdle(ctx, threadID)
	}
	return r.applyRetentionPolicy(ctx, threadID)
}
