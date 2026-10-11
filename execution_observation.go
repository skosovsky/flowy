package flowy

import "context"

func executionObservation(
	envelope ExecutionEnvelope,
	operation LifecycleOperation,
	stage LifecycleStage,
) LifecycleObservation {
	event := lifecycleObservation(operation, stage, envelope.ExecutionID, envelope.Progress.ExecutionPointer)
	event.SegmentID = envelope.RunMeta.Segment.SegmentID
	event.SourceRevision = envelope.Revision
	return event
}

func observeCheckpointOutcome(ctx context.Context, event LifecycleObservation, revision uint64, err error) {
	if err == nil {
		if revision <= event.SourceRevision {
			return // Cursor-neutral/replay paths performed no new publication.
		}
		event.Stage, event.Revision = LifecycleCommitted, revision
	}
	observeLifecycle(ctx, event)
}

func (c *executionCheckpointer[T, E]) commitStep(
	ctx context.Context,
	step directiveStep[T, E],
	directive directiveKind,
	state T,
	meta RunMetadata,
	effects []E,
) (uint64, error) {
	operation := LifecycleCheckpoint
	if step.terminal && step.result != nil &&
		(step.result.Status == RunStatusCompleted || step.result.Status == RunStatusFailed) {
		operation = LifecycleTerminal
	}
	c.mu.Lock()
	event := executionObservation(c.envelope, operation, LifecycleFailed)
	if operation == LifecycleTerminal {
		event.Code = lifecycleOutcomeCompleted
		if step.result.Status == RunStatusFailed {
			event.Code = lifecycleOutcomeFailed
		}
	}
	event.SegmentID = meta.Segment.SegmentID
	revision, err := c.commitStepLocked(ctx, step, directive, state, meta, effects)
	c.mu.Unlock()
	observeCheckpointOutcome(ctx, event, revision, err)
	return revision, err
}

func observeInvocationOutcome[T, E any](
	ctx context.Context,
	event LifecycleObservation,
	result *RunResult[T, E],
	runErr error,
) {
	event.Stage = LifecycleFailed
	if runErr == nil && result != nil {
		event.Stage, event.Revision = LifecycleCommitted, result.ResumeToken.SnapshotRevision
	}
	observeLifecycle(ctx, event)
}

func (c *executionCheckpointer[T, E]) commitRunFailure(
	ctx context.Context,
	result *RunResult[T, E],
	runErr error,
) error {
	c.mu.Lock()
	event := executionObservation(c.envelope, LifecycleTerminal, LifecycleFailed)
	event.Code = lifecycleOutcomeFailed
	err := c.commitRunFailureLocked(ctx, result, runErr)
	revision := c.envelope.Revision
	c.mu.Unlock()
	observeCheckpointOutcome(ctx, event, revision, err)
	return err
}

func cachedDurableResult[T, E any](ctx context.Context, sink eventSink[T, E], envelope ExecutionEnvelope,
	snapshot Snapshot[T, E]) (*RunResult[T, E], error) {
	result := &RunResult[T, E]{
		State: snapshot.State, Effects: snapshot.Effects, Status: envelope.Terminal.Status,
		Reason: envelope.Terminal.Reason, RunMeta: snapshot.RunMeta, ExecutionPointer: snapshot.ExecutionPointer,
		ResumeToken: ResumeToken{ThreadID: envelope.ExecutionID, SnapshotRevision: envelope.Revision},
	}
	emitCachedDurableTerminal(ctx, sink, envelope, snapshot)
	return result, terminalFailureError(envelope.Terminal)
}

func observeCommittedRetry(ctx context.Context, event LifecycleObservation, state ActivityState) {
	if state != ActivityPrepared || event.Stage != LifecycleCommitted {
		return
	}
	event.Operation = LifecycleRetry
	observeLifecycle(ctx, event)
}

func rolloverReplayObservation(
	event LifecycleObservation,
	receipt RolloverReceipt,
	replayErr error,
) LifecycleObservation {
	if replayErr == nil {
		event.Stage, event.Revision, event.TargetRevision = LifecycleReplayed, receipt.SourceRevision, receipt.Target.Revision
	}
	return event
}

func rolloverOperationObservation(token ResumeToken, request RolloverRequest) LifecycleObservation {
	event := lifecycleObservation(LifecycleRollover, LifecycleFailed, token.ThreadID, "")
	event.SourceExecutionID, event.SourceRevision = token.ThreadID, token.SnapshotRevision
	event.TargetExecutionID, event.DecisionID = request.TargetID, request.DecisionID
	return event
}
