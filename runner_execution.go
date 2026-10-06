package flowy

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"
)

//nolint:gocognit,funlen,nonamedreturns // central run loop; named retErr for session-scoped defer finish
func (r *graphRunner[T, E]) execute(
	ctx context.Context,
	threadID string,
	startNode string,
	state T,
	meta RunMetadata,
	effects []E,
	revision uint64,
	sink eventSink[T, E],
	inv runInvocationOptions[T, E],
) (result *RunResult[T, E], retErr error) {
	current := startNode
	runCtx, cancelRun := context.WithCancelCause(ctx)
	defer cancelRun(context.Canceled)
	runCtx = withRunThreadID(runCtx, threadID)
	session, regErr := r.registerRunSession(threadID, cancelRun)
	if regErr != nil {
		return nil, regErr
	}
	defer func() {
		session.finish(retErr)
		r.unregisterRunSessionIfSame(threadID, session)
	}()

	limit := r.graph.defaults.maxSteps
	if limit <= 0 {
		limit = defaultMaxSteps
	}

	stopHeartbeat := r.startLeaseHeartbeat(runCtx, inv, cancelRun)
	defer stopHeartbeat()

	for {
		if errors.Is(context.Cause(runCtx), ErrHandoffRequested) {
			return r.handleHandoff(
				runCtx, threadID, current, state, meta, effects, revision, sink, inv,
			)
		}
		if errors.Is(context.Cause(runCtx), ErrLeaseLost) {
			markSegmentFailed(&meta)
			emitTerminalEvent(
				runCtx,
				sink,
				newRunEventFailed[T, E](current, state, ErrLeaseLost, ErrLeaseLost.Error()),
			)
			return failedResultWithReason(
				state,
				effects,
				meta,
				current,
				ErrLeaseLost.Error(),
			), ErrLeaseLost
		}
		if ctxErr := runCtx.Err(); ctxErr != nil {
			return r.handleContextCancellation(
				runCtx, threadID, current, state, meta, effects, revision, sink, ctxErr, inv,
				streamConsumerClosed(runCtx),
			)
		}

		if meta.StepCount >= limit {
			markSegmentFailed(&meta)
			emitTerminalEvent(
				runCtx,
				sink,
				newRunEventFailed[T, E](current, state, ErrMaxStepsExceeded, ErrMaxStepsExceeded.Error()),
			)
			return failedResultWithReason(
				state,
				effects,
				meta,
				current,
				ErrMaxStepsExceeded.Error(),
			), ErrMaxStepsExceeded
		}

		nodeCtx := withNodeName(runCtx, current)
		step, stepErr := r.runNodeStep(nodeCtx, runCtx, current, state, meta, effects, sink, inv)
		if stepErr != nil {
			if errors.Is(context.Cause(runCtx), ErrLeaseLost) {
				markSegmentFailed(&step.meta)
				emitTerminalEvent(
					runCtx,
					sink,
					newRunEventFailed[T, E](
						current,
						step.state,
						ErrLeaseLost,
						ErrLeaseLost.Error(),
					),
				)
				return failedResultWithReason(
					step.state,
					step.effects,
					step.meta,
					current,
					ErrLeaseLost.Error(),
				), ErrLeaseLost
			}
			if runCtx.Err() != nil &&
				(errors.Is(stepErr, context.Canceled) || errors.Is(stepErr, context.DeadlineExceeded)) {
				if errors.Is(context.Cause(runCtx), ErrHandoffRequested) {
					return r.handleHandoff(
						runCtx,
						threadID,
						current,
						step.state,
						step.meta,
						step.effects,
						revision,
						sink,
						inv,
					)
				}
				return r.handleContextCancellation(
					runCtx,
					threadID,
					current,
					step.state,
					step.meta,
					step.effects,
					revision,
					sink,
					runCtx.Err(),
					inv,
					streamConsumerClosed(runCtx),
				)
			}
			return failedResultWithReason(
				step.state, step.effects, step.meta, current, failReasonForStepErr(stepErr),
			), stepErr
		}
		if step.emitCanceled {
			if errors.Is(context.Cause(runCtx), ErrHandoffRequested) {
				return r.handleHandoff(
					runCtx,
					threadID,
					current,
					step.state,
					step.meta,
					step.effects,
					revision,
					sink,
					inv,
				)
			}
			if errors.Is(context.Cause(runCtx), ErrLeaseLost) {
				markSegmentFailed(&step.meta)
				emitTerminalEvent(
					runCtx,
					sink,
					newRunEventFailed[T, E](
						current,
						step.state,
						ErrLeaseLost,
						ErrLeaseLost.Error(),
					),
				)
				return failedResultWithReason(
					step.state,
					step.effects,
					step.meta,
					current,
					ErrLeaseLost.Error(),
				), ErrLeaseLost
			}
			return r.handleContextCancellation(
				runCtx,
				threadID,
				current,
				step.state,
				step.meta,
				step.effects,
				revision,
				sink,
				context.Canceled,
				inv,
				true,
			)
		}
		if errors.Is(context.Cause(runCtx), ErrHandoffRequested) {
			return r.handleHandoff(
				runCtx, threadID, current, step.state, step.meta, step.effects, revision, sink, inv,
			)
		}
		if errors.Is(context.Cause(runCtx), ErrLeaseLost) {
			markSegmentFailed(&step.meta)
			emitTerminalEvent(
				runCtx,
				sink,
				newRunEventFailed[T, E](current, step.state, ErrLeaseLost, ErrLeaseLost.Error()),
			)
			return failedResultWithReason(
				step.state,
				step.effects,
				step.meta,
				current,
				ErrLeaseLost.Error(),
			), ErrLeaseLost
		}
		if runCtx.Err() != nil {
			return r.handleContextCancellation(
				runCtx,
				threadID,
				current,
				step.state,
				step.meta,
				step.effects,
				revision,
				sink,
				runCtx.Err(),
				inv,
				streamConsumerClosed(runCtx),
			)
		}

		state = step.state
		meta = step.meta
		effects = step.effects
		base := step.base

		if err := checkBudgetLimits(meta, r.graph.defaults.budgetLimits); err != nil {
			markSegmentFailed(&meta)
			emitTerminalEvent(
				runCtx,
				sink,
				newRunEventFailed[T, E](current, state, err, err.Error()),
			)
			return failedResultWithReason(state, effects, meta, current, err.Error()), err
		}

		if errors.Is(context.Cause(runCtx), ErrLeaseLost) {
			markSegmentFailed(&meta)
			emitTerminalEvent(
				runCtx,
				sink,
				newRunEventFailed[T, E](current, state, ErrLeaseLost, ErrLeaseLost.Error()),
			)
			return failedResultWithReason(
				state,
				effects,
				meta,
				current,
				ErrLeaseLost.Error(),
			), ErrLeaseLost
		}

		stepOut := r.applyDirective(
			runCtx, nodeCtx, threadID, current, state, meta, effects, revision, base, sink, inv,
		)
		if r.durable != nil {
			var commitErr error
			revision, commitErr = r.durable.commitStep(runCtx, stepOut, base.kind, state, meta, effects)
			if commitErr != nil {
				return r.failedDurableStep(runCtx, threadID, current, state, meta, effects, commitErr)
			}
			if stepOut.result != nil {
				stepOut.result.ResumeToken = ResumeToken{ThreadID: threadID, SnapshotRevision: revision}
			}
		}
		if stepOut.terminal {
			return stepOut.result, stepOut.err
		}
		current = stepOut.nextNode
	}
}

func (r *graphRunner[T, E]) handleContextCancellation(
	runCtx context.Context,
	threadID, current string,
	state T,
	meta RunMetadata,
	effects []E,
	revision uint64,
	sink eventSink[T, E],
	ctxErr error,
	inv runInvocationOptions[T, E],
	consumerClose bool,
) (*RunResult[T, E], error) {
	snapshot, restoreErr := r.interruptionSnapshot(runCtx, threadID, current, state, meta, effects)
	if restoreErr != nil {
		return failedDiagnosticResult(
			state,
			effects,
			meta,
			current,
			errors.Join(ctxErr, context.Cause(runCtx)),
			restoreErr,
		)
	}
	state, meta, effects = snapshot.State, snapshot.RunMeta, snapshot.Effects
	current = string(snapshot.ExecutionPointer)
	meta.TelemetryContext = extractTelemetryContext(runCtx)
	meta.Segment.EndTime = time.Now().UTC()
	meta.Segment.EndReason = SegmentEndContextCanceled
	result := newRunResultContextCanceled(state, effects, meta, current)

	newRev, persisted, saveErr := r.persistSnapshot(runCtx, revision, Snapshot[T, E]{
		ThreadID:         threadID,
		ExecutionPointer: ExecutionPointer(current),
		Revision:         0,
		State:            state,
		RunMeta:          meta,
		Effects:          append([]E(nil), effects...),
	}, sink, current, state, inv)
	if saveErr != nil {
		result.Reason = ReasonContextCanceledSaveFailed
		emitTerminalEvent(
			runCtx,
			sink,
			newRunEventContextCanceled[T, E](current, state, result.Reason),
		)
		return result, fmt.Errorf("flowy: context canceled and save failed: %w", saveErr)
	}
	if !persisted && inv.checkpointPolicy == CheckpointPolicySkipOnSaveError {
		result.Reason = ReasonContextCanceledCheckpointSkipped
	}
	if persisted {
		result.ResumeToken = ResumeToken{ThreadID: threadID, SnapshotRevision: newRev}
	}
	var policyErr error
	if persisted {
		policyCtx, cancelPolicy := context.WithTimeout(
			context.WithoutCancel(runCtx),
			contextCancelSaveTimeout,
		)
		policyErr = r.applyRetentionPolicy(policyCtx, threadID)
		cancelPolicy()
		if policyErr != nil {
			result.Reason = retentionFailedReason(result.Reason)
		}
	}
	emitTerminalEvent(runCtx, sink, newRunEventContextCanceled[T, E](current, state, result.Reason))
	if policyErr != nil {
		return result, fmt.Errorf("flowy: context canceled, retention failed: %w", policyErr)
	}
	if !persisted && inv.checkpointPolicy == CheckpointPolicySkipOnSaveError {
		if consumerClose || runCtx.Err() == nil {
			return result, ErrCheckpointSkipped
		}
		return result, fmt.Errorf("flowy: %w", ctxErr)
	}
	return result, fmt.Errorf("flowy: %w", ctxErr)
}

func directiveResumePointer(current string, directive Directive) ExecutionPointer {
	if directive.resumeAt != "" {
		return directive.resumeAt
	}
	return ExecutionPointer(current)
}

func (r *graphRunner[T, E]) runNodeStep(
	nodeCtx, runCtx context.Context,
	current string,
	state T,
	meta RunMetadata,
	effects []E,
	sink eventSink[T, E],
	inv runInvocationOptions[T, E],
) (nodeStepOutcome[T, E], error) {
	node, ok := r.graph.nodes[current]
	if !ok {
		err := fmt.Errorf("flowy: node %q not found", current)
		emitTerminalEvent(runCtx, sink, newRunEventFailed[T, E](current, state, err, err.Error()))
		return blankNodeStepOutcome(state, meta, effects), err
	}

	if !emitEvent(nodeCtx, sink, newRunEventNodeStarted[T, E](current, state)) {
		return canceledNodeStepOutcome(state, meta, effects), nil
	}

	meta.StepCount++
	nodeStart := time.Now()
	update, directive, err := node.handler(nodeCtx, state)
	nodeDuration := time.Since(nodeStart)
	if err != nil {
		emitTerminalEvent(runCtx, sink, newRunEventFailed[T, E](current, state, err, err.Error()))
		return blankNodeStepOutcome(
				state,
				meta,
				effects,
			), fmt.Errorf(
				"flowy: node %q: %w",
				current,
				err,
			)
	}

	state = r.graph.reducer(state, update)

	if inv.invariantValidator != nil {
		if invErr := inv.invariantValidator(state); invErr != nil {
			emitTerminalEvent(
				runCtx,
				sink,
				newRunEventFailed[T, E](current, state, invErr, invErr.Error()),
			)
			return blankNodeStepOutcome(state, meta, effects), invErr
		}
	}

	base, nodeEffects, err := UnwrapDirective[E](directive)
	if err != nil {
		emitTerminalEvent(runCtx, sink, newRunEventFailed[T, E](current, state, err, err.Error()))
		return blankNodeStepOutcome(state, meta, effects), err
	}

	effects = append(effects, nodeEffects...)
	if len(nodeEffects) == 0 {
		if !emitEvent(nodeCtx, sink, newRunEventNodeCompleted[T, E](current, state, nodeDuration)) {
			return canceledNodeStepOutcome(state, meta, effects), nil
		}
	} else {
		for _, effect := range nodeEffects {
			if !emitEvent(nodeCtx, sink, newRunEventNodeCompletedWithEffect[T, E](
				current, state, effect, nodeDuration,
			)) {
				return canceledNodeStepOutcome(state, meta, effects), nil
			}
		}
	}

	out := blankNodeStepOutcome(state, meta, effects)
	out.base = base
	return out, nil
}

func (r *graphRunner[T, E]) applyDirective(
	runCtx, nodeCtx context.Context,
	threadID, current string,
	state T,
	meta RunMetadata,
	effects []E,
	revision uint64,
	base Directive,
	sink eventSink[T, E],
	inv runInvocationOptions[T, E],
) directiveStep[T, E] {
	switch base.kind {
	case directiveWait:
		return r.applyDirectiveWait(runCtx, threadID, current, state, meta, effects, revision, base, sink)
	case directiveCompleted:
		return r.applyDirectiveCompleted(
			runCtx,
			nodeCtx,
			threadID,
			current,
			state,
			meta,
			effects,
			sink,
		)
	case directiveNext:
		return r.terminalFailDirectiveStep(
			runCtx, sink, current, state, meta, effects, ErrRemovedNext,
		)
	case directiveEnd:
		return r.finishCompleted(
			runCtx,
			threadID,
			current,
			state,
			meta,
			effects,
			sink,
			SegmentEndComplete,
		)
	case directiveSuspend:
		return r.applyDirectiveSuspend(
			runCtx,
			threadID,
			current,
			state,
			meta,
			effects,
			revision,
			base,
			sink,
			inv,
		)
	case directiveHandoff:
		return r.applyDirectiveHandoff(
			runCtx,
			threadID,
			current,
			state,
			meta,
			effects,
			revision,
			base,
			sink,
			inv,
		)
	case directiveRetry:
		return r.applyDirectiveRetry(runCtx, current, state, meta, effects, base, sink)
	case directiveFail:
		return r.applyDirectiveFail(runCtx, threadID, current, state, meta, effects, base, sink)
	default:
		unsupported := errors.New("flowy: node returned unsupported directive")
		return r.terminalFailDirectiveStep(
			runCtx, sink, current, state, meta, effects, unsupported,
		)
	}
}

func (r *graphRunner[T, E]) applyDirectiveFail(
	runCtx context.Context,
	_ string,
	current string,
	state T,
	meta RunMetadata,
	effects []E,
	base Directive,
	sink eventSink[T, E],
) directiveStep[T, E] {
	meta.Segment.EndTime = time.Now().UTC()
	meta.Segment.EndReason = SegmentEndFail
	result := newRunResultFailed(state, effects, meta, current, base.reason)
	emitTerminalEvent(
		runCtx,
		sink,
		newRunEventFailed[T, E](current, state, errors.New(base.reason), base.reason),
	)
	return terminalDirectiveStep(result, errors.New(base.reason))
}

func (r *graphRunner[T, E]) applyDirectiveCompleted(
	runCtx, nodeCtx context.Context,
	threadID, current string,
	state T,
	meta RunMetadata,
	effects []E,
	sink eventSink[T, E],
) directiveStep[T, E] {
	nextNode, err := r.resolveEdge(nodeCtx, current, state)
	if err != nil {
		emitTerminalEvent(runCtx, sink, newRunEventFailed[T, E](current, state, err, err.Error()))
		return terminalDirectiveStep(
			failedResultWithReason(state, effects, meta, current, err.Error()),
			err,
		)
	}
	if nextNode == EndNode {
		return r.finishCompleted(
			runCtx,
			threadID,
			current,
			state,
			meta,
			effects,
			sink,
			SegmentEndComplete,
		)
	}
	return continueDirectiveStep[T, E](nextNode)
}

func (r *graphRunner[T, E]) finishCompleted(
	runCtx context.Context,
	threadID, current string,
	state T,
	meta RunMetadata,
	effects []E,
	sink eventSink[T, E],
	endReason SegmentEndReason,
) directiveStep[T, E] {
	meta.Segment.EndTime = time.Now().UTC()
	meta.Segment.EndReason = endReason
	result := newRunResultCompleted(state, effects, meta, current)
	if !emitTerminalEvent(runCtx, sink, newRunEventCompleted[T, E](current, state)) {
		r.logger.DebugContext(runCtx, "flowy: completed but terminal event not delivered",
			"thread_id", threadID)
	}
	return terminalDirectiveStep[T, E](result, nil)
}

func (r *graphRunner[T, E]) applyDirectiveSuspend(
	runCtx context.Context,
	threadID, current string,
	state T,
	meta RunMetadata,
	effects []E,
	revision uint64,
	base Directive,
	sink eventSink[T, E],
	inv runInvocationOptions[T, E],
) directiveStep[T, E] {
	meta.TelemetryContext = extractTelemetryContext(runCtx)
	meta.Segment.EndTime = time.Now().UTC()
	meta.Segment.EndReason = SegmentEndSuspend
	resumePtr := directiveResumePointer(current, base)
	if validateErr := r.validateExecutionPointer(resumePtr); validateErr != nil {
		emitTerminalEvent(
			runCtx,
			sink,
			newRunEventFailed[T, E](current, state, validateErr, ReasonSuspendResumeTargetInvalid),
		)
		return terminalDirectiveStep(
			failedResultWithReason(
				state,
				effects,
				meta,
				current,
				ReasonSuspendResumeTargetInvalid,
			),
			fmt.Errorf("flowy: suspend resume target invalid: %w", validateErr),
		)
	}
	savedPointer := string(resumePtr)
	snapshot := Snapshot[T, E]{
		ThreadID:         threadID,
		ExecutionPointer: resumePtr,
		Revision:         0,
		State:            state,
		RunMeta:          meta,
		Effects:          append([]E(nil), effects...),
	}
	newRev, persisted, saveErr := r.persistSnapshot(runCtx, revision, snapshot, sink, current, state, inv)
	if saveErr != nil {
		emitTerminalEvent(
			runCtx,
			sink,
			newRunEventFailed[T, E](savedPointer, state, saveErr, ReasonSuspendSaveFailed),
		)
		return terminalDirectiveStep(
			failedResultWithReason(state, effects, meta, savedPointer, ReasonSuspendSaveFailed),
			fmt.Errorf("flowy: suspend save failed: %w", saveErr),
		)
	}
	suspendReason := base.reason
	if suspendReason == "" {
		suspendReason = "suspended"
	}
	if !persisted && inv.checkpointPolicy == CheckpointPolicySkipOnSaveError {
		suspendReason = ReasonSuspendedCheckpointSkipped
	}
	var policyErr error
	if persisted {
		policyCtx, cancelPolicy := context.WithTimeout(
			context.WithoutCancel(runCtx),
			contextCancelSaveTimeout,
		)
		policyErr = r.applyRetentionPolicy(policyCtx, threadID)
		cancelPolicy()
		if policyErr != nil {
			suspendReason = retentionFailedReason(suspendReason)
		}
	}
	suspendedResult := newRunResultSuspended(state, effects, meta, savedPointer, suspendReason)
	if persisted {
		suspendedResult.ResumeToken = ResumeToken{ThreadID: threadID, SnapshotRevision: newRev}
	}
	if !emitTerminalEvent(
		runCtx,
		sink,
		newRunEventSuspendedNoError[T, E](savedPointer, state, suspendReason),
	) {
		msg := "flowy: suspend terminal event not delivered"
		if persisted {
			msg = "flowy: suspend persisted but terminal event not delivered"
		}
		r.logger.DebugContext(runCtx, msg, "thread_id", threadID)
		if policyErr != nil {
			return terminalDirectiveStep(
				suspendedResult,
				fmt.Errorf("flowy: suspend retention failed: %w", policyErr),
			)
		}
		return terminalDirectiveStep(suspendedResult, nil)
	}
	if policyErr != nil {
		return terminalDirectiveStep(
			suspendedResult,
			fmt.Errorf("flowy: suspend retention failed: %w", policyErr),
		)
	}
	return terminalDirectiveStep(suspendedResult, nil)
}

func retentionFailedReason(reason string) string {
	if reason == "" {
		return "retention_failed"
	}
	return reason + "_retention_failed"
}

func (r *graphRunner[T, E]) applyDirectiveHandoff(
	runCtx context.Context,
	threadID, current string,
	state T,
	meta RunMetadata,
	effects []E,
	revision uint64,
	base Directive,
	sink eventSink[T, E],
	inv runInvocationOptions[T, E],
) directiveStep[T, E] {
	reason := base.reason
	if reason == "" {
		reason = string(RunStatusHandoff)
	}
	result, err := r.completeHandoffTerminal(
		runCtx,
		threadID,
		current,
		state,
		meta,
		effects,
		revision,
		reason,
		directiveResumePointer(current, base),
		sink,
		inv,
		false,
	)
	return terminalDirectiveStep(result, err)
}

func (r *graphRunner[T, E]) applyDirectiveRetry(
	runCtx context.Context,
	current string,
	state T,
	meta RunMetadata,
	effects []E,
	base Directive,
	sink eventSink[T, E],
) directiveStep[T, E] {
	if base.maxAttempts <= 0 {
		err := errors.New("flowy: Retry directive requires maxAttempts > 0")
		return r.terminalFailDirectiveStep(runCtx, sink, current, state, meta, effects, err)
	}
	meta.RetryCounts[current]++
	if meta.RetryCounts[current] > base.maxAttempts {
		markSegmentFailed(&meta)
		emitTerminalEvent(
			runCtx,
			sink,
			newRunEventFailed[T, E](
				current,
				state,
				ErrRetryBudgetExceeded,
				ErrRetryBudgetExceeded.Error(),
			),
		)
		return terminalDirectiveStep(
			failedResultWithReason(state, effects, meta, current, ErrRetryBudgetExceeded.Error()),
			ErrRetryBudgetExceeded,
		)
	}
	fallback, ok := r.graph.retryRoutes[current]
	if !ok {
		err := fmt.Errorf("flowy: node %q returned Retry without AddRetryRoute", current)
		return r.terminalFailDirectiveStep(runCtx, sink, current, state, meta, effects, err)
	}
	return continueDirectiveStep[T, E](fallback)
}

func (r *graphRunner[T, E]) terminalFailDirectiveStep(
	runCtx context.Context,
	sink eventSink[T, E],
	current string,
	state T,
	meta RunMetadata,
	effects []E,
	err error,
) directiveStep[T, E] {
	emitTerminalEvent(runCtx, sink, newRunEventFailed[T, E](current, state, err, err.Error()))
	return terminalDirectiveStep(
		failedResultWithReason(state, effects, meta, current, err.Error()),
		err,
	)
}

// failReasonForStepErr aligns RunResult.Reason with EventFailed.Reason for node-step errors.
func failReasonForStepErr(err error) string {
	if err == nil {
		return ""
	}
	if u := errors.Unwrap(err); u != nil && strings.HasPrefix(err.Error(), "flowy: node ") {
		return u.Error()
	}
	return err.Error()
}

func failedResultWithReason[T, E any](
	state T,
	effects []E,
	meta RunMetadata,
	pointer, reason string,
) *RunResult[T, E] {
	return newRunResultFailed(state, effects, meta, pointer, reason)
}

func markSegmentFailed(meta *RunMetadata) {
	now := time.Now().UTC()
	meta.Segment.EndTime = now
	meta.Segment.EndReason = SegmentEndFail
}

func (r *graphRunner[T, E]) resolveEdge(
	ctx context.Context,
	current string,
	state T,
) (string, error) {
	if next, ok := r.graph.edges[current]; ok {
		return next, nil
	}

	router, ok := r.graph.conditionalEdges[current]
	if !ok {
		return "", fmt.Errorf("flowy: node %q returned Completed but has no outgoing edge", current)
	}

	next, err := router(ctx, state)
	if err != nil {
		return "", fmt.Errorf("flowy: conditional edge from %q: %w", current, err)
	}
	if next == "" {
		return "", fmt.Errorf("flowy: conditional edge from %q returned empty target", current)
	}
	if allowed, ok := r.graph.conditionalAllowed[current]; ok {
		if _, declared := allowed[next]; !declared {
			return "", fmt.Errorf(
				"flowy: conditional edge from %q returned undeclared target %q",
				current, next,
			)
		}
	}
	if next == EndNode {
		return next, nil
	}
	if _, ok := r.graph.nodes[next]; !ok {
		return "", fmt.Errorf(
			"flowy: conditional edge from %q returned unknown node %q",
			current,
			next,
		)
	}
	return next, nil
}

func (r *graphRunner[T, E]) validateExecutionPointer(ptr ExecutionPointer) error {
	if ptr == "" {
		return ErrInvalidSnapshot
	}
	node := string(ptr)
	if _, ok := r.graph.nodes[node]; !ok {
		return invalidResumePointerError(node)
	}
	return nil
}

func invalidResumePointerError(node string) error {
	return errors.Join(ErrInvalidSnapshot, fmt.Errorf("%w: %q", ErrResumeStartNodeNotFound, node))
}

func validateInvariant[T, E any](state T, inv runInvocationOptions[T, E]) error {
	if inv.invariantValidator == nil {
		return nil
	}
	if err := inv.invariantValidator(state); err != nil {
		return fmt.Errorf("flowy: invariant violated: %w", err)
	}
	return nil
}
