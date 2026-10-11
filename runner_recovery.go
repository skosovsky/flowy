package flowy

import (
	"context"
	"errors"
	"fmt"
	"time"
)

func (r *graphRunner[T, E]) EvaluateResume(
	ctx context.Context,
	token ResumeToken,
	opts ...RunOption[T, E],
) (ResumeDecision[T, E], error) {
	if r.checkpointer == nil {
		var zero ResumeDecision[T, E]
		return zero, errors.New("flowy: checkpointer is required for EvaluateResume")
	}
	inv, optErr := r.resolveRunOptions(opts...)
	if optErr != nil {
		var zero ResumeDecision[T, E]
		return zero, optErr
	}
	return r.evaluateResume(ctx, token, inv)
}

func (r *graphRunner[T, E]) EvaluateHandoffRecovery(
	ctx context.Context,
	threadID string,
	opts ...RecoverStaleHandoffOption,
) (ResumeDecision[T, E], error) {
	if r.checkpointer == nil {
		var zero ResumeDecision[T, E]
		return zero, errors.New("flowy: checkpointer is required for EvaluateHandoffRecovery")
	}
	cfg := r.recoverStaleHandoffConfig(opts...)
	if cfg.staleAfter <= 0 {
		cfg.staleAfter = DefaultHandoffStaleAfter
	}
	if threadID == "" {
		var zero ResumeDecision[T, E]
		zero.Status = ResumeDecisionInvalidToken
		zero.Reason = "empty_thread_id"
		zero.Err = ErrInvalidResumeToken
		return zero, ErrInvalidResumeToken
	}
	snapshot, revision, err := r.loadNormalizedSnapshot(ctx, threadID)
	if err != nil {
		decision := newResumeDecision(
			resumeDecisionStatusForError(err),
			ResumeToken{ThreadID: threadID, SnapshotRevision: revision},
			snapshot,
			revision,
			snapshot.ExecutionPointer,
			resumeDecisionReasonForError(err),
			err,
		)
		decision.ThreadID = threadID
		return decision, err
	}
	if ptrErr := r.validateExecutionPointer(snapshot.ExecutionPointer); ptrErr != nil {
		reason := resumeReasonInvalidPointer
		if snapshot.ExecutionPointer == "" {
			reason = string(ResumeDecisionInvalidSnapshot)
		}
		decision := newResumeDecision(
			ResumeDecisionInvalidSnapshot,
			ResumeToken{ThreadID: threadID, SnapshotRevision: revision},
			snapshot,
			revision,
			snapshot.ExecutionPointer,
			reason,
			ptrErr,
		)
		return decision, ptrErr
	}
	decision := newResumeDecision(
		ResumeDecisionHandoffNotRecoverable,
		ResumeToken{ThreadID: threadID, SnapshotRevision: revision},
		snapshot,
		revision,
		snapshot.ExecutionPointer,
		"handoff_not_recoverable",
		ErrHandoffNotRecoverable,
	)
	switch snapshot.RunMeta.HandoffStatus {
	case HandoffStatusOrphaned:
		decision.Status = ResumeDecisionHandoffRecoverable
		decision.Reason = ReasonHandoffOrphaned
		decision.Err = ErrHandoffOrphaned
	case HandoffStatusPending:
		if isHandoffPendingStale(snapshot.RunMeta.HandoffPendingAt, cfg.staleAfter) {
			decision.Status = ResumeDecisionHandoffRecoverable
			decision.Reason = "handoff_pending_stale"
			decision.Err = ErrHandoffPending
		} else {
			emitResumeRejected(ctx, threadID, snapshot.ExecutionPointer, string(ResumeDecisionHandoffPending))
			decision.Status = ResumeDecisionHandoffPending
			decision.Reason = string(ResumeDecisionHandoffPending)
			decision.Err = ErrHandoffPending
		}
	case HandoffStatusEnqueued:
		if cfg.forceReenqueue {
			decision.Status = ResumeDecisionHandoffRecoverable
			decision.Reason = "handoff_force_reenqueue"
			decision.Err = nil
		} else {
			decision.Status = ResumeDecisionHandoffAlreadyScheduled
			decision.Reason = "handoff_already_enqueued"
			decision.Err = ErrHandoffAlreadyEnqueued
		}
	case HandoffStatusNone:
		decision.Status = ResumeDecisionHandoffNotRecoverable
		decision.Reason = "handoff_status_none"
		decision.Err = ErrHandoffNotRecoverable
	default:
		decision.Status = ResumeDecisionHandoffNotRecoverable
		decision.Reason = resumeReasonInvalidHandoffStatus
		decision.Err = errors.Join(
			ErrHandoffNotRecoverable,
			fmt.Errorf("%w: handoff status %q", ErrInvalidSnapshot, snapshot.RunMeta.HandoffStatus),
		)
	}
	if decision.Err != nil {
		return decision, decision.Err
	}
	return decision, nil
}

//nolint:gocognit,funlen // resume validation pipeline is intentionally linear
func (r *graphRunner[T, E]) evaluateResume(
	ctx context.Context,
	token ResumeToken,
	inv runInvocationOptions[T, E],
) (ResumeDecision[T, E], error) {
	if token.ThreadID == "" {
		emitResumeRejected(ctx, token.ThreadID, "", "empty_token")
		err := fmt.Errorf("%w: empty thread ID", ErrInvalidResumeToken)
		decision := newResumeDecision(
			ResumeDecisionInvalidToken,
			token,
			Snapshot[T, E]{
				ThreadID:         "",
				ExecutionPointer: "",
				Revision:         0,
				State:            *new(T),
				RunMeta:          newRunMetadata(),
				Effects:          nil,
			},
			0,
			"",
			"empty_token",
			err,
		)
		return decision, decision.Err
	}
	// 1) Load base snapshot
	snapshot, revision, err := r.loadNormalizedSnapshot(ctx, token.ThreadID)
	if err != nil {
		if errors.Is(err, ErrInvalidSnapshot) {
			emitResumeRejected(ctx, token.ThreadID, snapshot.ExecutionPointer, "invalid_snapshot")
		}
		decision := newResumeDecision(
			resumeDecisionStatusForError(err),
			token,
			snapshot,
			revision,
			snapshot.ExecutionPointer,
			resumeDecisionReasonForError(err),
			err,
		)
		decision.ThreadID = token.ThreadID
		return decision, err
	}
	if token.SnapshotRevision == 0 {
		emitResumeRejected(ctx, token.ThreadID, snapshot.ExecutionPointer, "zero_revision")
		decision := newResumeDecision(
			ResumeDecisionInvalidToken, token, snapshot, revision, snapshot.ExecutionPointer,
			"zero_revision", ErrConcurrencyConflict,
		)
		return decision, decision.Err
	}
	if token.SnapshotRevision != revision {
		emitResumeRejected(ctx, token.ThreadID, snapshot.ExecutionPointer, "stale_token")
		err := fmt.Errorf(
			"%w: resume token snapshot revision %d, snapshot revision %d",
			ErrConcurrencyConflict, token.SnapshotRevision, revision,
		)
		decision := newResumeDecision(
			ResumeDecisionStaleToken,
			ResumeToken{ThreadID: token.ThreadID, SnapshotRevision: revision},
			snapshot,
			revision,
			snapshot.ExecutionPointer,
			"stale_token", err,
		)
		return decision, err
	}
	switch snapshot.RunMeta.HandoffStatus {
	case HandoffStatusNone, HandoffStatusEnqueued:
		// Resume allowed after handoff enqueue completed or for non-handoff snapshots.
	case HandoffStatusPending:
		status := ResumeDecisionHandoffPending
		reason := string(ResumeDecisionHandoffPending)
		if isHandoffPendingStale(snapshot.RunMeta.HandoffPendingAt, r.handoffStaleAfter) {
			status = ResumeDecisionHandoffRecoverable
			reason = "handoff_pending_stale"
		}
		emitResumeRejected(ctx, token.ThreadID, snapshot.ExecutionPointer, reason)
		decision := newResumeDecision(
			status,
			token,
			snapshot,
			revision,
			snapshot.ExecutionPointer,
			reason,
			ErrHandoffPending,
		)
		return decision, decision.Err
	case HandoffStatusOrphaned:
		emitResumeRejected(ctx, token.ThreadID, snapshot.ExecutionPointer, ReasonHandoffOrphaned)
		decision := newResumeDecision(
			ResumeDecisionHandoffRecoverable, token, snapshot, revision, snapshot.ExecutionPointer,
			ReasonHandoffOrphaned, ErrHandoffOrphaned,
		)
		return decision, decision.Err
	default:
		if snapshot.RunMeta.HandoffStatus != HandoffStatusNone {
			emitResumeRejected(ctx, token.ThreadID, snapshot.ExecutionPointer, resumeReasonInvalidHandoffStatus)
			err := fmt.Errorf(
				"%w: handoff status %q",
				ErrInvalidSnapshot,
				snapshot.RunMeta.HandoffStatus,
			)
			decision := newResumeDecision(
				ResumeDecisionInvalidSnapshot, token, snapshot, revision, snapshot.ExecutionPointer,
				resumeReasonInvalidHandoffStatus, err,
			)
			return decision, err
		}
	}
	state := snapshot.State
	for _, interceptor := range r.interceptors {
		if loadErr := interceptor.AfterLoad(ctx, &state); loadErr != nil {
			err := fmt.Errorf(
				"flowy: after_load interceptor: %w",
				loadErr,
			)
			decision := newResumeDecision(
				ResumeDecisionInvalidSnapshot, token, snapshot, revision, snapshot.ExecutionPointer,
				"after_load_failed", err,
			)
			return decision, err
		}
	}

	// 2) Overlay merge
	if inv.overlay != nil {
		if inv.overlayMerger == nil {
			decision := newResumeDecision(
				ResumeDecisionInvalidSnapshot, token, snapshot, revision, snapshot.ExecutionPointer,
				"overlay_merger_required", ErrOverlayMergerRequired,
			)
			return decision, decision.Err
		}
		state = inv.overlayMerger(state, *inv.overlay)
	}
	meta := snapshot.RunMeta
	if meta.HandoffStatus == HandoffStatusEnqueued {
		meta.HandoffStatus = HandoffStatusNone
		meta.HandoffPendingAt = time.Time{}
	}
	if meta.RetryCounts == nil {
		meta.RetryCounts = map[string]int{}
	}
	resetSegmentCounters(&meta)
	mergeRunMetadataInput(&meta, inv.runMetadata)

	activePointer := snapshot.ExecutionPointer
	var policyErr error
	state, activePointer, policyErr = applyResumeTargetPolicy(ctx, state, activePointer, inv)
	if policyErr != nil {
		decision := newResumeDecision(
			ResumeDecisionInvalidSnapshot, token, snapshot, revision, activePointer,
			"resume_target_policy_failed", policyErr,
		)
		decision.State = state
		decision.RunMeta = meta
		return decision, policyErr
	}

	if activePointer == "" {
		emitResumeRejected(ctx, token.ThreadID, snapshot.ExecutionPointer, "invalid_snapshot")
		decision := newResumeDecision(
			ResumeDecisionInvalidSnapshot, token, snapshot, revision, activePointer,
			"invalid_snapshot", ErrInvalidSnapshot,
		)
		decision.State = state
		decision.RunMeta = meta
		return decision, decision.Err
	}
	startNode := string(activePointer)

	if _, ok := r.graph.nodes[startNode]; !ok {
		emitResumeRejected(ctx, token.ThreadID, activePointer, resumeReasonInvalidPointer)
		err := invalidResumePointerError(startNode)
		decision := newResumeDecision(
			ResumeDecisionInvalidSnapshot, token, snapshot, revision, activePointer,
			resumeReasonInvalidPointer, err,
		)
		decision.State = state
		decision.RunMeta = meta
		return decision, err
	}

	decision := newResumeDecision(
		ResumeDecisionReady, token, snapshot, revision, activePointer,
		string(ResumeDecisionReady), nil,
	)
	decision.State = state
	decision.RunMeta = meta
	decision.Effects = append([]E(nil), snapshot.Effects...)
	return decision, nil
}

func resetSegmentCounters(meta *RunMetadata) {
	meta.Segment = newSegmentInfo()
	meta.SegmentStartTime = time.Now().UTC()
	meta.StepCount = 0
	if meta.BudgetCounts == nil {
		meta.BudgetCounts = map[string]int{}
	}
}

func newResumeDecision[T, E any](
	status ResumeDecisionStatus,
	token ResumeToken,
	snapshot Snapshot[T, E],
	revision uint64,
	pointer ExecutionPointer,
	reason string,
	err error,
) ResumeDecision[T, E] {
	return ResumeDecision[T, E]{
		Status:           status,
		ThreadID:         snapshot.ThreadID,
		ResumeToken:      token,
		Snapshot:         snapshot,
		SnapshotRevision: revision,
		ExecutionPointer: pointer,
		HandoffStatus:    snapshot.RunMeta.HandoffStatus,
		Reason:           reason,
		Err:              err,
		State:            snapshot.State,
		RunMeta:          snapshot.RunMeta,
		Effects:          append([]E(nil), snapshot.Effects...),
	}
}

func resumeDecisionStatusForError(err error) ResumeDecisionStatus {
	switch {
	case errors.Is(err, ErrThreadNotFound):
		return ResumeDecisionThreadNotFound
	case errors.Is(err, ErrInvalidSnapshot), errors.Is(err, ErrSnapshotEnvelopeInvalid):
		return ResumeDecisionInvalidSnapshot
	default:
		return ResumeDecisionLoadFailed
	}
}

func resumeDecisionReasonForError(err error) string {
	switch {
	case errors.Is(err, ErrThreadNotFound):
		return string(ResumeDecisionThreadNotFound)
	case errors.Is(err, ErrInvalidSnapshot), errors.Is(err, ErrSnapshotEnvelopeInvalid):
		return string(ResumeDecisionInvalidSnapshot)
	default:
		return string(ResumeDecisionLoadFailed)
	}
}

func normalizeLoadedSnapshot[T, E any](
	threadID string,
	snapshot Snapshot[T, E],
	revision uint64,
) (Snapshot[T, E], error) {
	if threadID == "" {
		return snapshot, fmt.Errorf("%w: empty thread ID", ErrInvalidSnapshot)
	}
	if snapshot.ThreadID == "" {
		snapshot.ThreadID = threadID
	} else if snapshot.ThreadID != threadID {
		return snapshot, fmt.Errorf(
			"%w: snapshot thread %q != requested thread %q",
			ErrInvalidSnapshot,
			snapshot.ThreadID,
			threadID,
		)
	}
	if revision == 0 {
		return snapshot, fmt.Errorf("%w: zero snapshot revision", ErrInvalidSnapshot)
	}
	if snapshot.Revision == 0 {
		snapshot.Revision = revision
	} else if snapshot.Revision != revision {
		return snapshot, fmt.Errorf(
			"%w: snapshot revision %d != storage revision %d",
			ErrInvalidSnapshot,
			snapshot.Revision,
			revision,
		)
	}
	if snapshot.ExecutionPointer == "" {
		return snapshot, fmt.Errorf("%w: empty execution pointer", ErrInvalidSnapshot)
	}
	return snapshot, nil
}

func (r *graphRunner[T, E]) loadNormalizedSnapshot(
	ctx context.Context,
	threadID string,
) (Snapshot[T, E], uint64, error) {
	snapshot, revision, err := r.checkpointer.Load(ctx, threadID)
	if err != nil {
		if errors.Is(err, ErrThreadNotFound) {
			return snapshot, revision, err
		}
		if errors.Is(err, ErrSnapshotEnvelopeInvalid) || errors.Is(err, ErrInvalidSnapshot) {
			return snapshot, revision, ensureSnapshotEnvelopeInvalid(err)
		}
		return snapshot, revision, err
	}
	normalized, normErr := normalizeLoadedSnapshot(threadID, snapshot, revision)
	if normErr != nil {
		return normalized, revision, ensureSnapshotEnvelopeInvalid(normErr)
	}
	return normalized, revision, nil
}

func ensureSnapshotEnvelopeInvalid(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, ErrSnapshotEnvelopeInvalid) {
		return err
	}
	return fmt.Errorf("%w: %w", ErrSnapshotEnvelopeInvalid, err)
}
