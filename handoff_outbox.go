package flowy

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// DefaultHandoffStaleAfter is the default TTL for stale pending handoff recovery.
const DefaultHandoffStaleAfter = 5 * time.Minute

const (
	handoffMetricSuccess           = "success"
	handoffMetricEnqueueFailed     = "enqueue_failed"
	handoffMetricPatchEnqueuedFail = "patch_enqueued_failed"
	handoffMetricPatchOrphanFailed = "patch_orphan_failed"
	handoffMetricSaveFailed        = "save_failed"
	handoffMetricCommitFailed      = "commit_failed"
)

// HandoffOutbox enqueues a resume intent during the 3-phase handoff FSM:
//  1. Save checkpoint with HandoffStatusPending (revision N)
//  2. EnqueueIntent with ResumeToken{revision N}
//  3. patch HandoffStatusEnqueued or HandoffStatusOrphaned (revision N+1)
//
// EnqueueIntent and Save are not atomic; on enqueue OK + patch failure a compensating
// orphan patch is attempted. RunResult.ResumeToken uses the post-patch revision.
// Workers consuming outbox messages should Load the checkpoint and Resume with the
// loaded revision (outbox token revision N may lag behind the stored revision).
type HandoffOutbox interface {
	EnqueueIntent(ctx context.Context, token ResumeToken) error
}

// RecoverStaleHandoffOption configures RecoverStaleHandoff.
type RecoverStaleHandoffOption func(*recoverStaleHandoffOpts)

type recoverStaleHandoffOpts struct {
	staleAfter     time.Duration
	outbox         HandoffOutbox
	forceReenqueue bool
}

// WithRecoverStaleAfter overrides the stale-pending TTL for a single recovery call.
func WithRecoverStaleAfter(d time.Duration) RecoverStaleHandoffOption {
	return func(o *recoverStaleHandoffOpts) {
		o.staleAfter = d
	}
}

// WithRecoverOutbox supplies the outbox for a single recovery call.
func WithRecoverOutbox(outbox HandoffOutbox) RecoverStaleHandoffOption {
	return func(o *recoverStaleHandoffOpts) {
		o.outbox = outbox
	}
}

// WithRecoverForceReenqueue re-enqueues when HandoffStatus is enqueued (reconcile false enqueued).
func WithRecoverForceReenqueue(force bool) RecoverStaleHandoffOption {
	return func(o *recoverStaleHandoffOpts) {
		o.forceReenqueue = force
	}
}

// RecoverStaleHandoff re-enqueues orphaned or stale-pending handoff checkpoints.
func (r *graphRunner[T, E]) RecoverStaleHandoff(
	ctx context.Context,
	threadID string,
	opts ...RecoverStaleHandoffOption,
) error {
	cfg := recoverStaleHandoffOpts{
		staleAfter:     r.handoffStaleAfter,
		outbox:         r.handoffOutbox,
		forceReenqueue: false,
	}
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}
	if cfg.staleAfter <= 0 {
		cfg.staleAfter = DefaultHandoffStaleAfter
	}
	if cfg.outbox == nil {
		return ErrHandoffOutboxRequired
	}
	if threadID == "" {
		return fmt.Errorf("%w: recovery requires threadID", ErrInvalidResumeToken)
	}

	snapshot, revision, err := r.checkpointer.Load(ctx, threadID)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrThreadNotFound, err)
	}
	status := snapshot.RunMeta.HandoffStatus
	pointer := snapshot.ExecutionPointer

	switch status {
	case HandoffStatusEnqueued:
		if !cfg.forceReenqueue {
			return ErrHandoffAlreadyEnqueued
		}
		return r.recoverHandoffEnqueue(ctx, threadID, snapshot, revision, cfg.outbox, HandoffStatusEnqueued)
	case HandoffStatusOrphaned:
		return r.recoverHandoffEnqueue(ctx, threadID, snapshot, revision, cfg.outbox, HandoffStatusEnqueued)
	case HandoffStatusPending:
		if !isHandoffPendingStale(snapshot.RunMeta.HandoffPendingAt, cfg.staleAfter) {
			emitResumeRejected(ctx, threadID, pointer, "handoff_pending")
			return ErrHandoffPending
		}
		return r.recoverHandoffEnqueue(ctx, threadID, snapshot, revision, cfg.outbox, HandoffStatusEnqueued)
	default:
		return fmt.Errorf("%w: snapshot handoff status %q", ErrHandoffNotRecoverable, status)
	}
}

func isHandoffPendingStale(pendingAt time.Time, staleAfter time.Duration) bool {
	if pendingAt.IsZero() {
		return true // legacy rows without timestamp — treat as stale
	}
	return time.Since(pendingAt) > staleAfter
}

type handoffOutboxFSMResult struct {
	ResumeToken        ResumeToken
	EnqueueErr         error
	PatchErr           error
	PersistedStatus    HandoffStatus
	PersistedPendingAt time.Time
}

func (r *graphRunner[T, E]) runHandoffOutboxFSM(
	runCtx context.Context,
	threadID string,
	pendingRev uint64,
	snapshot Snapshot[T, E],
	meta RunMetadata,
	inv runInvocationOptions[T, E],
	outbox HandoffOutbox,
	resumePtr ExecutionPointer,
) handoffOutboxFSMResult {
	enqueueToken := ResumeToken{ThreadID: threadID, SnapshotRevision: pendingRev}
	if enqueueErr := r.enqueueHandoffIntent(runCtx, outbox, enqueueToken); enqueueErr != nil {
		orphanRev, patchErr := r.patchHandoffStatus(
			runCtx, pendingRev, snapshot, meta, HandoffStatusOrphaned, inv,
		)
		metricStatus := handoffMetricEnqueueFailed
		if patchErr != nil {
			metricStatus = handoffMetricPatchOrphanFailed
		}
		emitHandoffEnqueued(runCtx, threadID, resumePtr, metricStatus)
		token := ResumeToken{ThreadID: threadID, SnapshotRevision: pendingRev}
		if patchErr == nil {
			token = ResumeToken{ThreadID: threadID, SnapshotRevision: orphanRev}
		}
		status := HandoffStatusOrphaned
		if patchErr != nil {
			status = HandoffStatusPending
		}
		return handoffOutboxFSMResult{
			ResumeToken:        token,
			EnqueueErr:         enqueueErr,
			PatchErr:           wrapHandoffPatchFailed(patchErr),
			PersistedStatus:    status,
			PersistedPendingAt: persistedHandoffPendingAt(status, meta.HandoffPendingAt),
		}
	}
	enqueuedRev, patchErr := r.patchHandoffStatus(
		runCtx, pendingRev, snapshot, meta, HandoffStatusEnqueued, inv,
	)
	if patchErr != nil { //nolint:nestif // compensating orphan patch branches are intentional FSM paths
		orphanRev, orphanErr := r.patchHandoffStatus(
			runCtx, pendingRev, snapshot, meta, HandoffStatusOrphaned, inv,
		)
		metricStatus := handoffMetricPatchEnqueuedFail
		if orphanErr != nil {
			metricStatus = handoffMetricPatchOrphanFailed
		}
		emitHandoffEnqueued(runCtx, threadID, resumePtr, metricStatus)
		var combined error
		if orphanErr != nil {
			combined = errors.Join(
				wrapHandoffPatchFailed(patchErr),
				wrapHandoffPatchFailed(orphanErr),
			)
		} else {
			combined = wrapHandoffPatchFailed(patchErr)
		}
		token := ResumeToken{ThreadID: threadID, SnapshotRevision: pendingRev}
		if orphanErr == nil {
			token = ResumeToken{ThreadID: threadID, SnapshotRevision: orphanRev}
		}
		status := HandoffStatusOrphaned
		if orphanErr != nil {
			status = HandoffStatusPending
		}
		return handoffOutboxFSMResult{
			ResumeToken:        token,
			EnqueueErr:         nil,
			PatchErr:           combined,
			PersistedStatus:    status,
			PersistedPendingAt: persistedHandoffPendingAt(status, meta.HandoffPendingAt),
		}
	}
	emitHandoffEnqueued(runCtx, threadID, resumePtr, handoffMetricSuccess)
	return handoffOutboxFSMResult{
		ResumeToken:        ResumeToken{ThreadID: threadID, SnapshotRevision: enqueuedRev},
		EnqueueErr:         nil,
		PatchErr:           nil,
		PersistedStatus:    HandoffStatusEnqueued,
		PersistedPendingAt: time.Time{},
	}
}

func (r *graphRunner[T, E]) dispatchPersistedHandoffOutbox(
	runCtx context.Context,
	threadID string,
	pendingRev uint64,
	snapshot Snapshot[T, E],
	meta RunMetadata,
	inv runInvocationOptions[T, E],
	outbox HandoffOutbox,
	resumePtr ExecutionPointer,
	savedPointer string,
	state T,
	_ []E,
	result *RunResult[T, E],
	sink eventSink[T, E],
) (*RunResult[T, E], bool, error) {
	fsm := r.runHandoffOutboxFSM(
		runCtx, threadID, pendingRev, snapshot, meta, inv, outbox, resumePtr,
	)
	if fsm.EnqueueErr != nil || fsm.PatchErr != nil {
		result.ResumeToken = fsm.ResumeToken
		if result.ResumeToken.ThreadID == "" {
			result.ResumeToken = ResumeToken{ThreadID: threadID, SnapshotRevision: pendingRev}
		}
		applyFSMPersistedHandoffMeta(result, fsm)
		result.Reason = deriveHandoffTerminalReason(result.Reason, fsm.PersistedStatus)
		handoffErr := fsm.EnqueueErr
		if handoffErr == nil {
			handoffErr = fsm.PatchErr
		} else if fsm.PatchErr != nil {
			handoffErr = errors.Join(handoffErr, fsm.PatchErr)
		}
		res, err := r.handoffAfterEnqueueFailure(
			runCtx, threadID, savedPointer, state, result, sink, handoffErr,
		)
		return res, true, err
	}
	result.ResumeToken = fsm.ResumeToken
	applyFSMPersistedHandoffMeta(result, fsm)
	return nil, false, nil
}

func clearHandoffRunMeta(meta *RunMetadata) {
	meta.HandoffStatus = HandoffStatusNone
	meta.HandoffPendingAt = time.Time{}
}

func persistedHandoffPendingAt(status HandoffStatus, pendingAt time.Time) time.Time {
	if status == HandoffStatusPending {
		return pendingAt
	}
	return time.Time{}
}

func applyFSMPersistedHandoffMeta[T, E any](result *RunResult[T, E], fsm handoffOutboxFSMResult) {
	if result == nil {
		return
	}
	result.RunMeta.HandoffStatus = fsm.PersistedStatus
	result.RunMeta.HandoffPendingAt = fsm.PersistedPendingAt
}

// deriveHandoffTerminalReason maps persisted HandoffStatus to the terminal EventHandoff reason.
// Orphaned status yields ReasonHandoffOrphaned; pending (failed patch) keeps the directive reason.
func deriveHandoffTerminalReason(directiveReason string, status HandoffStatus) string {
	if status == HandoffStatusOrphaned {
		return ReasonHandoffOrphaned
	}
	return directiveReason
}

type transactionalCheckpointerSource[T, E any] interface {
	transactionalCheckpointerInner() (TransactionalCheckpointer[T, E], bool)
}

func resolveTransactionalCheckpointer[T, E any](cp Checkpointer[T, E]) (TransactionalCheckpointer[T, E], bool) {
	if cp == nil {
		return nil, false
	}
	if src, ok := cp.(transactionalCheckpointerSource[T, E]); ok {
		return src.transactionalCheckpointerInner()
	}
	txCP, ok := cp.(TransactionalCheckpointer[T, E])
	return txCP, ok
}

func (r *graphRunner[T, E]) tryTransactionalHandoffSave(
	runCtx context.Context,
	threadID string,
	expectedRevision uint64,
	snapshot Snapshot[T, E],
	meta RunMetadata,
	_ runInvocationOptions[T, E],
	outbox HandoffOutbox,
	result *RunResult[T, E],
) (uint64, bool, error) {
	txCP, ok := resolveTransactionalCheckpointer(r.checkpointer)
	// Silent fallback to 3-phase FSM when the checkpointer does not implement SaveWithOutbox.
	// ErrTransactionalOutboxUnsupported is surfaced only from lease-guard transactional paths.
	if !ok || outbox == nil {
		return 0, false, nil
	}
	meta.HandoffStatus = HandoffStatusEnqueued
	meta.HandoffPendingAt = time.Time{}
	snapshot.RunMeta = meta
	enqueueToken := ResumeToken{ThreadID: threadID, SnapshotRevision: expectedRevision + 1}
	newRev, err := txCP.SaveWithOutbox(runCtx, expectedRevision, snapshot, func(ctx context.Context) error {
		return r.enqueueHandoffIntent(ctx, outbox, enqueueToken)
	})
	if err != nil {
		emitHandoffEnqueued(runCtx, threadID, snapshot.ExecutionPointer, transactionalHandoffMetricStatus(err))
		return 0, true, err
	}
	emitHandoffEnqueued(runCtx, threadID, snapshot.ExecutionPointer, handoffMetricSuccess)
	result.ResumeToken = ResumeToken{ThreadID: threadID, SnapshotRevision: newRev}
	result.RunMeta.HandoffStatus = HandoffStatusEnqueued
	result.RunMeta.HandoffPendingAt = time.Time{}
	return newRev, true, nil
}

func (r *graphRunner[T, E]) recoverHandoffEnqueue(
	ctx context.Context,
	threadID string,
	snapshot Snapshot[T, E],
	expectedRevision uint64,
	outbox HandoffOutbox,
	targetStatus HandoffStatus,
) error {
	var inv runInvocationOptions[T, E]
	fsm := r.runHandoffOutboxFSM(
		ctx, threadID, expectedRevision, snapshot, snapshot.RunMeta,
		inv, outbox,
		snapshot.ExecutionPointer,
	)
	if fsm.EnqueueErr != nil {
		if fsm.PatchErr != nil {
			return errors.Join(fsm.EnqueueErr, fsm.PatchErr)
		}
		return fsm.EnqueueErr
	}
	if fsm.PatchErr != nil {
		return wrapHandoffPatchRecoveryErr(fsm.PatchErr)
	}
	if targetStatus != HandoffStatusEnqueued {
		return fmt.Errorf("flowy: unexpected recovery target status %q", targetStatus)
	}
	return nil
}

func wrapHandoffPatchFailed(err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("%w: %w", ErrHandoffPatchFailed, err)
}

func wrapHandoffPatchRecoveryErr(err error) error {
	if errors.Is(err, ErrConcurrencyConflict) {
		return fmt.Errorf("%w: recovery patch concurrency conflict: %w", ErrHandoffPatchFailed, err)
	}
	return err
}

func transactionalHandoffMetricStatus(err error) string {
	if errors.Is(err, ErrConcurrencyConflict) {
		return handoffMetricSaveFailed
	}
	if errors.Is(err, ErrTransactionalHandoffCommitFailed) {
		return handoffMetricCommitFailed
	}
	return handoffMetricEnqueueFailed
}
