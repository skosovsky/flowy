// Package flowy provides a state-agnostic framework for building and running
// agentic state machines with explicit directives and persistent resume.
//
// Nodes return a typed state update and a platform directive:
//
//	func(ctx context.Context, state T) (T, flowy.Directive, error)
//
// The framework owns the lifecycle loop via Runner.Start/Runner.Resume and
// asynchronous Runner.Stream/Runner.StreamResume handles. Streams attempt to
// emit a terminal event (completed/suspended/failed/handoff) before channel
// close; if the consumer closed the stream or the buffer is full, the snapshot
// may already be persisted without a delivered event (persist-vs-event semantics).
//
// Resume pipeline (prepareResume order):
//  1. ResumeToken validation (ThreadID required)
//  2. Checkpointer.Load → OCC: token.Generation == snapshot.Revision
//  3. StateInterceptor.AfterLoad (optional)
//  4. WithStateOverlay (optional)
//  5. resetSegmentCounters (StepCount/Segment; BudgetCounts preserved)
//  6. WithRunMetadata merge (optional)
//  7. ResumeReconciler.ReconcileResume (optional pointer rewind)
//  8. validate active ExecutionPointer (non-empty, node exists in graph)
//  9. execute from active (post-reconcile) ExecutionPointer
//
// Suspend/Handoff save path: WithSuspendPointerResolver normalizes ExecutionPointer before Save.
// Handoff Outbox: WithHandoffScheduler schedules ResumeToken after save (snapshot retained on schedule error).
// Soft checkpoint: WithCheckpointErrorPolicy(SoftWarn) emits EventCheckpointFailed on
// Stream/StreamResume without aborting terminal flow; sync Start/Resume have no event sink.
// ResumeToken is set only after a persisted Suspend/Handoff terminal save. When SoftWarn
// skips persist, terminal reasons use *_checkpoint_skipped suffixes.
//
// Post-save retention (Prune) runs on successful suspend/handoff/cancel checkpoint saves.
// Retention failure returns terminal success status with a wrapped error and reason suffix
// *_retention_failed (checkpoint already persisted). Prune is skipped when cancel save
// HardFails (early return) or when only schedule fails before finalize (prune still runs
// on schedule-fail path after persisted handoff save).
//
// ResumeTokenFromSnapshot builds a ResumeToken from a loaded snapshot (Generation = Revision).
// ErrStaleResumeToken, ErrInvalidResumeToken, ErrHandoffScheduleFailed, ErrThreadNotFound are
// returned from Resume/StreamResume on OCC or validation failures.
//
// WithInvariantValidator runs in-loop during execute, not in prepareResume.
//
// DeleteIfIdle and delete-on-success run after execute and releaseLease
// (postRunCleanup). Prune (retention) runs in-loop on suspend/handoff/cancel
// before releaseLease.
//
// Named budgets use UseBudget to record consumption and BudgetUsed to read
// current usage from the active execution context. ContextWithRunMetadata
// provisions a valid context for isolated node execution outside Runner.
//
// Typed bindings use BindingKey[T] + Bind/Extract (not persisted in Snapshot).
// Lease-aware delete uses Checkpointer.DeleteIfIdle; pair postgres/redis
// checkpointer with adapters/lease for distributed deployments.
package flowy
