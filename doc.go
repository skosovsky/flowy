// Package flowy provides a state-agnostic framework for building and running
// agentic state machines with explicit directives and persistent resume.
//
// Nodes return a typed state update and a platform directive:
//
//	func(ctx context.Context, state T) (T, flowy.Directive, error)
//
// The framework owns the lifecycle loop via Runner.Start/Runner.Resume and
// asynchronous Runner.Stream/Runner.ResumeStream handles. Stream events are best-effort
// progress/telemetry; WaitResult is authoritative for terminal outcome.
//
// Resume preflight pipeline:
//  1. ResumeToken validation (ThreadID required)
//  2. normalized Checkpointer.Load → OCC: token.SnapshotRevision == snapshot.Revision; mismatch or zero revision
//     returns ErrConcurrencyConflict (use WithRunLease for exclusive resume against parallel workers)
//  3. StateInterceptor.AfterLoad (optional)
//  4. WithStateOverlay (optional)
//  5. resetSegmentCounters (StepCount/Segment; BudgetCounts preserved)
//  6. WithRunMetadata merge (optional)
//  7. WithResumeTargetPolicy (optional explicit state-aware ResumePlan)
//  8. validate active ExecutionPointer (non-empty, node exists in graph)
//  9. execute from active ExecutionPointer
//
// Suspend/Handoff save path: flowy.ResumeAt(node) on the directive declares the persisted resume point.
// Handoff Outbox: WithHandoffOutbox passes HandoffIntent to EnqueueIntent.
// The intent carries pending, committed, and canonical resume-token revision fields, so consumers do not
// infer generation semantics from raw revision arithmetic. Workers consuming outbox messages should call
// EvaluateResume first: stale-token decisions return the current core-issued ResumeToken after normalized load.
// When the checkpointer implements TransactionalCheckpointer, handoff uses SaveWithOutbox:
// one TX for checkpoint + EnqueueIntentTx with an explicit transaction handle and
// a core-built HandoffIntent from the checkpointer-reported saved revision.
// Otherwise the 3-phase FSM applies (Save pending → patch enqueued → EnqueueIntent; enqueue
// failure patches orphaned). Normal outbox intents carry the committed enqueued revision.
// checkpoint.Record plus DecodeRecordOptions is the storage-facing envelope contract for adapters:
// decode returns a validated Snapshot or ErrSnapshotEnvelopeInvalid.
// EvaluateResume and EvaluateHandoffRecovery expose the same normalized preflight decisions used by Resume,
// ResumeStream, and RecoverStaleHandoff.
// RecoverStaleHandoff re-enqueues orphaned or stale-pending checkpoints and returns
// HandoffRecoveryResult plus error; use WithRecoverForceReenqueue to force re-enqueue
// of false-enqueued rows. Result.Decision carries typed recovery preflight metadata.
// Recovery cron should be single-leader or protected by an external lock.
// RecoverStaleHandoff itself does not acquire run leases.
// Checkpointer Save/Load use strict OCC (expectedRevision uint64); ErrConcurrencyConflict on conflict.
// LifecycleObserver (SetLifecycleObserver) receives handoff/resume/checkpoint-soft events.
// Soft checkpoint: WithCheckpointErrorPolicy(CheckpointPolicySkipOnSaveError) emits EventCheckpointFailed on
// Stream/ResumeStream without aborting terminal flow; sync Start/Resume have no event sink.
// ResumeToken is set only after a persisted Suspend/Handoff terminal save. When skip-on-save-error
// policy skips persist, terminal reasons use *_checkpoint_skipped suffixes.
//
// Stream consumer RequestStop closes the event sink and cancels the in-flight run context, using the same
// context-canceled terminal save path as parent ctx cancel. RequestLocalHandoff after RequestStop returns
// [ErrNoActiveExecution] because the run already terminated.
// StreamHandle.WaitResult returns the terminal RunResult and error. StreamHandle.Wait returns only the error.
// RequestStop after persisted cancel save returns nil; RequestStop with
// skip-on-save-error policy returns ErrCheckpointSkipped; parent context cancel returns
// [context.Canceled]; retention or enqueue failures return their respective errors.
// Post-save retention (Prune) runs in-loop on suspend/handoff/cancel only when the terminal
// checkpoint was persisted. Retention failure returns terminal success status with a wrapped
// error and reason suffix *_retention_failed (checkpoint already persisted). Stream terminal events use the same
// reason as RunResult (event.Reason == result.Reason on terminal fail, retention failure, and EventFailed).
// Handoff enqueue failure sets ReasonHandoffOrphaned on RunResult/EventHandoff only when the
// compensating patch persisted HandoffStatusOrphaned; otherwise the directive reason is kept.
// ErrHandoffEnqueueFailed may combine with retention error via [errors.Join].
// Cancel save HardFail uses reason context_canceled_save_failed (EventContextCanceled, not EventFailed).
// Handoff/suspend save HardFail uses ReasonHandoffSaveFailed / ReasonSuspendSaveFailed on RunResult and EventFailed.
// ErrThreadAlreadyRunning is returned when Start/Stream/ResumeStream/Resume targets a threadID with an active in-process session (without lease). ErrThreadLeaseBusy is lease-layer only.
// Invalid ResumeAt targets set ReasonHandoffResumeTargetInvalid or ReasonSuspendResumeTargetInvalid.
//
// Dual retention: in-loop Prune failures fail the terminal return; postRunCleanup Prune on
// Completed/Failed only logs a warning (does not change RunResult).
//
// ErrConcurrencyConflict (stale or zero SnapshotRevision), ErrInvalidResumeToken,
// ErrHandoffPending, ErrHandoffOrphaned, ErrHandoffEnqueueFailed, ErrHandoffPatchFailed,
// ErrThreadNotFound are
// returned from Resume/ResumeStream on validation failures.
// ErrCheckpointSkipped is returned from StreamHandle.Wait when consumer RequestStop stops execution
// with skip-on-save-error policy, and from RequestLocalHandoff when persist is skipped.
//
// Stream consumer patterns (see stream_helpers.go):
//
//	CollectEventsAndWait / ConsumeEventsAndWait — safe drain+Wait on a background goroutine.
//	BeginStreamCollect + AwaitStreamCollect — caller triggers RequestStop/handoff, then awaits terminal outcome.
//	AwaitStreamCollectWithSnapshot — loads snapshot after collection; Outcome owns ResumeToken.
//
// Event streams are not a business outcome source; use WaitResult or the returned RunResult.
//
// Node authoring: all I/O and sleeps must respect the node context; loops should select on
// ctx.Done() so RequestStop and parent cancel can unblock blocking nodes. See
// examples/context_deadline for a canonical pattern.
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
// Lease-aware delete uses Checkpointer.DeleteIfIdle; pair native checkpointer
// and lease adapters for distributed deployments.
package flowy
