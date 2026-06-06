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
//  1. Checkpointer.Load → ExecutionPointer from snapshot
//  2. StateInterceptor.AfterLoad (optional)
//  3. WithStateOverlay (optional)
//  4. resetSegmentCounters (StepCount/Segment; BudgetCounts preserved)
//  5. WithRunMetadata merge (optional)
//  6. ResumableState.Reconcile (optional)
//  7. execute from ExecutionPointer
//
// DeleteIfIdle and delete-on-success run after execute and releaseLease
// (postRunCleanup). Prune (retention) runs in-loop on suspend/handoff/cancel
// before releaseLease.
//
// Typed bindings use BindingKey[T] + Bind/Extract (not persisted in Snapshot).
// Lease-aware delete uses Checkpointer.DeleteIfIdle; pair postgres/redis
// checkpointer with adapters/lease for distributed deployments.
package flowy
