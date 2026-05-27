// Package flowy provides a state-agnostic framework for building and running
// agentic state machines with explicit directives and persistent resume.
//
// Nodes return a typed state update and a platform directive:
//
//	func(ctx context.Context, state T) (T, flowy.Directive, error)
//
// The framework owns the lifecycle loop via Runner.Start/Runner.Resume and
// asynchronous Runner.Stream/Runner.StreamResume handles. Streams always emit
// a terminal event (completed/suspended/failed) before channel close.
// Use StreamHandle.Events() to consume, StreamHandle.Close() for early stop,
// and StreamHandle.Done() to await producer completion.
//
// Checkpoint saving is framework-managed on suspend and context cancellation.
package flowy
