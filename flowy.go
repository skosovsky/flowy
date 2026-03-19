package flowy

import (
	"context"
	"errors"
)

// Node is the basic computation unit. It receives the current state and returns
// the updated state (or a delta to be merged by the Reducer).
type Node[T any] func(ctx context.Context, state T) (T, error)

// NodeHandler is the standard node signature; it is the same as Node and used for clarity in middleware contracts.
type NodeHandler[T any] = Node[T]

// ConditionalEdge is a routing function that decides which node to execute next.
type ConditionalEdge[T any] func(ctx context.Context, state T) (string, error)

// Reducer defines how to merge the current state with the update returned by a node.
type Reducer[T any] func(current T, update T) T

// MiddlewareExecutionKind describes what kind of executable step invoked the middleware.
type MiddlewareExecutionKind string

const (
	// MiddlewareExecutionNode is a normal sequential executable node.
	MiddlewareExecutionNode MiddlewareExecutionKind = "node"
	// MiddlewareExecutionFanOutBranch is a fan-out target node executing as one branch of a routing step.
	MiddlewareExecutionFanOutBranch MiddlewareExecutionKind = "fan_out_branch"
)

// MiddlewareContext carries step-level metadata for a middleware invocation.
// Successful middleware returns are still treated as node updates and will be passed
// through the reducer by the runner. If a middleware returns ErrSuspend, its returned
// state is treated as the snapshot to give back to the caller. The contract is
// fully step-level for sequential executable nodes; fan-out branches expose only
// branch-local metadata and do not support generic ResolveNext-based checkpointing.
type MiddlewareContext[T any] struct {
	NodeName       string
	SuspendTarget  string
	ExecutionKind  MiddlewareExecutionKind
	CanResolveNext bool
	IsFinish       bool
	ApplyUpdate    func(current T, update T) T
	ResolveNext    func(ctx context.Context, postState T) (string, error)
}

// Middleware is an interceptor that wraps node execution. It receives ctx, state,
// step metadata, and the next handler. Use it for logging, tracing, metrics,
// fallback behavior, and persistence.
type Middleware[T any] func(ctx context.Context, state T, meta MiddlewareContext[T], next NodeHandler[T]) (T, error)

// DynamicRouter decides at runtime which nodes should be executed in parallel (dynamic fan-out).
// Returned node names must be registered nodes.
type DynamicRouter[T any] func(ctx context.Context, state T) ([]string, error)

// Step describes the graph state after one graph step, yielded by Stream and ResumeStream.
// Each successful step produces one Step; on ErrSuspend the iterator yields one final
// Step with the saved state and the resumable node or routing label, then stops.
type Step[T any] struct {
	State    T      // State after the node run
	NodeName string // Executed node name, or the resume target on ErrSuspend
}

// Sentinel errors for flow control and validation.
var (
	// ErrSuspend is returned when a node or middleware suspends execution
	// (e.g. human-in-the-loop). Invoke returns the saved state snapshot and ErrSuspend.
	ErrSuspend = errors.New("flowy: suspend execution")
	// ErrMaxStepsExceeded is returned when the step limit is reached (e.g. infinite loop).
	ErrMaxStepsExceeded = errors.New("flowy: max steps exceeded")
)
