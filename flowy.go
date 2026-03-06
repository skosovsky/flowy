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

// Middleware is an interceptor that wraps node execution. It receives ctx, state, node name, and the next handler.
// Use it for logging, tracing, or metrics without changing business logic.
type Middleware[T any] func(ctx context.Context, state T, nodeName string, next NodeHandler[T]) (T, error)

// DynamicRouter decides at runtime which nodes should be executed in parallel (dynamic fan-out).
// Returned node names must be registered nodes.
type DynamicRouter[T any] func(ctx context.Context, state T) ([]string, error)

// Step describes the graph state after one node execution, yielded by Stream and ResumeStream.
// Each successful node run produces one Step; the iterator stops after an error or ErrSuspend.
type Step[T any] struct {
	State    T      // State after the node run
	NodeName string // Name of the node that just completed
}

// Sentinel errors for flow control and validation.
var (
	// ErrSuspend is returned when a node suspends execution (e.g. human-in-the-loop). Invoke returns (state, checkpoint, ErrSuspend).
	ErrSuspend = errors.New("flowy: suspend execution")
	// ErrMaxStepsExceeded is returned when the step limit is reached (e.g. infinite loop).
	ErrMaxStepsExceeded = errors.New("flowy: max steps exceeded")
)
