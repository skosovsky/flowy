package flowy

import (
	"context"
	"errors"
)

// Node is the basic computation unit. It receives the current state and returns
// the updated state (or a delta to be merged by the Reducer).
type Node[T any] func(ctx context.Context, state T) (T, error)

// Choice is a routing function that decides which node to execute next (single next node name).
type Choice[T any] func(ctx context.Context, state T) (string, error)

// Reducer defines how to merge the current state with the update returned by a node.
type Reducer[T any] func(current T, update T) T

// ExecutionChain carries step metadata and drives the middleware pipeline for one node execution.
// Call Next to run the next middleware or, after the last middleware, the node handler with
// the same timeout semantics as the runner. Middleware must not retain *ExecutionChain after
// the function returns (no async goroutines holding the pointer).
//
// Successful middleware returns are still treated as node updates and will be passed through
// the reducer by the runner. If a middleware returns ErrSuspend, it must return the full
// snapshot state for the caller.
type ExecutionChain[T any] struct {
	NodeName      string
	SuspendTarget string
	IsFinish      bool

	g           *Graph[T]
	cfg         *runConfig
	middlewares []Middleware[T]
	index       int
	handler     Node[T]
}

// ApplyUpdate merges an update into the current state using the graph reducer.
func (c *ExecutionChain[T]) ApplyUpdate(current, update T) T {
	return c.g.reducer(current, update)
}

// ResolveNext returns the next routing target after the current node, or an error if unavailable.
func (c *ExecutionChain[T]) ResolveNext(ctx context.Context, postState T) (string, error) {
	if c.g.finishPoints[c.NodeName] {
		return "", nil
	}
	return c.g.resolveNext(ctx, c.NodeName, postState)
}

// Next runs the next middleware in the pipeline, or the node handler if all middleware have run.
// Middleware order matches the historical onion: first registered global middleware runs first on
// the way in (same order as compiledMiddlewares slice: globals then per-node, each in registration order).
func (c *ExecutionChain[T]) Next(ctx context.Context, state T) (T, error) {
	if c.index < len(c.middlewares) {
		mw := c.middlewares[c.index]
		c.index++
		return mw(ctx, state, c)
	}
	nodeCtx, cancel := nodeContextWithTimeout(ctx, c.cfg)
	defer cancel()
	out, err := c.handler(nodeCtx, state)
	if errors.Is(err, ErrSuspend) {
		return out, ErrSuspend
	}
	return out, err
}

// Middleware is an interceptor that wraps node execution. It receives ctx, state, and a chain.
// To continue the pipeline, call chain.Next(ctx, state). Use it for logging, tracing, metrics,
// and fallback behavior. Persistence belongs outside the core (see Stream steps).
type Middleware[T any] func(ctx context.Context, state T, chain *ExecutionChain[T]) (T, error)

// Step describes the graph state after one graph step, yielded by Stream.
//
// On success, each yielded step has State, NodeName, and NextNode populated consistently.
//
// On ErrSuspend, the final yielded step has a full snapshot State, NodeName set to the suspending
// node, and NextNode set to the resume cursor; Stream then stops with ErrSuspend.
//
// On other errors, the iterator may yield a zero Step or a partially filled Step depending on
// where the failure occurred; callers should not rely on Step fields except for ErrSuspend.
type Step[T any] struct {
	State    T
	NodeName string // Last executed node
	NextNode string // Next node to run; empty when the graph stops at a finish point
}

// Sentinel errors for flow control and validation.
var (
	// ErrSuspend is returned when a node or middleware suspends execution
	// (e.g. human-in-the-loop). The suspending node or middleware must return the
	// full snapshot state; Invoke returns that state together with ErrSuspend.
	ErrSuspend = errors.New("flowy: suspend execution")
	// ErrMaxStepsExceeded is returned when the step limit is reached (e.g. infinite loop).
	ErrMaxStepsExceeded = errors.New("flowy: max steps exceeded")
)
