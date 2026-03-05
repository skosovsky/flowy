package flowy

import (
	"context"
	"time"
)

const defaultMaxSteps = 25

// runConfig holds options for a single run (Invoke, Stream, or Resume).
// Options can be set at Compile time (defaults) and overridden per call.
type runConfig[T any] struct {
	threadID       string
	maxSteps       int
	nodeTimeout    time.Duration
	checkpointer   Checkpointer[T]
	maxConcurrency int // max concurrent goroutines in fan-out; <= 0 means no limit
	// Internal: set by runner for invocation middleware and Stream.
	startNode   string          // entry point or resume node
	resumeFirst bool            // skip interruptBefore on first step (Resume)
	eventCh     chan<- Event[T] // stream channel; nil for Invoke/Resume
}

// Option configures a run (Invoke, Stream, Resume) or compile defaults.
type Option[T any] func(*runConfig[T])

// WithThreadID sets the thread/session ID for checkpointing.
func WithThreadID[T any](id string) Option[T] {
	return func(c *runConfig[T]) {
		c.threadID = id
	}
}

// WithMaxSteps sets the maximum number of steps (prevents infinite loops).
func WithMaxSteps[T any](n int) Option[T] {
	return func(c *runConfig[T]) {
		c.maxSteps = n
	}
}

// WithNodeTimeout sets a timeout for each node execution.
func WithNodeTimeout[T any](d time.Duration) Option[T] {
	return func(c *runConfig[T]) {
		c.nodeTimeout = d
	}
}

// WithCheckpointer sets the checkpointer for persistence (required for HITL).
func WithCheckpointer[T any](cp Checkpointer[T]) Option[T] {
	return func(c *runConfig[T]) {
		c.checkpointer = cp
	}
}

// WithMaxConcurrency sets the maximum number of goroutines that can run concurrently
// during a fan-out (both static and dynamic). If n <= 0, there is no limit (default behavior).
func WithMaxConcurrency[T any](n int) Option[T] {
	return func(c *runConfig[T]) {
		c.maxConcurrency = n
	}
}

// withStartNode sets the starting node (internal use by runner for Resume).
func withStartNode[T any](name string) Option[T] {
	return func(c *runConfig[T]) { c.startNode = name }
}

// withResumeFirst skips interruptBefore on the first step (internal use by runner for Resume).
func withResumeFirst[T any](b bool) Option[T] {
	return func(c *runConfig[T]) { c.resumeFirst = b }
}

// withEventCh sets the event channel for Stream (internal use by runner).
func withEventCh[T any](ch chan<- Event[T]) Option[T] {
	return func(c *runConfig[T]) { c.eventCh = ch }
}

// applyOptions merges default config with per-call options.
// defaultCfg can be nil (zero runConfig used as base).
func applyOptions[T any](defaultCfg *runConfig[T], opts []Option[T]) runConfig[T] {
	var c runConfig[T]
	if defaultCfg != nil {
		c = *defaultCfg
	}
	for _, opt := range opts {
		opt(&c)
	}
	if c.maxSteps <= 0 {
		c.maxSteps = defaultMaxSteps
	}
	return c
}

// nodeContextWithTimeout returns ctx unchanged, or a context with nodeTimeout if set.
// When no timeout is set, the second return is a no-op cancel func.
func nodeContextWithTimeout[T any](ctx context.Context, cfg *runConfig[T]) (context.Context, func()) {
	if cfg.nodeTimeout > 0 {
		return context.WithTimeout(ctx, cfg.nodeTimeout)
	}
	return ctx, func() {}
}
