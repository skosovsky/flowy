package flowy

import (
	"context"
	"time"
)

const defaultMaxSteps = 1000

// runConfig holds options for a run (set at Compile from BuildOption).
type runConfig struct {
	maxSteps       int
	nodeTimeout    time.Duration
	maxConcurrency int
}

// buildOpts holds compile-time options: run config and optional middlewares.
type buildOpts[T any] struct {
	run         runConfig
	middlewares []Middleware[T]
}

// BuildOption configures the graph at compile time. Generic over state type T so WithMiddleware can be typed.
type BuildOption[T any] func(*buildOpts[T])

// WithMaxSteps sets the maximum number of steps (prevents infinite loops). Default is 1000 if <= 0.
func WithMaxSteps[T any](limit int) BuildOption[T] {
	return func(o *buildOpts[T]) {
		o.run.maxSteps = limit
	}
}

// WithNodeTimeout sets a timeout for each node execution.
func WithNodeTimeout[T any](d time.Duration) BuildOption[T] {
	return func(o *buildOpts[T]) {
		o.run.nodeTimeout = d
	}
}

// WithMaxConcurrency sets the maximum number of goroutines during a fan-out. If <= 0, no limit.
func WithMaxConcurrency[T any](n int) BuildOption[T] {
	return func(o *buildOpts[T]) {
		o.run.maxConcurrency = n
	}
}

// WithMiddleware adds a node-level interceptor at compile time. Can be combined with Use() middlewares.
func WithMiddleware[T any](mw Middleware[T]) BuildOption[T] {
	return func(o *buildOpts[T]) {
		o.middlewares = append(o.middlewares, mw)
	}
}

// applyBuildOptions applies opts and returns buildOpts; used in Compile.
func applyBuildOptions[T any](opts []BuildOption[T]) buildOpts[T] {
	o := buildOpts[T]{run: runConfig{maxSteps: defaultMaxSteps}}
	for _, opt := range opts {
		opt(&o)
	}
	if o.run.maxSteps <= 0 {
		o.run.maxSteps = defaultMaxSteps
	}
	return o
}

// nodeContextWithTimeout returns ctx with nodeTimeout if set.
func nodeContextWithTimeout(ctx context.Context, cfg *runConfig) (context.Context, func()) {
	if cfg.nodeTimeout > 0 {
		return context.WithTimeout(ctx, cfg.nodeTimeout)
	}
	return ctx, func() {}
}
