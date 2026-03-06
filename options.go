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

// BuildOption configures the graph at compile time.
type BuildOption func(*runConfig)

// WithMaxSteps sets the maximum number of steps (prevents infinite loops). Default is 1000 if <= 0.
func WithMaxSteps(limit int) BuildOption {
	return func(c *runConfig) {
		c.maxSteps = limit
	}
}

// WithNodeTimeout sets a timeout for each node execution.
func WithNodeTimeout(d time.Duration) BuildOption {
	return func(c *runConfig) {
		c.nodeTimeout = d
	}
}

// WithMaxConcurrency sets the maximum number of goroutines during a fan-out. If <= 0, no limit.
func WithMaxConcurrency(n int) BuildOption {
	return func(c *runConfig) {
		c.maxConcurrency = n
	}
}

// applyBuildOptions returns runConfig from options; used in Compile.
func applyBuildOptions(opts []BuildOption) runConfig {
	c := runConfig{maxSteps: defaultMaxSteps}
	for _, opt := range opts {
		opt(&c)
	}
	if c.maxSteps <= 0 {
		c.maxSteps = defaultMaxSteps
	}
	return c
}

// nodeContextWithTimeout returns ctx with nodeTimeout if set.
func nodeContextWithTimeout(ctx context.Context, cfg *runConfig) (context.Context, func()) {
	if cfg.nodeTimeout > 0 {
		return context.WithTimeout(ctx, cfg.nodeTimeout)
	}
	return ctx, func() {}
}
