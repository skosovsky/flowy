package flowy

import "time"

const defaultMaxSteps = 1000

// runConfig holds options for a run.
type runConfig struct {
	maxSteps        int
	deleteOnSuccess bool
	retentionLimit  int
	budgetLimits    map[string]int
}

// buildOpts holds compile-time options.
type buildOpts struct {
	run runConfig
}

// BuildOption configures the graph at compile time.
type BuildOption func(*buildOpts)

// WithMaxSteps sets the maximum number of steps (prevents infinite loops). Default is 1000 if <= 0.
func WithMaxSteps(limit int) BuildOption {
	return func(o *buildOpts) {
		o.run.maxSteps = limit
	}
}

// WithNamedBudget declares a named budget limit checked after each step.
func WithNamedBudget(name string, limit int) BuildOption {
	return func(o *buildOpts) {
		if o.run.budgetLimits == nil {
			o.run.budgetLimits = make(map[string]int)
		}
		o.run.budgetLimits[name] = limit
	}
}

// WithDeleteOnSuccess deletes thread checkpoint on successful completion.
func WithDeleteOnSuccess(enabled bool) BuildOption {
	return func(o *buildOpts) {
		o.run.deleteOnSuccess = enabled
	}
}

// WithRetentionLimit retains at most N snapshots per thread (0 disables auto-prune).
func WithRetentionLimit(limit int) BuildOption {
	return func(o *buildOpts) {
		o.run.retentionLimit = limit
	}
}

// applyBuildOptions applies opts and returns buildOpts; used in Compile.
func applyBuildOptions(opts []BuildOption) buildOpts {
	o := buildOpts{run: runConfig{maxSteps: defaultMaxSteps}}
	for _, opt := range opts {
		opt(&o)
	}
	if o.run.maxSteps <= 0 {
		o.run.maxSteps = defaultMaxSteps
	}
	return o
}

// RunnerOption configures runner-level defaults (lease manager).
type RunnerOption[T, E any] func(*graphRunner[T, E])

// WithLeaseManager attaches a lease manager for exclusive thread execution.
func WithLeaseManager[T, E any](manager LeaseManager) RunnerOption[T, E] {
	return func(r *graphRunner[T, E]) {
		r.leaseManager = manager
	}
}

// WithRunnerHandoffOutbox sets the default outbox for handoff and RecoverStaleHandoff.
func WithRunnerHandoffOutbox[T, E any](outbox HandoffOutbox) RunnerOption[T, E] {
	return func(r *graphRunner[T, E]) {
		r.handoffOutbox = outbox
	}
}

// WithHandoffStaleAfter sets the TTL for stale-pending handoff recovery (default 5m).
func WithHandoffStaleAfter[T, E any](d time.Duration) RunnerOption[T, E] {
	return func(r *graphRunner[T, E]) {
		r.handoffStaleAfter = d
	}
}
