package flowy

import (
	"context"
	"errors"
	"time"
)

// StateMerger merges snapshot base state with invocation overlay deterministically.
type StateMerger[T any] func(base, overlay T) T

// ResumeReconciler remaps resume start node after overlay merge.
type ResumeReconciler[T, E any] func(snapshot Snapshot[T, E]) (startNode string, err error)

// InvariantValidator validates state after reducer and before checkpoint save.
type InvariantValidator[T any] func(state T) error

// RunOption configures a single Start/Resume/Stream invocation.
type RunOption[T, E any] interface {
	apply(*runInvocationOptions[T, E])
}

type runInvocationOptions[T, E any] struct {
	bindings           *RunBindings
	overlay            *T
	overlayMerger      StateMerger[T]
	reconciler         ResumeReconciler[T, E]
	invariantValidator InvariantValidator[T]
	leaseOwner         string
	leaseTTL           time.Duration
}

type runOptionFunc[T, E any] func(*runInvocationOptions[T, E])

//nolint:unused // invoked via RunOption interface in applyRunOptions
func (f runOptionFunc[T, E]) apply(opts *runInvocationOptions[T, E]) {
	f(opts)
}

// WithBindings injects ephemeral dependencies for this invocation (not persisted).
func WithBindings[T, E any](bindings *RunBindings) RunOption[T, E] {
	return runOptionFunc[T, E](func(opts *runInvocationOptions[T, E]) {
		opts.bindings = bindings
	})
}

// WithStateOverlay merges overlay into loaded state using merger before resume execution.
func WithStateOverlay[T, E any](overlay T, merger StateMerger[T]) RunOption[T, E] {
	return runOptionFunc[T, E](func(opts *runInvocationOptions[T, E]) {
		opts.overlay = &overlay
		opts.overlayMerger = merger
	})
}

// ErrOverlayMergerRequired is returned when overlay is set without a merger.
var ErrOverlayMergerRequired = errors.New("flowy: WithStateOverlay requires a non-nil merger")

// WithResumeReconciler remaps start node after overlay merge.
func WithResumeReconciler[T, E any](fn ResumeReconciler[T, E]) RunOption[T, E] {
	return runOptionFunc[T, E](func(opts *runInvocationOptions[T, E]) {
		opts.reconciler = fn
	})
}

// WithInvariantValidator validates state after each reducer application.
func WithInvariantValidator[T, E any](fn InvariantValidator[T]) RunOption[T, E] {
	return runOptionFunc[T, E](func(opts *runInvocationOptions[T, E]) {
		opts.invariantValidator = fn
	})
}

// WithRunLease sets lease owner id for this invocation (requires LeaseManager on runner).
func WithRunLease[T, E any](owner string, ttl time.Duration) RunOption[T, E] {
	return runOptionFunc[T, E](func(opts *runInvocationOptions[T, E]) {
		opts.leaseOwner = owner
		opts.leaseTTL = ttl
	})
}

func applyRunOptions[T, E any](opts ...RunOption[T, E]) runInvocationOptions[T, E] {
	var out runInvocationOptions[T, E]
	for _, opt := range opts {
		if opt != nil {
			opt.apply(&out)
		}
	}
	return out
}

// UseBudget records consumption of a named budget in run metadata.
func UseBudget(ctx context.Context, name string, amount int) error {
	meta, ok := runMetadataFromContext(ctx)
	if !ok {
		return nil
	}
	if amount < 0 {
		return nil
	}
	if meta.BudgetCounts == nil {
		meta.BudgetCounts = map[string]int{}
	}
	meta.BudgetCounts[name] += amount
	return nil
}

type runMetadataContextKey struct{}

func withRunMetadata(ctx context.Context, meta *RunMetadata) context.Context {
	return context.WithValue(ctx, runMetadataContextKey{}, meta)
}

func runMetadataFromContext(ctx context.Context) (*RunMetadata, bool) {
	meta, ok := ctx.Value(runMetadataContextKey{}).(*RunMetadata)
	return meta, ok
}
