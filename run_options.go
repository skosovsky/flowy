package flowy

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"time"
)

// StateMerger merges snapshot base state with invocation overlay deterministically.
type StateMerger[T any] func(base, overlay T) T

// ResumeTargetPolicy adjusts state and the resume execution pointer after overlay merge.
// It is explicit per Resume invocation; the runner validates the returned pointer.
type ResumeTargetPolicy[T any] func(
	ctx context.Context,
	state T,
	current ExecutionPointer,
) (T, ResumePlan, error)

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
	resumeTargetPolicy ResumeTargetPolicy[T]
	invariantValidator InvariantValidator[T]
	runMetadata        RunMetadataInput
	leaseOwner         string
	leaseTTL           time.Duration
	handoffOutbox      HandoffOutbox
	checkpointPolicy   CheckpointFailurePolicy
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

// WithResumeTargetPolicy applies an explicit state-aware resume target policy after overlay merge.
func WithResumeTargetPolicy[T, E any](policy ResumeTargetPolicy[T]) RunOption[T, E] {
	return runOptionFunc[T, E](func(opts *runInvocationOptions[T, E]) {
		opts.resumeTargetPolicy = policy
	})
}

// WithRunMetadata merges invocation metadata into run metadata before execution.
func WithRunMetadata[T, E any](input RunMetadataInput) RunOption[T, E] {
	return runOptionFunc[T, E](func(opts *runInvocationOptions[T, E]) {
		opts.runMetadata = input
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

// WithHandoffOutbox enqueues handoff continuation after a successful handoff checkpoint save.
func WithHandoffOutbox[T, E any](outbox HandoffOutbox) RunOption[T, E] {
	return runOptionFunc[T, E](func(opts *runInvocationOptions[T, E]) {
		opts.handoffOutbox = outbox
	})
}

// WithCheckpointErrorPolicy sets behavior when Checkpointer.Save fails during terminal saves.
// CheckpointPolicySkipOnSaveError emits EventCheckpointFailed on Stream/ResumeStream only; sync Start/Resume swallow
// the save error without observable signal and do not populate ResumeToken. Terminal flow
// continues with reason suffixes suspended_checkpoint_skipped, handoff_checkpoint_skipped, or
// context_canceled_checkpoint_skipped when the checkpoint was not persisted.
func WithCheckpointErrorPolicy[T, E any](policy CheckpointFailurePolicy) RunOption[T, E] {
	return runOptionFunc[T, E](func(opts *runInvocationOptions[T, E]) {
		opts.checkpointPolicy = policy
	})
}

func applyRunOptions[T, E any](opts ...RunOption[T, E]) (runInvocationOptions[T, E], error) {
	var out runInvocationOptions[T, E]
	for _, opt := range opts {
		if opt != nil {
			opt.apply(&out)
		}
	}
	if out.checkpointPolicy != "" &&
		out.checkpointPolicy != CheckpointPolicyHardFail &&
		out.checkpointPolicy != CheckpointPolicySkipOnSaveError {
		return out, fmt.Errorf("%w: %q", ErrInvalidCheckpointPolicy, out.checkpointPolicy)
	}
	return out, nil
}

// UseBudget records consumption of a named budget in run metadata.
// See also BudgetUsed for read access.
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

// BudgetUsed returns the consumed units for a named budget from the active execution context.
// It returns 0 if the budget is not found or the context does not contain run metadata.
func BudgetUsed(ctx context.Context, name string) int {
	meta, ok := runMetadataFromContext(ctx)
	if !ok || meta == nil || meta.BudgetCounts == nil {
		return 0
	}
	return meta.BudgetCounts[name]
}

// ContextWithRunMetadata injects RunMetadata into the provided context.
// Use it for Isolated Node Execution outside the Runner lifecycle: dry-runs,
// debugging, utilities, and direct node handler invocation (including unit tests).
func ContextWithRunMetadata(ctx context.Context, input RunMetadataInput) context.Context {
	meta := newRunMetadata()
	mergeRunMetadataInput(&meta, input)
	return withRunMetadata(ctx, &meta)
}

type runMetadataContextKey struct{}

func withRunMetadata(ctx context.Context, meta *RunMetadata) context.Context {
	return context.WithValue(ctx, runMetadataContextKey{}, meta)
}

func runMetadataFromContext(ctx context.Context) (*RunMetadata, bool) {
	meta, ok := ctx.Value(runMetadataContextKey{}).(*RunMetadata)
	return meta, ok
}

func applyResumeTargetPolicy[T, E any](
	ctx context.Context,
	state T,
	currentPtr ExecutionPointer,
	inv runInvocationOptions[T, E],
) (T, ExecutionPointer, error) {
	if inv.resumeTargetPolicy == nil {
		return state, currentPtr, nil
	}
	nextState, plan, err := inv.resumeTargetPolicy(ctx, state, currentPtr)
	if err != nil {
		return state, "", fmt.Errorf("%w: %w", ErrResumeTargetPolicyFailed, err)
	}
	newPtr, err := plan.resolve(currentPtr)
	if err != nil {
		return state, "", fmt.Errorf("%w: %w", ErrResumeTargetPolicyFailed, err)
	}
	return nextState, newPtr, nil
}

func mergeRunMetadataInput(meta *RunMetadata, input RunMetadataInput) {
	if len(input.BudgetCounts) > 0 {
		if meta.BudgetCounts == nil {
			meta.BudgetCounts = map[string]int{}
		}
		maps.Copy(meta.BudgetCounts, input.BudgetCounts)
	}
	if len(input.TelemetryContext) > 0 {
		if meta.TelemetryContext == nil {
			meta.TelemetryContext = map[string]string{}
		}
		maps.Copy(meta.TelemetryContext, input.TelemetryContext)
	}
}
