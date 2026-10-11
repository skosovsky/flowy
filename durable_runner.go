package flowy

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/skosovsky/flowy/internal/nilvalue"
)

// ErrExecutionCapability rejects an unsupported durable storage capability.
var ErrExecutionCapability = errors.New("flowy: execution capability unsupported")

const executionHeartbeatDivisor = 3

// DurableOptions declares an explicit owner, lease duration and pure migrations.
// Storage assigns the lease incarnation; owner text can safely be reused.
type DurableOptions struct {
	Owner      string
	LeaseTTL   time.Duration
	Migrations []ExecutionMigration
	Clock      ExecutionClock
	// RetryRandom is a pure, prompt, concurrency-safe scheduling sampler; nil uses the standard source.
	RetryRandom func() uint64
	WaitProfile *WaitCapabilityProfile
	ForkPolicy  *ForkExecutionPolicy
}

// DurableRunner executes a compiled BYOT graph with synchronous step commits.
// Host codecs are selected only after descriptor compatibility is established.
// External side effects require the activity boundary to be crash-safe.
type DurableRunner[T, E any] struct {
	graph        *Graph[T, E]
	store        ExecutionStore
	descriptor   ExecutionDescriptor
	stateCodec   StateSerializer[T]
	effectsCodec StateSerializer[[]E]
	options      DurableOptions
	waitBackend  DurableWaitBackend
}

// NewDurableRunner binds a graph and codecs to a fencing-capable raw store.
func NewDurableRunner[T, E any](
	graph *Graph[T, E],
	store ExecutionStore,
	descriptor ExecutionDescriptor,
	stateCodec StateSerializer[T],
	effectsCodec StateSerializer[[]E],
	options DurableOptions,
) (*DurableRunner[T, E], error) {
	if graph == nil || nilvalue.IsNil(store) || nilvalue.IsNil(stateCodec) || nilvalue.IsNil(effectsCodec) ||
		options.Owner == "" ||
		options.LeaseTTL <= 0 {
		return nil, ErrExecutionCapability
	}
	if err := descriptor.Validate(); err != nil {
		return nil, err
	}
	if !validRuntimeText(options.Owner) {
		return nil, ErrExecutionCapability
	}
	for node := range graph.nodes {
		if !validRuntimeText(node) {
			return nil, ErrExecutionCapability
		}
	}
	if graph.defaults.deleteOnSuccess || graph.defaults.retentionLimit != 0 {
		return nil, fmt.Errorf("%w: durable retention must preserve lineage", ErrExecutionCapability)
	}
	options.Migrations = append([]ExecutionMigration(nil), options.Migrations...)
	if options.ForkPolicy != nil {
		policy := *options.ForkPolicy
		options.ForkPolicy = &policy
	}
	if nilvalue.IsNil(options.Clock) {
		options.Clock = wallExecutionClock{}
	}
	var waitBackend DurableWaitBackend
	if options.WaitProfile != nil {
		var backendErr error
		waitBackend, backendErr = configuredWaitBackend(store, &options)
		if backendErr != nil {
			return nil, backendErr
		}
	}
	return &DurableRunner[T, E]{
		graph:        graph,
		store:        store,
		descriptor:   descriptor,
		stateCodec:   stateCodec,
		effectsCodec: effectsCodec,
		options:      options,
		waitBackend:  waitBackend,
	}, nil
}

// Start creates a new execution. An existing identity is never silently reset.
func (r *DurableRunner[T, E]) Start(
	ctx context.Context,
	id string,
	initial T,
	opts ...RunOption[T, E],
) (result *RunResult[T, E], retErr error) { //nolint:nonamedreturns // Cleanup must join every return path.
	if err := r.validateRunOptions(opts...); err != nil {
		return nil, err
	}
	if profileErr := r.checkRuntimeProfile(r.options.WaitProfile); profileErr != nil {
		return nil, profileErr
	}
	session, err := r.acquireSession(ctx, id)
	if err != nil {
		return nil, err
	}
	defer func() { retErr = errors.Join(retErr, session.finish()) }()
	ctx = session.ctx
	lease := session.lease
	envelope, err := r.prepareStart(ctx, lease, initial)
	if err != nil {
		return nil, err
	}
	return r.run(ctx, lease, envelope, opts...)
}

// Resume validates the requested latest revision, compatibility and target graph
// before selecting host codecs. A completed execution returns its stored result.
func (r *DurableRunner[T, E]) Resume(
	ctx context.Context,
	token ResumeToken,
	opts ...RunOption[T, E],
) (result *RunResult[T, E], retErr error) { //nolint:nonamedreturns // Cleanup must join every return path.
	if err := r.validateRunOptions(opts...); err != nil {
		return nil, err
	}
	session, err := r.acquireSession(ctx, token.ThreadID)
	if err != nil {
		return nil, err
	}
	defer func() { retErr = errors.Join(retErr, session.finish()) }()
	ctx = session.ctx
	lease := session.lease
	envelope, err := r.prepareResume(ctx, lease, token)
	if err != nil {
		return nil, err
	}
	return r.run(ctx, lease, envelope, opts...)
}

// ResumeStream uses the same compatibility and commit path as Resume. Terminal
// results are observed via WaitResult after the durable commit.
func (r *DurableRunner[T, E]) ResumeStream(
	ctx context.Context,
	token ResumeToken,
	opts ...RunOption[T, E],
) (StreamHandle[T, E], error) {
	if err := r.validateRunOptions(opts...); err != nil {
		return nil, err
	}
	return r.preparedStream(
		ctx,
		token.ThreadID,
		func(ownedCtx context.Context, lease ExecutionLease) (ExecutionEnvelope, error) {
			return r.prepareResume(ownedCtx, lease, token)
		},
		opts...)
}

func (r *DurableRunner[T, E]) prepareResume(
	ctx context.Context,
	lease ExecutionLease,
	token ResumeToken,
) (ExecutionEnvelope, error) {
	source, err := r.store.LoadExecution(ctx, token.ThreadID)
	if err != nil {
		return ExecutionEnvelope{}, err
	}
	if ctx.Err() != nil {
		return ExecutionEnvelope{}, context.Cause(ctx)
	}
	if integrityErr := ValidateExecutionIntegrity(source, token.ThreadID, source.Revision); integrityErr != nil {
		return ExecutionEnvelope{}, integrityErr
	}
	if token.SnapshotRevision == 0 || source.Revision != token.SnapshotRevision {
		return ExecutionEnvelope{}, ErrConcurrencyConflict
	}
	if collectionsErr := validateExecutionCollections(source); collectionsErr != nil {
		return ExecutionEnvelope{}, collectionsErr
	}
	if waitErr := r.checkArmedWaitCompatibility(source); waitErr != nil {
		return ExecutionEnvelope{}, waitErr
	}
	if metadataErr := validateExecutionSourceMetadata(source); metadataErr != nil {
		return ExecutionEnvelope{}, metadataErr
	}
	if policyErr := r.checkForkPolicy(ctx, source.Fork); policyErr != nil {
		return ExecutionEnvelope{}, policyErr
	}
	if err := source.Descriptor.Check(r.descriptor); err != nil {
		if source.Descriptor.Validate() != nil {
			return ExecutionEnvelope{}, err
		}
		return r.prepareMigratedTarget(ctx, lease, source)
	}
	if err := r.validatePointer(source.Progress.ExecutionPointer); err != nil {
		return ExecutionEnvelope{}, err
	}
	return source, nil
}

func (r *DurableRunner[T, E]) prepareMigratedTarget(
	ctx context.Context, lease ExecutionLease, source ExecutionEnvelope,
) (ExecutionEnvelope, error) {
	ctx = restoreExecutionTelemetry(ctx, source)
	event := executionObservation(source, LifecycleMigration, LifecycleStarted)
	observeLifecycle(ctx, event)
	event.Stage = LifecycleFailed
	defer func() { observeLifecycle(ctx, event) }()
	target, migrationErr := PrepareExecutionMigration(source, r.descriptor, r.options.Migrations, r.validatePointer)
	if migrationErr != nil {
		return ExecutionEnvelope{}, errors.Join(ErrExecutionIncompatible, migrationErr)
	}
	if collectionsErr := validateExecutionCollections(target); collectionsErr != nil {
		return ExecutionEnvelope{}, errors.Join(ErrMigrationInvalid, collectionsErr)
	}
	if codecErr := r.validateTargetCodecs(target); codecErr != nil {
		return ExecutionEnvelope{}, errors.Join(ErrMigrationInvalid, codecErr)
	}
	if ctx.Err() != nil {
		return ExecutionEnvelope{}, context.Cause(ctx)
	}
	committed, err := commitExecution(ctx, r.store, source.Revision, lease, target)
	if err == nil {
		event.Stage, event.Revision = LifecycleCommitted, committed.Revision
	}
	return committed, err
}

func (r *DurableRunner[T, E]) validateTargetCodecs(envelope ExecutionEnvelope) error {
	if _, err := r.stateCodec.Unmarshal(bytes.Clone(envelope.Progress.StatePayload)); err != nil {
		return fmt.Errorf("target state: %w", err)
	}
	if _, err := r.effectsCodec.Unmarshal(bytes.Clone(envelope.EffectsPayload)); err != nil {
		return fmt.Errorf("target effects: %w", err)
	}
	return nil
}

func (r *DurableRunner[T, E]) validatePointer(pointer ExecutionPointer) error {
	if _, exists := r.graph.nodes[string(pointer)]; !exists {
		return ErrResumeStartNodeNotFound
	}
	return nil
}

func (r *DurableRunner[T, E]) run(
	ctx context.Context,
	lease ExecutionLease,
	envelope ExecutionEnvelope,
	opts ...RunOption[T, E],
) (*RunResult[T, E], error) {
	return r.runWithSink(ctx, lease, envelope, nil, opts...)
}

func (r *DurableRunner[T, E]) runWithSink(
	ctx context.Context,
	lease ExecutionLease,
	envelope ExecutionEnvelope,
	sink eventSink[T, E],
	opts ...RunOption[T, E],
) (*RunResult[T, E], error) {
	inv, err := applyRunOptions(opts...)
	if err != nil {
		return nil, err
	}
	if inv.checkpointPolicy == CheckpointPolicySkipOnSaveError || inv.leaseOwner != "" {
		return nil, ErrExecutionCapability
	}
	runCtx := injectTelemetryContext(ctx, envelope.RunMeta.TelemetryContext)
	if ctx.Err() != nil {
		return nil, context.Cause(ctx)
	}
	cp := &executionCheckpointer[T, E]{
		mu:                       sync.Mutex{},
		store:                    r.store,
		lease:                    lease,
		envelope:                 cloneExecutionEnvelope(envelope),
		stateCodec:               r.stateCodec,
		effectsCodec:             r.effectsCodec,
		stepRevision:             envelope.Revision,
		clock:                    r.options.Clock,
		retryRandom:              r.options.RetryRandom,
		persistenceFailed:        false,
		childCancellationSignals: nil,
		waitBackend:              r.waitBackend,
		waitProfile:              r.options.WaitProfile,
		forkPolicy:               r.options.ForkPolicy,
		forkMode:                 executionForkMode(envelope),
	}
	if waitErr := r.checkArmedWaitCompatibility(envelope); waitErr != nil {
		return nil, waitErr
	}
	snapshot, _, err := cp.Load(runCtx, envelope.ExecutionID)
	if err != nil {
		return nil, err
	}
	attachSessionObservation(runCtx, envelope)
	if ctx.Err() != nil {
		return nil, context.Cause(ctx)
	}
	if envelope.Terminal != nil {
		return cachedDurableResult(runCtx, sink, envelope, snapshot)
	}
	if waiting, found, waitingErr := r.recoverArmedWait(runCtx, envelope, snapshot, sink); found || waitingErr != nil {
		return &waiting, waitingErr
	}
	base, ok := r.graph.NewRunner(cp).(*graphRunner[T, E])
	if !ok {
		return nil, ErrExecutionCapability
	}
	base.durable = cp
	decision, err := base.evaluateResume(
		runCtx,
		ResumeToken{ThreadID: envelope.ExecutionID, SnapshotRevision: envelope.Revision},
		inv,
	)
	if err != nil {
		return nil, err
	}
	attached := base.attachInvocation(runCtx, inv, &decision.RunMeta)
	attached = context.WithValue(attached, activityContextKey{}, activityBackend(cp))
	attached = context.WithValue(attached, childGroupContextKey{}, childGroupBackend(cp))
	// Hold terminal events until the corresponding aggregate has committed.
	var terminal *RunEvent[T, E]
	buffered := eventSink[T, E](nil)
	if sink != nil {
		buffered = func(eventCtx context.Context, event RunEvent[T, E]) bool {
			if isTerminalEventType(event.Type) {
				terminal = &event
				return true
			}
			return sink(eventCtx, event)
		}
	}
	event := executionObservation(envelope, LifecycleExecution, LifecycleStarted)
	event.SegmentID = decision.RunMeta.Segment.SegmentID
	observeLifecycle(attached, event)
	result, runErr := base.execute(
		attached,
		envelope.ExecutionID,
		string(decision.ExecutionPointer),
		decision.State,
		decision.RunMeta,
		decision.Effects,
		decision.SnapshotRevision,
		buffered,
		inv,
	)
	runErr = cp.finalizeRunOutcome(runCtx, result, runErr, sink, terminal)
	observeInvocationOutcome(runCtx, event, result, runErr)
	return result, runErr
}

func emitCachedDurableTerminal[T, E any](
	ctx context.Context,
	sink eventSink[T, E],
	envelope ExecutionEnvelope,
	snapshot Snapshot[T, E],
) {
	event := executionObservation(envelope, LifecycleTerminal, LifecycleReplayed)
	event.Code = lifecycleOutcomeCompleted
	if envelope.Terminal.Status == RunStatusFailed {
		event.Code = lifecycleOutcomeFailed
	}
	event.Revision = envelope.Revision
	observeLifecycle(ctx, event)
	if sink == nil {
		return
	}
	pointer := string(snapshot.ExecutionPointer)
	if envelope.Terminal.Status == RunStatusCompleted {
		sink(ctx, newRunEventCompleted[T, E](pointer, snapshot.State))
		return
	}
	sink(
		ctx,
		newRunEventFailed[T, E](
			pointer,
			snapshot.State,
			terminalFailureError(envelope.Terminal),
			envelope.Terminal.Reason,
		),
	)
}

func (*DurableRunner[T, E]) validateRunOptions(opts ...RunOption[T, E]) error {
	inv, err := applyRunOptions(opts...)
	if err != nil {
		return err
	}
	if inv.atomicHandoff {
		// The durable ExecutionStore profile has no ordinary transactional outbox.
		return ErrTransactionalOutboxUnsupported
	}
	if inv.checkpointPolicy == CheckpointPolicySkipOnSaveError || inv.leaseOwner != "" {
		return ErrExecutionCapability
	}
	return nil
}
