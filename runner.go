package flowy

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"
)

// Graph is the compiled, immutable graph.
type Graph[T, E any] struct {
	nodes              map[string]nodeDef[T, E]
	edges              map[string]string
	conditionalEdges   map[string]EdgeRouter[T]
	conditionalAllowed map[string]map[string]struct{}
	retryRoutes        map[string]string
	entryPoint         string
	reducer            Reducer[T]
	defaults           runConfig
}

type graphRunner[T, E any] struct {
	graph        *Graph[T, E]
	checkpointer Checkpointer[T, E]
	interceptors []StateInterceptor[T]
	leaseManager LeaseManager
	logger       *slog.Logger
	sessions     sync.Map // threadID -> *runSession
}

type eventSink[T, E any] func(ctx context.Context, event RunEvent[T, E]) bool

type streamHandle[T, E any] struct {
	events chan RunEvent[T, E]
	stop   chan struct{}
	done   chan struct{}
	once   sync.Once
	err    error
}

func (s *streamHandle[T, E]) Events() <-chan RunEvent[T, E] {
	return s.events
}

func (s *streamHandle[T, E]) Close() {
	s.once.Do(func() {
		close(s.stop)
	})
}

func (s *streamHandle[T, E]) Done() error {
	<-s.done
	return s.err
}

func (s *streamHandle[T, E]) stopped() bool {
	select {
	case <-s.stop:
		return true
	default:
		return false
	}
}

// NewRunner binds a compiled graph to persistence and returns a lifecycle runner.
func (g *Graph[T, E]) NewRunner(
	checkpointer Checkpointer[T, E],
	interceptors ...StateInterceptor[T],
) Runner[T, E] {
	return g.NewRunnerWithOptions(checkpointer, nil, interceptors...)
}

// NewRunnerWithOptions creates a runner with lease manager and other runner-level options.
func (g *Graph[T, E]) NewRunnerWithOptions(
	checkpointer Checkpointer[T, E],
	runnerOpts []RunnerOption[T, E],
	interceptors ...StateInterceptor[T],
) Runner[T, E] {
	r := &graphRunner[T, E]{
		graph:        g,
		checkpointer: checkpointer,
		interceptors: append([]StateInterceptor[T](nil), interceptors...),
		leaseManager: nil,
		logger:       slog.Default(),
		sessions:     sync.Map{},
	}
	for _, opt := range runnerOpts {
		if opt != nil {
			opt(r)
		}
	}
	if r.leaseManager != nil && r.checkpointer != nil {
		type leaseGuarded interface{ isLeaseGuardCheckpointer() }
		if _, guarded := r.checkpointer.(leaseGuarded); !guarded {
			if _, native := r.checkpointer.(NativeDeleteIfIdleCheckpointer); !native {
				r.checkpointer = NewLeaseGuardCheckpointer(r.checkpointer, r.leaseManager)
			}
		}
	}
	return r
}

func (r *graphRunner[T, E]) Start(
	ctx context.Context,
	threadID string,
	initialState T,
	opts ...RunOption[T, E],
) (*RunResult[T, E], error) {
	inv := applyRunOptions(opts...)
	if err := r.acquireLease(ctx, threadID, inv); err != nil {
		return nil, err
	}

	meta := newRunMetadata()
	mergeRunMetadataInput(&meta, inv.runMetadata)
	runCtx := r.attachInvocation(ctx, inv, &meta)
	result, err := r.execute(runCtx, threadID, r.graph.entryPoint, initialState, meta, nil, 0, nil, inv)
	r.postRunCleanup(context.WithoutCancel(ctx), threadID, inv, result)
	return result, err
}

func (r *graphRunner[T, E]) Resume(
	ctx context.Context,
	token ResumeToken,
	opts ...RunOption[T, E],
) (*RunResult[T, E], error) {
	if r.checkpointer == nil {
		return nil, errors.New("flowy: checkpointer is required for Resume")
	}
	if token.ThreadID == "" {
		return nil, fmt.Errorf("%w: empty thread ID", ErrInvalidResumeToken)
	}
	inv := applyRunOptions(opts...)
	if err := r.acquireLease(ctx, token.ThreadID, inv); err != nil {
		return nil, err
	}

	startNode, state, meta, effects, revision, err := r.prepareResume(ctx, token, inv)
	if err != nil {
		r.logReleaseLeaseError(
			context.WithoutCancel(ctx),
			token.ThreadID,
			r.releaseLease(context.WithoutCancel(ctx), token.ThreadID, inv),
		)
		return nil, err
	}
	runCtx := injectTelemetryContext(r.attachInvocation(ctx, inv, &meta), meta.TelemetryContext)
	result, runErr := r.execute(runCtx, token.ThreadID, startNode, state, meta, effects, revision, nil, inv)
	r.postRunCleanup(context.WithoutCancel(ctx), token.ThreadID, inv, result)
	return result, runErr
}

func (r *graphRunner[T, E]) Stream(
	ctx context.Context,
	threadID string,
	initialState T,
	opts ...RunOption[T, E],
) (StreamHandle[T, E], error) {
	inv := applyRunOptions(opts...)
	if err := r.acquireLease(ctx, threadID, inv); err != nil {
		return nil, err
	}
	meta := newRunMetadata()
	mergeRunMetadataInput(&meta, inv.runMetadata)
	runCtx := r.attachInvocation(ctx, inv, &meta)
	return r.startStream(runCtx, func(sink eventSink[T, E]) error {
		result, err := r.execute(runCtx, threadID, r.graph.entryPoint, initialState, meta, nil, 0, sink, inv)
		r.postRunCleanup(context.WithoutCancel(runCtx), threadID, inv, result)
		return err
	}), nil
}

func (r *graphRunner[T, E]) StreamResume(
	ctx context.Context,
	token ResumeToken,
	opts ...RunOption[T, E],
) (StreamHandle[T, E], error) {
	if r.checkpointer == nil {
		return nil, errors.New("flowy: checkpointer is required for StreamResume")
	}
	if token.ThreadID == "" {
		return nil, fmt.Errorf("%w: empty thread ID", ErrInvalidResumeToken)
	}
	inv := applyRunOptions(opts...)
	if err := r.acquireLease(ctx, token.ThreadID, inv); err != nil {
		return nil, err
	}
	startNode, state, meta, effects, revision, err := r.prepareResume(ctx, token, inv)
	if err != nil {
		r.logReleaseLeaseError(
			context.WithoutCancel(ctx),
			token.ThreadID,
			r.releaseLease(context.WithoutCancel(ctx), token.ThreadID, inv),
		)
		return nil, err
	}
	runCtx := injectTelemetryContext(r.attachInvocation(ctx, inv, &meta), meta.TelemetryContext)
	return r.startStream(runCtx, func(sink eventSink[T, E]) error {
		result, runErr := r.execute(runCtx, token.ThreadID, startNode, state, meta, effects, revision, sink, inv)
		r.postRunCleanup(context.WithoutCancel(runCtx), token.ThreadID, inv, result)
		return runErr
	}), nil
}

func (r *graphRunner[T, E]) HandoffToBackground(ctx context.Context, threadID string) error {
	if threadID == "" {
		return errors.New("flowy: handoff requires threadID")
	}
	value, ok := r.sessions.Load(threadID)
	if !ok {
		return fmt.Errorf("%w: thread %q", ErrNoActiveExecution, threadID)
	}
	session, ok := value.(*runSession)
	if !ok || session.cancel == nil {
		return fmt.Errorf("%w: thread %q", ErrNoActiveExecution, threadID)
	}
	session.cancel(ErrHandoffRequested)

	waitCtx := ctx
	if _, hasDeadline := ctx.Deadline(); !hasDeadline {
		var cancel context.CancelFunc
		waitCtx, cancel = context.WithTimeout(ctx, handoffCompletionTimeout)
		defer cancel()
	}
	select {
	case <-session.done:
		if err := session.completionError(); err != nil {
			return err
		}
		return nil
	case <-waitCtx.Done():
		return fmt.Errorf("%w: %w", ErrHandoffNotCompleted, waitCtx.Err())
	}
}

func (r *graphRunner[T, E]) registerRunSession(threadID string, cancel context.CancelCauseFunc) *runSession {
	session := newRunSession(cancel)
	r.sessions.Store(threadID, session)
	return session
}

func (r *graphRunner[T, E]) finishRunSession(threadID string, err error) {
	value, ok := r.sessions.Load(threadID)
	if !ok {
		return
	}
	session, ok := value.(*runSession)
	if !ok {
		return
	}
	session.finish(err)
}

func (r *graphRunner[T, E]) unregisterRunSession(threadID string) {
	r.sessions.Delete(threadID)
}

func newRunMetadata() RunMetadata {
	now := time.Now().UTC()
	seg := newSegmentInfo()
	return RunMetadata{
		Segment:          seg,
		SegmentStartTime: now,
		RetryCounts:      map[string]int{},
		BudgetCounts:     map[string]int{},
		StepCount:        0,
		TelemetryContext: nil,
	}
}

func (r *graphRunner[T, E]) attachInvocation(
	ctx context.Context,
	inv runInvocationOptions[T, E],
	meta *RunMetadata,
) context.Context {
	runCtx := withRunMetadata(ctx, meta)
	if inv.bindings != nil {
		runCtx = inv.bindings.WithContext(runCtx)
	}
	if inv.leaseOwner != "" {
		runCtx = WithLeaseOwner(runCtx, inv.leaseOwner)
	}
	return runCtx
}

func (r *graphRunner[T, E]) acquireLease(ctx context.Context, threadID string, inv runInvocationOptions[T, E]) error {
	if r.leaseManager == nil {
		return nil
	}
	if _, nativeCP := r.checkpointer.(NativeDeleteIfIdleCheckpointer); nativeCP {
		if _, paired := r.leaseManager.(NativeLeaseManager); !paired {
			return errors.New(
				"flowy: native DeleteIfIdle checkpointer requires paired adapters/lease manager (postgres or redis)",
			)
		}
	}
	if inv.leaseOwner == "" {
		return ErrLeaseOwnerRequired
	}
	ttl := inv.leaseTTL
	if ttl <= 0 {
		ttl = defaultLeaseTTL
	}
	return r.leaseManager.Acquire(ctx, threadID, inv.leaseOwner, ttl)
}

func (r *graphRunner[T, E]) releaseLease(ctx context.Context, threadID string, inv runInvocationOptions[T, E]) error {
	if r.leaseManager == nil || inv.leaseOwner == "" {
		return nil
	}
	return r.leaseManager.Release(ctx, threadID, inv.leaseOwner)
}

func (r *graphRunner[T, E]) leaseTTL(inv runInvocationOptions[T, E]) time.Duration {
	ttl := inv.leaseTTL
	if ttl <= 0 {
		ttl = defaultLeaseTTL
	}
	return ttl
}

func (r *graphRunner[T, E]) startLeaseHeartbeat(
	ctx context.Context,
	threadID string,
	inv runInvocationOptions[T, E],
	cancelRun context.CancelCauseFunc,
) func() {
	if r.leaseManager == nil || inv.leaseOwner == "" {
		return func() {}
	}
	ttl := r.leaseTTL(inv)
	interval := leaseHeartbeatInterval(ttl)

	hbCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		renew := func() bool {
			if err := r.leaseManager.Renew(hbCtx, threadID, inv.leaseOwner, ttl); err != nil {
				if cancelRun != nil {
					cancelRun(ErrLeaseLost)
				}
				return false
			}
			return true
		}
		if !renew() {
			return
		}
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-hbCtx.Done():
				return
			case <-ticker.C:
				if !renew() {
					return
				}
			}
		}
	}()
	return func() {
		cancel()
		<-done
	}
}

func (r *graphRunner[T, E]) startStream(_ context.Context, runFn func(sink eventSink[T, E]) error) StreamHandle[T, E] {
	stream := &streamHandle[T, E]{
		events: make(chan RunEvent[T, E], streamEventBufferSize),
		stop:   make(chan struct{}),
		done:   make(chan struct{}),
		once:   sync.Once{},
		err:    nil,
	}
	go func() {
		defer close(stream.events)
		defer close(stream.done)
		sink := func(eventCtx context.Context, event RunEvent[T, E]) bool {
			select {
			case <-stream.stop:
				return false
			case <-eventCtx.Done():
				return false
			case stream.events <- event:
				return true
			}
		}
		err := runFn(sink)
		if errors.Is(err, context.Canceled) && stream.stopped() {
			err = nil
		}
		stream.err = err
	}()
	return stream
}

func (r *graphRunner[T, E]) prepareResume(
	ctx context.Context,
	token ResumeToken,
	inv runInvocationOptions[T, E],
) (string, T, RunMetadata, []E, int, error) {
	// 1) Load base snapshot
	snapshot, err := r.checkpointer.Load(ctx, token.ThreadID)
	if err != nil {
		var zero T
		return "", zero, RunMetadata{}, nil, 0, fmt.Errorf("%w: %w", ErrThreadNotFound, err)
	}
	if token.Generation != snapshot.Revision {
		var zero T
		return "", zero, RunMetadata{}, nil, 0, fmt.Errorf(
			"%w: token generation %d, snapshot revision %d",
			ErrStaleResumeToken, token.Generation, snapshot.Revision,
		)
	}
	state := snapshot.State
	for _, interceptor := range r.interceptors {
		if err := interceptor.AfterLoad(ctx, &state); err != nil {
			var zero T
			return "", zero, RunMetadata{}, nil, 0, fmt.Errorf("flowy: after_load interceptor: %w", err)
		}
	}

	// 2) Overlay merge
	if inv.overlay != nil {
		if inv.overlayMerger == nil {
			var zero T
			return "", zero, RunMetadata{}, nil, 0, ErrOverlayMergerRequired
		}
		state = inv.overlayMerger(state, *inv.overlay)
	}
	meta := snapshot.RunMeta
	if meta.RetryCounts == nil {
		meta.RetryCounts = map[string]int{}
	}
	resetSegmentCounters(&meta)
	mergeRunMetadataInput(&meta, inv.runMetadata)

	activePointer := snapshot.ExecutionPointer
	var reconcileErr error
	state, activePointer, reconcileErr = reconcileResume(state, activePointer)
	if reconcileErr != nil {
		var zero T
		return "", zero, RunMetadata{}, nil, 0, reconcileErr
	}

	if activePointer == "" {
		var zero T
		return "", zero, RunMetadata{}, nil, 0, ErrInvalidSnapshot
	}
	startNode := string(activePointer)

	if _, ok := r.graph.nodes[startNode]; !ok {
		var zero T
		return "", zero, RunMetadata{}, nil, 0, fmt.Errorf("%w: %q", ErrResumeStartNodeNotFound, startNode)
	}

	return startNode, state, meta, append([]E(nil), snapshot.Effects...), snapshot.Revision, nil
}

func resetSegmentCounters(meta *RunMetadata) {
	meta.Segment = newSegmentInfo()
	meta.SegmentStartTime = time.Now().UTC()
	meta.StepCount = 0
	if meta.BudgetCounts == nil {
		meta.BudgetCounts = map[string]int{}
	}
}

//nolint:gocognit,funlen // central run loop; splitting obscures lifecycle transitions
func (r *graphRunner[T, E]) execute(
	ctx context.Context,
	threadID string,
	startNode string,
	state T,
	meta RunMetadata,
	effects []E,
	revision int,
	sink eventSink[T, E],
	inv runInvocationOptions[T, E],
) (*RunResult[T, E], error) {
	current := startNode
	runCtx, cancelRun := context.WithCancelCause(ctx)
	runCtx = withRunThreadID(runCtx, threadID)
	_ = r.registerRunSession(threadID, cancelRun)
	defer r.unregisterRunSession(threadID)

	limit := r.graph.defaults.maxSteps
	if limit <= 0 {
		limit = defaultMaxSteps
	}

	stopHeartbeat := r.startLeaseHeartbeat(runCtx, threadID, inv, cancelRun)
	defer stopHeartbeat()

	for {
		if errors.Is(context.Cause(runCtx), ErrHandoffRequested) {
			return r.handleHandoff(
				runCtx, threadID, current, state, meta, effects, revision, sink, inv,
			)
		}
		if errors.Is(context.Cause(runCtx), ErrLeaseLost) {
			markSegmentFailed(&meta)
			emitTerminalEvent(runCtx, sink, newRunEventFailed[T, E](current, state, ErrLeaseLost))
			return failedResultWithReason(state, effects, meta, current, ErrLeaseLost.Error()), ErrLeaseLost
		}
		if ctxErr := runCtx.Err(); ctxErr != nil {
			return r.handleContextCancellation(
				runCtx, threadID, current, state, meta, effects, revision, sink, ctxErr, inv,
			)
		}

		nodeCtx := withNodeName(runCtx, current)
		step, stepErr := r.runNodeStep(nodeCtx, runCtx, current, state, meta, effects, sink, inv)
		if stepErr != nil {
			if errors.Is(context.Cause(runCtx), ErrLeaseLost) {
				markSegmentFailed(&step.meta)
				emitTerminalEvent(runCtx, sink, newRunEventFailed[T, E](current, step.state, ErrLeaseLost))
				return failedResultWithReason(
					step.state,
					step.effects,
					step.meta,
					current,
					ErrLeaseLost.Error(),
				), ErrLeaseLost
			}
			if errors.Is(stepErr, context.Canceled) && runCtx.Err() != nil {
				if errors.Is(context.Cause(runCtx), ErrHandoffRequested) {
					return r.handleHandoff(
						runCtx, threadID, current, step.state, step.meta, step.effects, revision, sink, inv,
					)
				}
				return r.handleContextCancellation(
					runCtx, threadID, current, step.state, step.meta, step.effects, revision, sink, runCtx.Err(), inv,
				)
			}
			return failedResult(step.state, step.effects, step.meta, current), stepErr
		}
		if step.emitCanceled {
			if errors.Is(context.Cause(runCtx), ErrHandoffRequested) {
				return r.handleHandoff(
					runCtx, threadID, current, step.state, step.meta, step.effects, revision, sink, inv,
				)
			}
			return failedResult(step.state, step.effects, step.meta, current), context.Canceled
		}
		if errors.Is(context.Cause(runCtx), ErrHandoffRequested) {
			return r.handleHandoff(
				runCtx, threadID, current, step.state, step.meta, step.effects, revision, sink, inv,
			)
		}
		if errors.Is(context.Cause(runCtx), ErrLeaseLost) {
			markSegmentFailed(&step.meta)
			emitTerminalEvent(runCtx, sink, newRunEventFailed[T, E](current, step.state, ErrLeaseLost))
			return failedResultWithReason(
				step.state,
				step.effects,
				step.meta,
				current,
				ErrLeaseLost.Error(),
			), ErrLeaseLost
		}

		state = step.state
		meta = step.meta
		effects = step.effects
		base := step.base

		if meta.StepCount > limit {
			markSegmentFailed(&meta)
			emitTerminalEvent(runCtx, sink, newRunEventFailed[T, E](current, state, ErrMaxStepsExceeded))
			return failedResultWithReason(
				state,
				effects,
				meta,
				current,
				ErrMaxStepsExceeded.Error(),
			), ErrMaxStepsExceeded
		}

		if err := checkBudgetLimits(meta, r.graph.defaults.budgetLimits); err != nil {
			markSegmentFailed(&meta)
			emitTerminalEvent(runCtx, sink, newRunEventFailed[T, E](current, state, err))
			return failedResultWithReason(state, effects, meta, current, err.Error()), err
		}

		if errors.Is(context.Cause(runCtx), ErrLeaseLost) {
			markSegmentFailed(&meta)
			emitTerminalEvent(runCtx, sink, newRunEventFailed[T, E](current, state, ErrLeaseLost))
			return failedResultWithReason(state, effects, meta, current, ErrLeaseLost.Error()), ErrLeaseLost
		}

		stepOut := r.applyDirective(
			runCtx, nodeCtx, threadID, current, state, meta, effects, revision, base, sink, inv,
		)
		if stepOut.terminal {
			return stepOut.result, stepOut.err
		}
		current = stepOut.nextNode
	}
}

func (r *graphRunner[T, E]) handleContextCancellation(
	runCtx context.Context,
	threadID, current string,
	state T,
	meta RunMetadata,
	effects []E,
	revision int,
	sink eventSink[T, E],
	ctxErr error,
	inv runInvocationOptions[T, E],
) (*RunResult[T, E], error) {
	revision++
	meta.TelemetryContext = extractTelemetryContext(runCtx)
	meta.Segment.EndTime = time.Now().UTC()
	meta.Segment.EndReason = SegmentEndContextCanceled
	result := newRunResultContextCanceled(state, effects, meta, current)

	persisted, saveErr := r.persistSnapshot(runCtx, Snapshot[T, E]{
		ThreadID:         threadID,
		ExecutionPointer: ExecutionPointer(current),
		Revision:         revision,
		State:            state,
		RunMeta:          meta,
		Effects:          append([]E(nil), effects...),
	}, sink, current, state, inv)
	if saveErr != nil {
		result.Reason = ReasonContextCanceledSaveFailed
		emitTerminalEvent(runCtx, sink, newRunEventContextCanceled[T, E](current, state, result.Reason))
		return result, fmt.Errorf("flowy: context canceled and save failed: %w", saveErr)
	}
	if !persisted && inv.checkpointPolicy == CheckpointPolicySoftWarn {
		result.Reason = ReasonContextCanceledCheckpointSkipped
	}
	policyCtx, cancelPolicy := context.WithTimeout(context.WithoutCancel(runCtx), contextCancelSaveTimeout)
	policyErr := r.applyRetentionPolicy(policyCtx, threadID)
	cancelPolicy()

	emitTerminalEvent(runCtx, sink, newRunEventContextCanceled[T, E](current, state, result.Reason))
	if policyErr != nil {
		result.Reason = retentionFailedReason(result.Reason)
		return result, fmt.Errorf("flowy: context canceled, retention failed: %w", policyErr)
	}
	return result, fmt.Errorf("flowy: %w", ctxErr)
}

func (r *graphRunner[T, E]) completeHandoffTerminal(
	runCtx context.Context,
	threadID, current string,
	state T,
	meta RunMetadata,
	effects []E,
	revision int,
	reason string,
	sink eventSink[T, E],
	inv runInvocationOptions[T, E],
	signalSession bool,
) (*RunResult[T, E], error) {
	revision++
	meta.TelemetryContext = extractTelemetryContext(runCtx)
	meta.Segment.EndTime = time.Now().UTC()
	meta.Segment.EndReason = SegmentEndHandoff
	resumePtr, ptrErr := resolveSuspendPointer(state, current, inv.suspendPointerResolver)
	if ptrErr != nil {
		handoffErr := fmt.Errorf("flowy: handoff pointer resolve failed: %w", ptrErr)
		if signalSession {
			r.finishRunSession(threadID, handoffErr)
		}
		emitTerminalEvent(runCtx, sink, newRunEventFailed[T, E](current, state, ptrErr))
		return newRunResultFailed(state, effects, meta, current, reason), handoffErr
	}
	if ptrErr := r.validateExecutionPointer(resumePtr); ptrErr != nil {
		handoffErr := fmt.Errorf("flowy: handoff pointer resolve failed: %w", ptrErr)
		if signalSession {
			r.finishRunSession(threadID, handoffErr)
		}
		emitTerminalEvent(runCtx, sink, newRunEventFailed[T, E](current, state, ptrErr))
		return newRunResultFailed(state, effects, meta, current, reason), handoffErr
	}
	savedPointer := string(resumePtr)
	result := newRunResultHandoff(state, effects, meta, savedPointer, reason)

	persisted, saveErr := r.persistSnapshot(runCtx, Snapshot[T, E]{
		ThreadID:         threadID,
		ExecutionPointer: resumePtr,
		Revision:         revision,
		State:            state,
		RunMeta:          meta,
		Effects:          append([]E(nil), effects...),
	}, sink, current, state, inv)
	if saveErr != nil {
		handoffErr := fmt.Errorf("flowy: handoff save failed: %w", saveErr)
		if signalSession {
			r.finishRunSession(threadID, handoffErr)
		}
		emitTerminalEvent(runCtx, sink, newRunEventFailed[T, E](current, state, saveErr))
		return newRunResultFailed(state, effects, meta, current, reason), handoffErr
	}
	if !persisted && inv.checkpointPolicy == CheckpointPolicySoftWarn {
		result.Reason = ReasonHandoffCheckpointSkipped
	}
	if persisted {
		result.ResumeToken = ResumeToken{ThreadID: threadID, Generation: revision}
		if scheduleErr := r.scheduleHandoffContinuation(runCtx, inv, result.ResumeToken); scheduleErr != nil {
			return r.handoffAfterScheduleFailure(
				runCtx, threadID, savedPointer, state, result, sink, signalSession, scheduleErr,
			)
		}
	}
	return r.finalizeHandoffTerminal(runCtx, threadID, savedPointer, state, result.Reason, result, sink, signalSession)
}

func (r *graphRunner[T, E]) handoffAfterScheduleFailure(
	runCtx context.Context,
	threadID, savedPointer string,
	state T,
	result *RunResult[T, E],
	sink eventSink[T, E],
	signalSession bool,
	scheduleErr error,
) (*RunResult[T, E], error) {
	if signalSession {
		r.finishRunSession(threadID, scheduleErr)
	}
	r.emitHandoffTerminalEvent(runCtx, sink, threadID, savedPointer, state, result.Reason)
	policyCtx, cancelPolicy := context.WithTimeout(context.WithoutCancel(runCtx), contextCancelSaveTimeout)
	policyErr := r.applyRetentionPolicy(policyCtx, threadID)
	cancelPolicy()
	if policyErr != nil {
		result.Reason = retentionFailedReason(result.Reason)
	}
	return result, scheduleErr
}

func (r *graphRunner[T, E]) finalizeHandoffTerminal(
	runCtx context.Context,
	threadID, savedPointer string,
	state T,
	reason string,
	result *RunResult[T, E],
	sink eventSink[T, E],
	signalSession bool,
) (*RunResult[T, E], error) {
	policyCtx, cancelPolicy := context.WithTimeout(context.WithoutCancel(runCtx), contextCancelSaveTimeout)
	policyErr := r.applyRetentionPolicy(policyCtx, threadID)
	cancelPolicy()

	r.emitHandoffTerminalEvent(runCtx, sink, threadID, savedPointer, state, reason)
	return r.completeHandoffSession(threadID, result, policyErr, signalSession)
}

func (r *graphRunner[T, E]) emitHandoffTerminalEvent(
	runCtx context.Context,
	sink eventSink[T, E],
	threadID, savedPointer string,
	state T,
	reason string,
) {
	if !emitTerminalEvent(runCtx, sink, newRunEventHandoff[T, E](savedPointer, state, reason)) {
		r.logger.DebugContext(runCtx, "flowy: handoff persisted but terminal event not delivered",
			"thread_id", threadID)
	}
}

func (r *graphRunner[T, E]) completeHandoffSession(
	threadID string,
	result *RunResult[T, E],
	policyErr error,
	signalSession bool,
) (*RunResult[T, E], error) {
	if policyErr != nil {
		result.Reason = retentionFailedReason(result.Reason)
		retentionErr := fmt.Errorf("flowy: handoff retention failed: %w", policyErr)
		if signalSession {
			r.finishRunSession(threadID, retentionErr)
		}
		return result, retentionErr
	}
	if signalSession {
		r.finishRunSession(threadID, nil)
	}
	return result, nil
}

func (r *graphRunner[T, E]) handleHandoff(
	runCtx context.Context,
	threadID, current string,
	state T,
	meta RunMetadata,
	effects []E,
	revision int,
	sink eventSink[T, E],
	inv runInvocationOptions[T, E],
) (*RunResult[T, E], error) {
	reason := "background_handoff"
	if cause := context.Cause(runCtx); cause != nil && !errors.Is(cause, ErrHandoffRequested) {
		reason = cause.Error()
	}
	return r.completeHandoffTerminal(
		runCtx, threadID, current, state, meta, effects, revision, reason, sink, inv, true,
	)
}

type nodeStepOutcome[T, E any] struct {
	state        T
	meta         RunMetadata
	effects      []E
	base         Directive
	emitCanceled bool
}

type directiveStep[T, E any] struct {
	nextNode string
	result   *RunResult[T, E]
	err      error
	terminal bool
}

func (r *graphRunner[T, E]) runNodeStep(
	nodeCtx, runCtx context.Context,
	current string,
	state T,
	meta RunMetadata,
	effects []E,
	sink eventSink[T, E],
	inv runInvocationOptions[T, E],
) (nodeStepOutcome[T, E], error) {
	node, ok := r.graph.nodes[current]
	if !ok {
		err := fmt.Errorf("flowy: node %q not found", current)
		emitTerminalEvent(runCtx, sink, newRunEventFailed[T, E](current, state, err))
		return blankNodeStepOutcome(state, meta, effects), err
	}

	if !emitEvent(nodeCtx, sink, newRunEventNodeStarted[T, E](current, state)) {
		return canceledNodeStepOutcome(state, meta, effects), nil
	}

	nodeStart := time.Now()
	update, directive, err := node.handler(nodeCtx, state)
	nodeDuration := time.Since(nodeStart)
	if err != nil {
		emitTerminalEvent(runCtx, sink, newRunEventFailed[T, E](current, state, err))
		return blankNodeStepOutcome(state, meta, effects), fmt.Errorf("flowy: node %q: %w", current, err)
	}

	state = r.graph.reducer(state, update)
	meta.StepCount++

	if inv.invariantValidator != nil {
		if invErr := inv.invariantValidator(state); invErr != nil {
			emitTerminalEvent(runCtx, sink, newRunEventFailed[T, E](current, state, invErr))
			return blankNodeStepOutcome(state, meta, effects), invErr
		}
	}

	base, nodeEffects, err := UnwrapDirective[E](directive)
	if err != nil {
		emitTerminalEvent(runCtx, sink, newRunEventFailed[T, E](current, state, err))
		return blankNodeStepOutcome(state, meta, effects), err
	}

	effects = append(effects, nodeEffects...)
	if len(nodeEffects) == 0 {
		if !emitEvent(nodeCtx, sink, newRunEventNodeCompleted[T, E](current, state, nodeDuration)) {
			return canceledNodeStepOutcome(state, meta, effects), nil
		}
	} else {
		for _, effect := range nodeEffects {
			if !emitEvent(nodeCtx, sink, newRunEventNodeCompletedWithEffect[T, E](
				current, state, effect, nodeDuration,
			)) {
				return canceledNodeStepOutcome(state, meta, effects), nil
			}
		}
	}

	out := blankNodeStepOutcome(state, meta, effects)
	out.base = base
	return out, nil
}

func (r *graphRunner[T, E]) applyDirective(
	runCtx, nodeCtx context.Context,
	threadID, current string,
	state T,
	meta RunMetadata,
	effects []E,
	revision int,
	base Directive,
	sink eventSink[T, E],
	inv runInvocationOptions[T, E],
) directiveStep[T, E] {
	switch base.kind {
	case directiveCompleted:
		return r.applyDirectiveCompleted(runCtx, nodeCtx, threadID, current, state, meta, effects, sink)
	case directiveNext:
		emitTerminalEvent(runCtx, sink, newRunEventFailed[T, E](current, state, ErrLegacyNext))
		return terminalDirectiveStep(
			failedResult(state, effects, meta, current),
			ErrLegacyNext,
		)
	case directiveEnd:
		return r.finishCompleted(runCtx, threadID, current, state, meta, effects, sink, SegmentEndComplete)
	case directiveSuspend:
		return r.applyDirectiveSuspend(runCtx, threadID, current, state, meta, effects, revision, base, sink, inv)
	case directiveHandoff:
		return r.applyDirectiveHandoff(runCtx, threadID, current, state, meta, effects, revision, base, sink, inv)
	case directiveRetry:
		return r.applyDirectiveRetry(runCtx, current, state, meta, effects, base, sink)
	case directiveFail:
		return r.applyDirectiveFail(runCtx, threadID, current, state, meta, effects, base, sink)
	default:
		unsupported := errors.New("flowy: node returned unsupported directive")
		emitTerminalEvent(runCtx, sink, newRunEventFailed[T, E](current, state, unsupported))
		return terminalDirectiveStep(
			failedResult(state, effects, meta, current),
			unsupported,
		)
	}
}

func (r *graphRunner[T, E]) applyDirectiveFail(
	runCtx context.Context,
	_ string,
	current string,
	state T,
	meta RunMetadata,
	effects []E,
	base Directive,
	sink eventSink[T, E],
) directiveStep[T, E] {
	meta.Segment.EndTime = time.Now().UTC()
	meta.Segment.EndReason = SegmentEndFail
	result := newRunResultFailed(state, effects, meta, current, base.reason)
	emitTerminalEvent(runCtx, sink, newRunEventFailed[T, E](current, state, errors.New(base.reason)))
	return terminalDirectiveStep(result, errors.New(base.reason))
}

func (r *graphRunner[T, E]) applyDirectiveCompleted(
	runCtx, nodeCtx context.Context,
	threadID, current string,
	state T,
	meta RunMetadata,
	effects []E,
	sink eventSink[T, E],
) directiveStep[T, E] {
	nextNode, err := r.resolveEdge(nodeCtx, current, state)
	if err != nil {
		emitTerminalEvent(runCtx, sink, newRunEventFailed[T, E](current, state, err))
		return terminalDirectiveStep(failedResult(state, effects, meta, current), err)
	}
	if nextNode == EndNode {
		return r.finishCompleted(runCtx, threadID, current, state, meta, effects, sink, SegmentEndComplete)
	}
	return continueDirectiveStep[T, E](nextNode)
}

func (r *graphRunner[T, E]) finishCompleted(
	runCtx context.Context,
	threadID, current string,
	state T,
	meta RunMetadata,
	effects []E,
	sink eventSink[T, E],
	endReason SegmentEndReason,
) directiveStep[T, E] {
	meta.Segment.EndTime = time.Now().UTC()
	meta.Segment.EndReason = endReason
	result := newRunResultCompleted(state, effects, meta, current)
	if !emitTerminalEvent(runCtx, sink, newRunEventCompleted[T, E](current, state)) {
		r.logger.DebugContext(runCtx, "flowy: completed but terminal event not delivered",
			"thread_id", threadID)
	}
	return terminalDirectiveStep[T, E](result, nil)
}

func (r *graphRunner[T, E]) applyDirectiveSuspend(
	runCtx context.Context,
	threadID, current string,
	state T,
	meta RunMetadata,
	effects []E,
	revision int,
	base Directive,
	sink eventSink[T, E],
	inv runInvocationOptions[T, E],
) directiveStep[T, E] {
	revision++
	meta.TelemetryContext = extractTelemetryContext(runCtx)
	meta.Segment.EndTime = time.Now().UTC()
	meta.Segment.EndReason = SegmentEndSuspend
	resumePtr, ptrErr := resolveSuspendPointer(state, current, inv.suspendPointerResolver)
	if ptrErr != nil {
		emitTerminalEvent(runCtx, sink, newRunEventFailed[T, E](current, state, ptrErr))
		return terminalDirectiveStep(
			failedResult(state, effects, meta, current),
			fmt.Errorf("flowy: suspend pointer resolve failed: %w", ptrErr),
		)
	}
	if validateErr := r.validateExecutionPointer(resumePtr); validateErr != nil {
		emitTerminalEvent(runCtx, sink, newRunEventFailed[T, E](current, state, validateErr))
		return terminalDirectiveStep(
			failedResult(state, effects, meta, current),
			fmt.Errorf("flowy: suspend pointer resolve failed: %w", validateErr),
		)
	}
	savedPointer := string(resumePtr)
	snapshot := Snapshot[T, E]{
		ThreadID:         threadID,
		ExecutionPointer: resumePtr,
		Revision:         revision,
		State:            state,
		RunMeta:          meta,
		Effects:          append([]E(nil), effects...),
	}
	persisted, saveErr := r.persistSnapshot(runCtx, snapshot, sink, current, state, inv)
	if saveErr != nil {
		emitTerminalEvent(runCtx, sink, newRunEventFailed[T, E](current, state, saveErr))
		return terminalDirectiveStep(
			failedResult(state, effects, meta, current),
			fmt.Errorf("flowy: suspend save failed: %w", saveErr),
		)
	}
	policyCtx, cancelPolicy := context.WithTimeout(context.WithoutCancel(runCtx), contextCancelSaveTimeout)
	policyErr := r.applyRetentionPolicy(policyCtx, threadID)
	cancelPolicy()
	suspendReason := base.reason
	if suspendReason == "" {
		suspendReason = "suspended"
	}
	if !persisted && inv.checkpointPolicy == CheckpointPolicySoftWarn {
		suspendReason = ReasonSuspendedCheckpointSkipped
	}
	suspendedResult := newRunResultSuspended(state, effects, meta, savedPointer, suspendReason)
	if persisted {
		suspendedResult.ResumeToken = ResumeToken{ThreadID: threadID, Generation: revision}
	}
	if !emitTerminalEvent(runCtx, sink, newRunEventSuspendedNoError[T, E](savedPointer, state, suspendReason)) {
		r.logger.DebugContext(runCtx, "flowy: suspend persisted but terminal event not delivered",
			"thread_id", threadID)
		if policyErr != nil {
			suspendedResult.Reason = retentionFailedReason(suspendedResult.Reason)
			return terminalDirectiveStep(
				suspendedResult,
				fmt.Errorf("flowy: suspend retention failed: %w", policyErr),
			)
		}
		return terminalDirectiveStep(suspendedResult, nil)
	}
	if policyErr != nil {
		suspendedResult.Reason = retentionFailedReason(suspendedResult.Reason)
		return terminalDirectiveStep(
			suspendedResult,
			fmt.Errorf("flowy: suspend retention failed: %w", policyErr),
		)
	}
	return terminalDirectiveStep(suspendedResult, nil)
}

func retentionFailedReason(reason string) string {
	if reason == "" {
		return "retention_failed"
	}
	return reason + "_retention_failed"
}

func (r *graphRunner[T, E]) applyDirectiveHandoff(
	runCtx context.Context,
	threadID, current string,
	state T,
	meta RunMetadata,
	effects []E,
	revision int,
	base Directive,
	sink eventSink[T, E],
	inv runInvocationOptions[T, E],
) directiveStep[T, E] {
	reason := base.reason
	if reason == "" {
		reason = "handoff"
	}
	result, err := r.completeHandoffTerminal(
		runCtx, threadID, current, state, meta, effects, revision, reason, sink, inv, false,
	)
	return terminalDirectiveStep(result, err)
}

func (r *graphRunner[T, E]) applyDirectiveRetry(
	runCtx context.Context,
	current string,
	state T,
	meta RunMetadata,
	effects []E,
	base Directive,
	sink eventSink[T, E],
) directiveStep[T, E] {
	if base.maxAttempts <= 0 {
		err := errors.New("flowy: Retry directive requires maxAttempts > 0")
		emitTerminalEvent(runCtx, sink, newRunEventFailed[T, E](current, state, err))
		return terminalDirectiveStep(failedResult(state, effects, meta, current), err)
	}
	meta.RetryCounts[current]++
	if meta.RetryCounts[current] > base.maxAttempts {
		markSegmentFailed(&meta)
		emitTerminalEvent(runCtx, sink, newRunEventFailed[T, E](current, state, ErrRetryBudgetExceeded))
		return terminalDirectiveStep(
			failedResultWithReason(state, effects, meta, current, ErrRetryBudgetExceeded.Error()),
			ErrRetryBudgetExceeded,
		)
	}
	fallback, ok := r.graph.retryRoutes[current]
	if !ok {
		err := fmt.Errorf("flowy: node %q returned Retry without AddRetryRoute", current)
		emitTerminalEvent(runCtx, sink, newRunEventFailed[T, E](current, state, err))
		return terminalDirectiveStep(failedResult(state, effects, meta, current), err)
	}
	return continueDirectiveStep[T, E](fallback)
}

func failedResult[T, E any](state T, effects []E, meta RunMetadata, pointer string) *RunResult[T, E] {
	return failedResultWithReason(state, effects, meta, pointer, "")
}

func failedResultWithReason[T, E any](state T, effects []E, meta RunMetadata, pointer, reason string) *RunResult[T, E] {
	return &RunResult[T, E]{
		State:            state,
		Status:           RunStatusFailed,
		Effects:          append([]E(nil), effects...),
		RunMeta:          meta,
		ExecutionPointer: ExecutionPointer(pointer),
		Reason:           reason,
	}
}

func markSegmentFailed(meta *RunMetadata) {
	now := time.Now().UTC()
	meta.Segment.EndTime = now
	meta.Segment.EndReason = SegmentEndFail
}

func (r *graphRunner[T, E]) resolveEdge(ctx context.Context, current string, state T) (string, error) {
	if next, ok := r.graph.edges[current]; ok {
		return next, nil
	}

	router, ok := r.graph.conditionalEdges[current]
	if !ok {
		return "", fmt.Errorf("flowy: node %q returned Completed but has no outgoing edge", current)
	}

	next, err := router(ctx, state)
	if err != nil {
		return "", fmt.Errorf("flowy: conditional edge from %q: %w", current, err)
	}
	if next == "" {
		return "", fmt.Errorf("flowy: conditional edge from %q returned empty target", current)
	}
	if allowed, ok := r.graph.conditionalAllowed[current]; ok {
		if _, declared := allowed[next]; !declared {
			return "", fmt.Errorf(
				"flowy: conditional edge from %q returned undeclared target %q",
				current, next,
			)
		}
	}
	if next == EndNode {
		return next, nil
	}
	if _, ok := r.graph.nodes[next]; !ok {
		return "", fmt.Errorf("flowy: conditional edge from %q returned unknown node %q", current, next)
	}
	return next, nil
}

func (r *graphRunner[T, E]) saveSnapshot(
	ctx context.Context,
	snapshot Snapshot[T, E],
	inv runInvocationOptions[T, E],
) error {
	if r.checkpointer == nil {
		return errors.New("flowy: checkpointer is required")
	}
	if err := validateInvariant(snapshot.State, inv); err != nil {
		return err
	}
	state := snapshot.State
	for _, interceptor := range r.interceptors {
		if err := interceptor.BeforeSave(ctx, &state); err != nil {
			return fmt.Errorf("flowy: before_save interceptor: %w", err)
		}
	}
	snapshot.State = state
	return r.checkpointer.Save(ctx, snapshot)
}

func (r *graphRunner[T, E]) scheduleHandoffContinuation(
	runCtx context.Context,
	inv runInvocationOptions[T, E],
	token ResumeToken,
) error {
	if inv.handoffScheduler == nil {
		return nil
	}
	scheduleCtx, cancelSchedule := context.WithTimeout(context.WithoutCancel(runCtx), handoffScheduleTimeout)
	defer cancelSchedule()
	if err := inv.handoffScheduler.ScheduleContinuation(scheduleCtx, token); err != nil {
		return fmt.Errorf("%w: %w", ErrHandoffScheduleFailed, err)
	}
	return nil
}

func (r *graphRunner[T, E]) validateExecutionPointer(ptr ExecutionPointer) error {
	if ptr == "" {
		return ErrInvalidSnapshot
	}
	node := string(ptr)
	if _, ok := r.graph.nodes[node]; !ok {
		return fmt.Errorf("%w: %q", ErrResumeStartNodeNotFound, node)
	}
	return nil
}

func (r *graphRunner[T, E]) emitCheckpointFailed(
	ctx context.Context,
	sink eventSink[T, E],
	threadID, current string,
	state T,
	saveErr error,
) {
	if sink == nil {
		r.logger.DebugContext(ctx, "flowy: checkpoint soft warn without event sink",
			"thread_id", threadID, "err", saveErr)
		return
	}
	if !emitTerminalEvent(ctx, sink, newRunEventCheckpointFailed[T, E](current, state, saveErr)) {
		r.logger.DebugContext(ctx, "flowy: checkpoint failed event not delivered",
			"thread_id", threadID)
	}
}

func (r *graphRunner[T, E]) persistSnapshot(
	ctx context.Context,
	snapshot Snapshot[T, E],
	sink eventSink[T, E],
	current string,
	state T,
	inv runInvocationOptions[T, E],
) (bool, error) {
	saveCtx, cancelSave := context.WithTimeout(context.WithoutCancel(ctx), contextCancelSaveTimeout)
	defer cancelSave()
	saveErr := r.saveSnapshot(saveCtx, snapshot, inv)
	if saveErr == nil {
		return true, nil
	}
	if inv.checkpointPolicy != CheckpointPolicySoftWarn {
		return false, saveErr
	}
	r.emitCheckpointFailed(ctx, sink, snapshot.ThreadID, current, state, saveErr)
	return false, nil
}

func validateInvariant[T, E any](state T, inv runInvocationOptions[T, E]) error {
	if inv.invariantValidator == nil {
		return nil
	}
	if err := inv.invariantValidator(state); err != nil {
		return fmt.Errorf("flowy: invariant violated: %w", err)
	}
	return nil
}

func (r *graphRunner[T, E]) applyRetentionPolicy(ctx context.Context, threadID string) error {
	if r.checkpointer == nil || r.graph.defaults.retentionLimit <= 0 {
		return nil
	}
	return r.checkpointer.Prune(ctx, threadID, r.graph.defaults.retentionLimit)
}

func appliesTerminalPolicies(status RunStatus) bool {
	return status == RunStatusCompleted || status == RunStatusFailed
}

func (r *graphRunner[T, E]) postRunCleanup(
	ctx context.Context,
	threadID string,
	inv runInvocationOptions[T, E],
	result *RunResult[T, E],
) {
	r.logReleaseLeaseError(ctx, threadID, r.releaseLease(ctx, threadID, inv))
	if result != nil && threadID != "" && appliesTerminalPolicies(result.Status) {
		r.tryApplyTerminalPolicies(ctx, threadID, result.Status)
	}
}

func (r *graphRunner[T, E]) logReleaseLeaseError(ctx context.Context, threadID string, err error) {
	if err != nil {
		r.logger.WarnContext(ctx, "flowy: release lease failed",
			"thread_id", threadID, "err", err)
	}
}

func (r *graphRunner[T, E]) tryApplyTerminalPolicies(ctx context.Context, threadID string, status RunStatus) {
	if err := r.applyTerminalPolicies(ctx, threadID, status); err != nil {
		if errors.Is(err, ErrThreadBusy) {
			r.logger.DebugContext(ctx, "flowy: terminal policy skipped, thread busy",
				"thread_id", threadID, "status", status)
			return
		}
		r.logger.WarnContext(ctx, "flowy: terminal policy failed",
			"thread_id", threadID, "status", status, "err", err)
	}
}

func (r *graphRunner[T, E]) applyTerminalPolicies(ctx context.Context, threadID string, status RunStatus) error {
	if r.checkpointer == nil {
		return nil
	}
	if status == RunStatusCompleted && r.graph.defaults.deleteOnSuccess {
		return r.checkpointer.DeleteIfIdle(ctx, threadID)
	}
	return r.applyRetentionPolicy(ctx, threadID)
}

// AsNode composes a graph as a node.
// The inline runner uses an ephemeral checkpointer and does not receive parent RunOptions
// (WithSuspendPointerResolver, WithHandoffScheduler, WithCheckpointErrorPolicy).
// Suspend/Handoff inside AsNode are not resumable: inner ResumeToken is not propagated.
// For suspend/handoff continuity use SubgraphNodeWithSlot.
func (g *Graph[T, E]) AsNode() Node[T, E] {
	return func(ctx context.Context, state T) (T, Directive, error) {
		runner := g.NewRunner(newCaptureCheckpointer[T, E]())
		result, err := runner.Start(ctx, "__inline__", state)
		if err != nil {
			return state, Completed(), err
		}
		switch result.Status {
		case RunStatusSuspended:
			return result.State, Suspend(result.Reason), nil
		case RunStatusHandoff:
			return result.State, Handoff(result.Reason), nil
		case RunStatusContextCanceled:
			return result.State, Completed(), context.Canceled
		case RunStatusCompleted:
			return result.State, Completed(), nil
		case RunStatusFailed:
			reason := result.Reason
			if reason == "" {
				reason = "inline graph failed"
			}
			return result.State, Fail(reason), nil
		default:
			return result.State, Fail("subgraph failed"), nil
		}
	}
}

type captureCheckpointer[T, E any] struct {
	history []Snapshot[T, E]
}

func newCaptureCheckpointer[T, E any]() *captureCheckpointer[T, E] {
	return &captureCheckpointer[T, E]{}
}

type failingCaptureCheckpointer[T, E any] struct {
	captureCheckpointer[T, E]

	failSave bool
	failLoad bool
}

func (f *failingCaptureCheckpointer[T, E]) Save(ctx context.Context, s Snapshot[T, E]) error {
	if f.failSave {
		return errors.New("subgraph seed save failed")
	}
	return f.captureCheckpointer.Save(ctx, s)
}

func (f *failingCaptureCheckpointer[T, E]) Load(ctx context.Context, threadID string) (Snapshot[T, E], error) {
	if f.failLoad {
		var zero Snapshot[T, E]
		return zero, errors.New("subgraph slot load failed")
	}
	return f.captureCheckpointer.Load(ctx, threadID)
}

func (n *captureCheckpointer[T, E]) Save(_ context.Context, s Snapshot[T, E]) error {
	n.history = append(n.history, s)
	return nil
}

func (n *captureCheckpointer[T, E]) Load(_ context.Context, _ string) (Snapshot[T, E], error) {
	if len(n.history) == 0 {
		var zero Snapshot[T, E]
		return zero, ErrThreadNotFound
	}
	return n.history[len(n.history)-1], nil
}

func (n *captureCheckpointer[T, E]) GetHistory(_ context.Context, _ string, limit int) ([]Snapshot[T, E], error) {
	if len(n.history) == 0 {
		return []Snapshot[T, E]{}, nil
	}
	if limit <= 0 || limit > len(n.history) {
		limit = len(n.history)
	}
	out := make([]Snapshot[T, E], 0, limit)
	for i := len(n.history) - 1; i >= len(n.history)-limit; i-- {
		out = append(out, n.history[i])
	}
	return out, nil
}

func (n *captureCheckpointer[T, E]) Prune(_ context.Context, _ string, retainCount int) error {
	if retainCount <= 0 {
		n.history = nil
		return nil
	}
	if len(n.history) <= retainCount {
		return nil
	}
	n.history = append([]Snapshot[T, E](nil), n.history[len(n.history)-retainCount:]...)
	return nil
}

func (n *captureCheckpointer[T, E]) Delete(_ context.Context, _ string) error {
	n.history = nil
	return nil
}

// DeleteIfIdle clears ephemeral history (same as Delete for inline/subgraph runners).
func (n *captureCheckpointer[T, E]) DeleteIfIdle(_ context.Context, _ string) error {
	n.history = nil
	return nil
}
