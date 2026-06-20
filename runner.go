package flowy

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
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
	graph             *Graph[T, E]
	checkpointer      Checkpointer[T, E]
	interceptors      []StateInterceptor[T]
	leaseManager      LeaseManager
	handoffOutbox     HandoffOutbox
	handoffStaleAfter time.Duration
	logger            *slog.Logger
	sessions          sync.Map // threadID -> *runSession
}

type eventSink[T, E any] func(ctx context.Context, event RunEvent[T, E]) bool

type streamCloseKey struct{}

type streamHandle[T, E any] struct {
	events   chan RunEvent[T, E]
	stop     chan struct{}
	drop     chan struct{}
	done     chan struct{}
	once     sync.Once
	dropOnce sync.Once
	err      error
	result   *RunResult[T, E]
	onStop   func()
}

func streamConsumerClosed(ctx context.Context) bool {
	type stopChecker interface {
		stopped() bool
	}
	sc, ok := ctx.Value(streamCloseKey{}).(stopChecker)
	return ok && sc.stopped()
}

func (s *streamHandle[T, E]) Events() <-chan RunEvent[T, E] {
	return s.events
}

func (s *streamHandle[T, E]) RequestStop() {
	s.once.Do(func() {
		close(s.stop)
		if s.onStop != nil {
			s.onStop()
		}
	})
}

func (s *streamHandle[T, E]) Wait() error {
	s.enableEventDrop()
	<-s.done
	return s.err
}

func (s *streamHandle[T, E]) WaitResult() (*RunResult[T, E], error) {
	s.enableEventDrop()
	<-s.done
	return s.result, s.err
}

func (s *streamHandle[T, E]) stopped() bool {
	select {
	case <-s.stop:
		return true
	default:
		return false
	}
}

func (s *streamHandle[T, E]) enableEventDrop() {
	s.dropOnce.Do(func() {
		close(s.drop)
	})
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
	r := &graphRunner[T, E]{ //nolint:exhaustruct // handoffOutbox defaults nil until WithRunnerHandoffOutbox
		graph:             g,
		checkpointer:      checkpointer,
		interceptors:      append([]StateInterceptor[T](nil), interceptors...),
		leaseManager:      nil,
		handoffStaleAfter: DefaultHandoffStaleAfter,
		logger:            slog.Default(),
		sessions:          sync.Map{},
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
	if threadID == "" {
		return nil, fmt.Errorf("%w: empty thread ID", ErrInvalidResumeToken)
	}
	inv, optErr := applyRunOptions(opts...)
	if optErr != nil {
		return nil, optErr
	}
	if leaseErr := r.acquireLease(ctx, threadID, inv); leaseErr != nil {
		return nil, leaseErr
	}

	meta := newRunMetadata()
	mergeRunMetadataInput(&meta, inv.runMetadata)
	runCtx := r.attachInvocation(ctx, inv, &meta)
	result, err := r.execute(
		runCtx,
		threadID,
		r.graph.entryPoint,
		initialState,
		meta,
		nil,
		0,
		nil,
		inv,
	)
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
		emitResumeRejected(ctx, token.ThreadID, "", "empty_token")
		return nil, fmt.Errorf("%w: empty thread ID", ErrInvalidResumeToken)
	}
	inv, optErr := applyRunOptions(opts...)
	if optErr != nil {
		return nil, optErr
	}
	if leaseErr := r.acquireLease(ctx, token.ThreadID, inv); leaseErr != nil {
		return nil, leaseErr
	}

	decision, err := r.evaluateResume(ctx, token, inv)
	if err != nil {
		r.logReleaseLeaseError(
			context.WithoutCancel(ctx),
			token.ThreadID,
			r.releaseLease(context.WithoutCancel(ctx), token.ThreadID, inv),
		)
		return nil, err
	}
	runCtx := injectTelemetryContext(r.attachInvocation(ctx, inv, &decision.RunMeta), decision.RunMeta.TelemetryContext)
	result, runErr := r.execute(
		runCtx,
		token.ThreadID,
		string(decision.ExecutionPointer),
		decision.State,
		decision.RunMeta,
		decision.Effects,
		decision.SnapshotRevision,
		nil,
		inv,
	)
	r.postRunCleanup(context.WithoutCancel(ctx), token.ThreadID, inv, result)
	return result, runErr
}

func (r *graphRunner[T, E]) Stream(
	ctx context.Context,
	threadID string,
	initialState T,
	opts ...RunOption[T, E],
) (StreamHandle[T, E], error) {
	if threadID == "" {
		return nil, fmt.Errorf("%w: empty thread ID", ErrInvalidResumeToken)
	}
	inv, optErr := applyRunOptions(opts...)
	if optErr != nil {
		return nil, optErr
	}
	if leaseErr := r.acquireLease(ctx, threadID, inv); leaseErr != nil {
		return nil, leaseErr
	}
	meta := newRunMetadata()
	mergeRunMetadataInput(&meta, inv.runMetadata)
	runCtx := r.attachInvocation(ctx, inv, &meta)
	return r.startStream(runCtx, threadID, func(
		streamCtx context.Context,
		sink eventSink[T, E],
	) (*RunResult[T, E], error) {
		result, err := r.execute(
			streamCtx,
			threadID,
			r.graph.entryPoint,
			initialState,
			meta,
			nil,
			0,
			sink,
			inv,
		)
		r.postRunCleanup(context.WithoutCancel(runCtx), threadID, inv, result)
		return result, err
	}), nil
}

func (r *graphRunner[T, E]) ResumeStream(
	ctx context.Context,
	token ResumeToken,
	opts ...RunOption[T, E],
) (StreamHandle[T, E], error) {
	if r.checkpointer == nil {
		return nil, errors.New("flowy: checkpointer is required for ResumeStream")
	}
	if token.ThreadID == "" {
		emitResumeRejected(ctx, token.ThreadID, "", "empty_token")
		return nil, fmt.Errorf("%w: empty thread ID", ErrInvalidResumeToken)
	}
	inv, optErr := applyRunOptions(opts...)
	if optErr != nil {
		return nil, optErr
	}
	if leaseErr := r.acquireLease(ctx, token.ThreadID, inv); leaseErr != nil {
		return nil, leaseErr
	}
	decision, err := r.evaluateResume(ctx, token, inv)
	if err != nil {
		r.logReleaseLeaseError(
			context.WithoutCancel(ctx),
			token.ThreadID,
			r.releaseLease(context.WithoutCancel(ctx), token.ThreadID, inv),
		)
		return nil, err
	}
	runCtx := injectTelemetryContext(r.attachInvocation(ctx, inv, &decision.RunMeta), decision.RunMeta.TelemetryContext)
	return r.startStream(runCtx, token.ThreadID, func(
		streamCtx context.Context,
		sink eventSink[T, E],
	) (*RunResult[T, E], error) {
		result, runErr := r.execute(
			streamCtx,
			token.ThreadID,
			string(decision.ExecutionPointer),
			decision.State,
			decision.RunMeta,
			decision.Effects,
			decision.SnapshotRevision,
			sink,
			inv,
		)
		r.postRunCleanup(context.WithoutCancel(runCtx), token.ThreadID, inv, result)
		return result, runErr
	}), nil
}

func (r *graphRunner[T, E]) RequestLocalHandoff(ctx context.Context, threadID string) error {
	if threadID == "" {
		return fmt.Errorf("%w: handoff requires threadID", ErrInvalidResumeToken)
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

func (r *graphRunner[T, E]) EvaluateResume(
	ctx context.Context,
	token ResumeToken,
	opts ...RunOption[T, E],
) (ResumeDecision[T, E], error) {
	if r.checkpointer == nil {
		var zero ResumeDecision[T, E]
		return zero, errors.New("flowy: checkpointer is required for EvaluateResume")
	}
	inv, optErr := applyRunOptions(opts...)
	if optErr != nil {
		var zero ResumeDecision[T, E]
		return zero, optErr
	}
	return r.evaluateResume(ctx, token, inv)
}

func (r *graphRunner[T, E]) EvaluateHandoffRecovery(
	ctx context.Context,
	threadID string,
	opts ...RecoverStaleHandoffOption,
) (ResumeDecision[T, E], error) {
	if r.checkpointer == nil {
		var zero ResumeDecision[T, E]
		return zero, errors.New("flowy: checkpointer is required for EvaluateHandoffRecovery")
	}
	cfg := r.recoverStaleHandoffConfig(opts...)
	if cfg.staleAfter <= 0 {
		cfg.staleAfter = DefaultHandoffStaleAfter
	}
	if threadID == "" {
		var zero ResumeDecision[T, E]
		zero.Status = ResumeDecisionInvalidToken
		zero.Reason = "empty_thread_id"
		zero.Err = ErrInvalidResumeToken
		return zero, ErrInvalidResumeToken
	}
	snapshot, revision, err := r.loadNormalizedSnapshot(ctx, threadID)
	if err != nil {
		decision := newResumeDecision(
			resumeDecisionStatusForError(err),
			ResumeToken{ThreadID: threadID, SnapshotRevision: revision},
			snapshot,
			revision,
			snapshot.ExecutionPointer,
			resumeDecisionReasonForError(err),
			err,
		)
		decision.ThreadID = threadID
		return decision, err
	}
	if ptrErr := r.validateExecutionPointer(snapshot.ExecutionPointer); ptrErr != nil {
		reason := "invalid_pointer"
		if snapshot.ExecutionPointer == "" {
			reason = string(ResumeDecisionInvalidSnapshot)
		}
		decision := newResumeDecision(
			ResumeDecisionInvalidSnapshot,
			ResumeToken{ThreadID: threadID, SnapshotRevision: revision},
			snapshot,
			revision,
			snapshot.ExecutionPointer,
			reason,
			ptrErr,
		)
		return decision, ptrErr
	}
	decision := newResumeDecision(
		ResumeDecisionHandoffNotRecoverable,
		ResumeToken{ThreadID: threadID, SnapshotRevision: revision},
		snapshot,
		revision,
		snapshot.ExecutionPointer,
		"handoff_not_recoverable",
		ErrHandoffNotRecoverable,
	)
	switch snapshot.RunMeta.HandoffStatus {
	case HandoffStatusOrphaned:
		decision.Status = ResumeDecisionHandoffRecoverable
		decision.Reason = "handoff_orphaned"
		decision.Err = ErrHandoffOrphaned
	case HandoffStatusPending:
		if isHandoffPendingStale(snapshot.RunMeta.HandoffPendingAt, cfg.staleAfter) {
			decision.Status = ResumeDecisionHandoffRecoverable
			decision.Reason = "handoff_pending_stale"
			decision.Err = ErrHandoffPending
		} else {
			emitResumeRejected(ctx, threadID, snapshot.ExecutionPointer, "handoff_pending")
			decision.Status = ResumeDecisionHandoffPending
			decision.Reason = "handoff_pending"
			decision.Err = ErrHandoffPending
		}
	case HandoffStatusEnqueued:
		if cfg.forceReenqueue {
			decision.Status = ResumeDecisionHandoffRecoverable
			decision.Reason = "handoff_force_reenqueue"
			decision.Err = nil
		} else {
			decision.Status = ResumeDecisionHandoffAlreadyScheduled
			decision.Reason = "handoff_already_enqueued"
			decision.Err = ErrHandoffAlreadyEnqueued
		}
	case HandoffStatusNone:
		decision.Status = ResumeDecisionHandoffNotRecoverable
		decision.Reason = "handoff_status_none"
		decision.Err = ErrHandoffNotRecoverable
	default:
		decision.Status = ResumeDecisionHandoffNotRecoverable
		decision.Reason = "invalid_handoff_status"
		decision.Err = errors.Join(
			ErrHandoffNotRecoverable,
			fmt.Errorf("%w: handoff status %q", ErrInvalidSnapshot, snapshot.RunMeta.HandoffStatus),
		)
	}
	if decision.Err != nil {
		return decision, decision.Err
	}
	return decision, nil
}

func (r *graphRunner[T, E]) registerRunSession(
	threadID string,
	cancel context.CancelCauseFunc,
) (*runSession, error) {
	if _, loaded := r.sessions.Load(threadID); loaded {
		return nil, fmt.Errorf("%w: thread %q", ErrThreadAlreadyRunning, threadID)
	}
	session := newRunSession(cancel)
	if _, loaded := r.sessions.LoadOrStore(threadID, session); loaded {
		return nil, fmt.Errorf("%w: thread %q", ErrThreadAlreadyRunning, threadID)
	}
	return session, nil
}

func (r *graphRunner[T, E]) unregisterRunSessionIfSame(threadID string, session *runSession) {
	value, ok := r.sessions.Load(threadID)
	if !ok {
		return
	}
	if value == session {
		r.sessions.Delete(threadID)
	}
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
		HandoffStatus:    HandoffStatusNone,
		HandoffPendingAt: time.Time{},
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

func (r *graphRunner[T, E]) acquireLease(
	ctx context.Context,
	threadID string,
	inv runInvocationOptions[T, E],
) error {
	if r.leaseManager == nil {
		return nil
	}
	if _, nativeCP := r.checkpointer.(NativeDeleteIfIdleCheckpointer); nativeCP {
		if _, paired := r.leaseManager.(NativeLeaseManager); !paired {
			return errors.New(
				"flowy: native DeleteIfIdle checkpointer requires paired native adapters/lease manager",
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

func (r *graphRunner[T, E]) releaseLease(
	ctx context.Context,
	threadID string,
	inv runInvocationOptions[T, E],
) error {
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

func (r *graphRunner[T, E]) cancelSessionForConsumerStop(threadID string) {
	value, ok := r.sessions.Load(threadID)
	if !ok {
		return
	}
	session, ok := value.(*runSession)
	if !ok || session.cancel == nil {
		return
	}
	session.cancel(context.Canceled)
}

func (r *graphRunner[T, E]) startStream(
	ctx context.Context,
	threadID string,
	runFn func(context.Context, eventSink[T, E]) (*RunResult[T, E], error),
) StreamHandle[T, E] {
	streamCtx, cancelStream := context.WithCancelCause(ctx)
	stream := &streamHandle[T, E]{
		events:   make(chan RunEvent[T, E], streamEventBufferSize),
		stop:     make(chan struct{}),
		drop:     make(chan struct{}),
		done:     make(chan struct{}),
		once:     sync.Once{},
		dropOnce: sync.Once{},
		err:      nil,
		result:   nil,
		onStop: func() {
			cancelStream(context.Canceled)
			r.cancelSessionForConsumerStop(threadID)
		},
	}
	go func() {
		defer cancelStream(context.Canceled)
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
			case <-stream.drop:
				return true
			}
		}
		runCtx := context.WithValue(streamCtx, streamCloseKey{}, stream)
		result, err := runFn(runCtx, sink)
		if errors.Is(err, context.Canceled) && stream.stopped() && ctx.Err() == nil &&
			!errors.Is(err, ErrCheckpointSkipped) {
			err = nil
		}
		stream.result = result
		stream.err = err
	}()
	return stream
}

//nolint:gocognit,funlen // resume validation pipeline is intentionally linear
func (r *graphRunner[T, E]) evaluateResume(
	ctx context.Context,
	token ResumeToken,
	inv runInvocationOptions[T, E],
) (ResumeDecision[T, E], error) {
	if token.ThreadID == "" {
		emitResumeRejected(ctx, token.ThreadID, "", "empty_token")
		err := fmt.Errorf("%w: empty thread ID", ErrInvalidResumeToken)
		decision := newResumeDecision(
			ResumeDecisionInvalidToken,
			token,
			Snapshot[T, E]{
				ThreadID:         "",
				ExecutionPointer: "",
				Revision:         0,
				State:            *new(T),
				RunMeta:          newRunMetadata(),
				Effects:          nil,
			},
			0,
			"",
			"empty_token",
			err,
		)
		return decision, decision.Err
	}
	// 1) Load base snapshot
	snapshot, revision, err := r.loadNormalizedSnapshot(ctx, token.ThreadID)
	if err != nil {
		if errors.Is(err, ErrInvalidSnapshot) {
			emitResumeRejected(ctx, token.ThreadID, snapshot.ExecutionPointer, "invalid_snapshot")
		}
		decision := newResumeDecision(
			resumeDecisionStatusForError(err),
			token,
			snapshot,
			revision,
			snapshot.ExecutionPointer,
			resumeDecisionReasonForError(err),
			err,
		)
		decision.ThreadID = token.ThreadID
		return decision, err
	}
	if token.SnapshotRevision == 0 {
		emitResumeRejected(ctx, token.ThreadID, snapshot.ExecutionPointer, "zero_revision")
		decision := newResumeDecision(
			ResumeDecisionInvalidToken, token, snapshot, revision, snapshot.ExecutionPointer,
			"zero_revision", ErrConcurrencyConflict,
		)
		return decision, decision.Err
	}
	if token.SnapshotRevision != revision {
		emitResumeRejected(ctx, token.ThreadID, snapshot.ExecutionPointer, "stale_token")
		err := fmt.Errorf(
			"%w: resume token snapshot revision %d, snapshot revision %d",
			ErrConcurrencyConflict, token.SnapshotRevision, revision,
		)
		decision := newResumeDecision(
			ResumeDecisionStaleToken,
			ResumeToken{ThreadID: token.ThreadID, SnapshotRevision: revision},
			snapshot,
			revision,
			snapshot.ExecutionPointer,
			"stale_token", err,
		)
		return decision, err
	}
	switch snapshot.RunMeta.HandoffStatus {
	case HandoffStatusNone, HandoffStatusEnqueued:
		// Resume allowed after handoff enqueue completed or for non-handoff snapshots.
	case HandoffStatusPending:
		status := ResumeDecisionHandoffPending
		reason := "handoff_pending"
		if isHandoffPendingStale(snapshot.RunMeta.HandoffPendingAt, r.handoffStaleAfter) {
			status = ResumeDecisionHandoffRecoverable
			reason = "handoff_pending_stale"
		}
		emitResumeRejected(ctx, token.ThreadID, snapshot.ExecutionPointer, reason)
		decision := newResumeDecision(
			status,
			token,
			snapshot,
			revision,
			snapshot.ExecutionPointer,
			reason,
			ErrHandoffPending,
		)
		return decision, decision.Err
	case HandoffStatusOrphaned:
		emitResumeRejected(ctx, token.ThreadID, snapshot.ExecutionPointer, "handoff_orphaned")
		decision := newResumeDecision(
			ResumeDecisionHandoffRecoverable, token, snapshot, revision, snapshot.ExecutionPointer,
			"handoff_orphaned", ErrHandoffOrphaned,
		)
		return decision, decision.Err
	default:
		if snapshot.RunMeta.HandoffStatus != HandoffStatusNone {
			emitResumeRejected(ctx, token.ThreadID, snapshot.ExecutionPointer, "invalid_handoff_status")
			err := fmt.Errorf(
				"%w: handoff status %q",
				ErrInvalidSnapshot,
				snapshot.RunMeta.HandoffStatus,
			)
			decision := newResumeDecision(
				ResumeDecisionInvalidSnapshot, token, snapshot, revision, snapshot.ExecutionPointer,
				"invalid_handoff_status", err,
			)
			return decision, err
		}
	}
	state := snapshot.State
	for _, interceptor := range r.interceptors {
		if loadErr := interceptor.AfterLoad(ctx, &state); loadErr != nil {
			err := fmt.Errorf(
				"flowy: after_load interceptor: %w",
				loadErr,
			)
			decision := newResumeDecision(
				ResumeDecisionInvalidSnapshot, token, snapshot, revision, snapshot.ExecutionPointer,
				"after_load_failed", err,
			)
			return decision, err
		}
	}

	// 2) Overlay merge
	if inv.overlay != nil {
		if inv.overlayMerger == nil {
			decision := newResumeDecision(
				ResumeDecisionInvalidSnapshot, token, snapshot, revision, snapshot.ExecutionPointer,
				"overlay_merger_required", ErrOverlayMergerRequired,
			)
			return decision, decision.Err
		}
		state = inv.overlayMerger(state, *inv.overlay)
	}
	meta := snapshot.RunMeta
	if meta.HandoffStatus == HandoffStatusEnqueued {
		meta.HandoffStatus = HandoffStatusNone
		meta.HandoffPendingAt = time.Time{}
	}
	if meta.RetryCounts == nil {
		meta.RetryCounts = map[string]int{}
	}
	resetSegmentCounters(&meta)
	mergeRunMetadataInput(&meta, inv.runMetadata)

	activePointer := snapshot.ExecutionPointer
	var policyErr error
	state, activePointer, policyErr = applyResumeTargetPolicy(ctx, state, activePointer, inv)
	if policyErr != nil {
		decision := newResumeDecision(
			ResumeDecisionInvalidSnapshot, token, snapshot, revision, activePointer,
			"resume_target_policy_failed", policyErr,
		)
		decision.State = state
		decision.RunMeta = meta
		return decision, policyErr
	}

	if activePointer == "" {
		emitResumeRejected(ctx, token.ThreadID, snapshot.ExecutionPointer, "invalid_snapshot")
		decision := newResumeDecision(
			ResumeDecisionInvalidSnapshot, token, snapshot, revision, activePointer,
			"invalid_snapshot", ErrInvalidSnapshot,
		)
		decision.State = state
		decision.RunMeta = meta
		return decision, decision.Err
	}
	startNode := string(activePointer)

	if _, ok := r.graph.nodes[startNode]; !ok {
		emitResumeRejected(ctx, token.ThreadID, activePointer, "invalid_pointer")
		err := invalidResumePointerError(startNode)
		decision := newResumeDecision(
			ResumeDecisionInvalidSnapshot, token, snapshot, revision, activePointer,
			"invalid_pointer", err,
		)
		decision.State = state
		decision.RunMeta = meta
		return decision, err
	}

	decision := newResumeDecision(
		ResumeDecisionReady, token, snapshot, revision, activePointer,
		string(ResumeDecisionReady), nil,
	)
	decision.State = state
	decision.RunMeta = meta
	decision.Effects = append([]E(nil), snapshot.Effects...)
	return decision, nil
}

func resetSegmentCounters(meta *RunMetadata) {
	meta.Segment = newSegmentInfo()
	meta.SegmentStartTime = time.Now().UTC()
	meta.StepCount = 0
	if meta.BudgetCounts == nil {
		meta.BudgetCounts = map[string]int{}
	}
}

func newResumeDecision[T, E any](
	status ResumeDecisionStatus,
	token ResumeToken,
	snapshot Snapshot[T, E],
	revision uint64,
	pointer ExecutionPointer,
	reason string,
	err error,
) ResumeDecision[T, E] {
	return ResumeDecision[T, E]{
		Status:           status,
		ThreadID:         snapshot.ThreadID,
		ResumeToken:      token,
		Snapshot:         snapshot,
		SnapshotRevision: revision,
		ExecutionPointer: pointer,
		HandoffStatus:    snapshot.RunMeta.HandoffStatus,
		Reason:           reason,
		Err:              err,
		State:            snapshot.State,
		RunMeta:          snapshot.RunMeta,
		Effects:          append([]E(nil), snapshot.Effects...),
	}
}

func resumeDecisionStatusForError(err error) ResumeDecisionStatus {
	switch {
	case errors.Is(err, ErrThreadNotFound):
		return ResumeDecisionThreadNotFound
	case errors.Is(err, ErrInvalidSnapshot), errors.Is(err, ErrSnapshotEnvelopeInvalid):
		return ResumeDecisionInvalidSnapshot
	default:
		return ResumeDecisionLoadFailed
	}
}

func resumeDecisionReasonForError(err error) string {
	switch {
	case errors.Is(err, ErrThreadNotFound):
		return string(ResumeDecisionThreadNotFound)
	case errors.Is(err, ErrInvalidSnapshot), errors.Is(err, ErrSnapshotEnvelopeInvalid):
		return string(ResumeDecisionInvalidSnapshot)
	default:
		return string(ResumeDecisionLoadFailed)
	}
}

func normalizeLoadedSnapshot[T, E any](
	threadID string,
	snapshot Snapshot[T, E],
	revision uint64,
) (Snapshot[T, E], error) {
	if threadID == "" {
		return snapshot, fmt.Errorf("%w: empty thread ID", ErrInvalidSnapshot)
	}
	if snapshot.ThreadID == "" {
		snapshot.ThreadID = threadID
	} else if snapshot.ThreadID != threadID {
		return snapshot, fmt.Errorf(
			"%w: snapshot thread %q != requested thread %q",
			ErrInvalidSnapshot,
			snapshot.ThreadID,
			threadID,
		)
	}
	if revision == 0 {
		return snapshot, fmt.Errorf("%w: zero snapshot revision", ErrInvalidSnapshot)
	}
	if snapshot.Revision == 0 {
		snapshot.Revision = revision
	} else if snapshot.Revision != revision {
		return snapshot, fmt.Errorf(
			"%w: snapshot revision %d != storage revision %d",
			ErrInvalidSnapshot,
			snapshot.Revision,
			revision,
		)
	}
	if snapshot.ExecutionPointer == "" {
		return snapshot, fmt.Errorf("%w: empty execution pointer", ErrInvalidSnapshot)
	}
	return snapshot, nil
}

func (r *graphRunner[T, E]) loadNormalizedSnapshot(
	ctx context.Context,
	threadID string,
) (Snapshot[T, E], uint64, error) {
	snapshot, revision, err := r.checkpointer.Load(ctx, threadID)
	if err != nil {
		if errors.Is(err, ErrThreadNotFound) {
			return snapshot, revision, err
		}
		if errors.Is(err, ErrSnapshotEnvelopeInvalid) || errors.Is(err, ErrInvalidSnapshot) {
			return snapshot, revision, ensureSnapshotEnvelopeInvalid(err)
		}
		return snapshot, revision, err
	}
	normalized, normErr := normalizeLoadedSnapshot(threadID, snapshot, revision)
	if normErr != nil {
		return normalized, revision, ensureSnapshotEnvelopeInvalid(normErr)
	}
	return normalized, revision, nil
}

func ensureSnapshotEnvelopeInvalid(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, ErrSnapshotEnvelopeInvalid) {
		return err
	}
	return fmt.Errorf("%w: %w", ErrSnapshotEnvelopeInvalid, err)
}

//nolint:gocognit,funlen,nonamedreturns // central run loop; named retErr for session-scoped defer finish
func (r *graphRunner[T, E]) execute(
	ctx context.Context,
	threadID string,
	startNode string,
	state T,
	meta RunMetadata,
	effects []E,
	revision uint64,
	sink eventSink[T, E],
	inv runInvocationOptions[T, E],
) (result *RunResult[T, E], retErr error) {
	current := startNode
	runCtx, cancelRun := context.WithCancelCause(ctx)
	runCtx = withRunThreadID(runCtx, threadID)
	session, regErr := r.registerRunSession(threadID, cancelRun)
	if regErr != nil {
		cancelRun(context.Canceled)
		return nil, regErr
	}
	defer func() {
		session.finish(retErr)
		r.unregisterRunSessionIfSame(threadID, session)
	}()

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
			emitTerminalEvent(
				runCtx,
				sink,
				newRunEventFailed[T, E](current, state, ErrLeaseLost, ErrLeaseLost.Error()),
			)
			return failedResultWithReason(
				state,
				effects,
				meta,
				current,
				ErrLeaseLost.Error(),
			), ErrLeaseLost
		}
		if ctxErr := runCtx.Err(); ctxErr != nil {
			return r.handleContextCancellation(
				runCtx, threadID, current, state, meta, effects, revision, sink, ctxErr, inv,
				streamConsumerClosed(runCtx),
			)
		}

		nodeCtx := withNodeName(runCtx, current)
		step, stepErr := r.runNodeStep(nodeCtx, runCtx, current, state, meta, effects, sink, inv)
		if stepErr != nil {
			if errors.Is(context.Cause(runCtx), ErrLeaseLost) {
				markSegmentFailed(&step.meta)
				emitTerminalEvent(
					runCtx,
					sink,
					newRunEventFailed[T, E](
						current,
						step.state,
						ErrLeaseLost,
						ErrLeaseLost.Error(),
					),
				)
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
						runCtx,
						threadID,
						current,
						step.state,
						step.meta,
						step.effects,
						revision,
						sink,
						inv,
					)
				}
				return r.handleContextCancellation(
					runCtx,
					threadID,
					current,
					step.state,
					step.meta,
					step.effects,
					revision,
					sink,
					runCtx.Err(),
					inv,
					streamConsumerClosed(runCtx),
				)
			}
			return failedResultWithReason(
				step.state, step.effects, step.meta, current, failReasonForStepErr(stepErr),
			), stepErr
		}
		if step.emitCanceled {
			if errors.Is(context.Cause(runCtx), ErrHandoffRequested) {
				return r.handleHandoff(
					runCtx,
					threadID,
					current,
					step.state,
					step.meta,
					step.effects,
					revision,
					sink,
					inv,
				)
			}
			if errors.Is(context.Cause(runCtx), ErrLeaseLost) {
				markSegmentFailed(&step.meta)
				emitTerminalEvent(
					runCtx,
					sink,
					newRunEventFailed[T, E](
						current,
						step.state,
						ErrLeaseLost,
						ErrLeaseLost.Error(),
					),
				)
				return failedResultWithReason(
					step.state,
					step.effects,
					step.meta,
					current,
					ErrLeaseLost.Error(),
				), ErrLeaseLost
			}
			return r.handleContextCancellation(
				runCtx,
				threadID,
				current,
				step.state,
				step.meta,
				step.effects,
				revision,
				sink,
				context.Canceled,
				inv,
				true,
			)
		}
		if errors.Is(context.Cause(runCtx), ErrHandoffRequested) {
			return r.handleHandoff(
				runCtx, threadID, current, step.state, step.meta, step.effects, revision, sink, inv,
			)
		}
		if errors.Is(context.Cause(runCtx), ErrLeaseLost) {
			markSegmentFailed(&step.meta)
			emitTerminalEvent(
				runCtx,
				sink,
				newRunEventFailed[T, E](current, step.state, ErrLeaseLost, ErrLeaseLost.Error()),
			)
			return failedResultWithReason(
				step.state,
				step.effects,
				step.meta,
				current,
				ErrLeaseLost.Error(),
			), ErrLeaseLost
		}
		if runCtx.Err() != nil {
			return r.handleContextCancellation(
				runCtx,
				threadID,
				current,
				step.state,
				step.meta,
				step.effects,
				revision,
				sink,
				runCtx.Err(),
				inv,
				streamConsumerClosed(runCtx),
			)
		}

		state = step.state
		meta = step.meta
		effects = step.effects
		base := step.base

		if meta.StepCount > limit {
			markSegmentFailed(&meta)
			emitTerminalEvent(
				runCtx,
				sink,
				newRunEventFailed[T, E](
					current,
					state,
					ErrMaxStepsExceeded,
					ErrMaxStepsExceeded.Error(),
				),
			)
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
			emitTerminalEvent(
				runCtx,
				sink,
				newRunEventFailed[T, E](current, state, err, err.Error()),
			)
			return failedResultWithReason(state, effects, meta, current, err.Error()), err
		}

		if errors.Is(context.Cause(runCtx), ErrLeaseLost) {
			markSegmentFailed(&meta)
			emitTerminalEvent(
				runCtx,
				sink,
				newRunEventFailed[T, E](current, state, ErrLeaseLost, ErrLeaseLost.Error()),
			)
			return failedResultWithReason(
				state,
				effects,
				meta,
				current,
				ErrLeaseLost.Error(),
			), ErrLeaseLost
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
	revision uint64,
	sink eventSink[T, E],
	ctxErr error,
	inv runInvocationOptions[T, E],
	consumerClose bool,
) (*RunResult[T, E], error) {
	meta.TelemetryContext = extractTelemetryContext(runCtx)
	meta.Segment.EndTime = time.Now().UTC()
	meta.Segment.EndReason = SegmentEndContextCanceled
	result := newRunResultContextCanceled(state, effects, meta, current)

	newRev, persisted, saveErr := r.persistSnapshot(runCtx, revision, Snapshot[T, E]{
		ThreadID:         threadID,
		ExecutionPointer: ExecutionPointer(current),
		Revision:         0,
		State:            state,
		RunMeta:          meta,
		Effects:          append([]E(nil), effects...),
	}, sink, current, state, inv)
	if saveErr != nil {
		result.Reason = ReasonContextCanceledSaveFailed
		emitTerminalEvent(
			runCtx,
			sink,
			newRunEventContextCanceled[T, E](current, state, result.Reason),
		)
		return result, fmt.Errorf("flowy: context canceled and save failed: %w", saveErr)
	}
	if !persisted && inv.checkpointPolicy == CheckpointPolicySkipOnSaveError {
		result.Reason = ReasonContextCanceledCheckpointSkipped
	}
	if persisted {
		result.ResumeToken = ResumeToken{ThreadID: threadID, SnapshotRevision: newRev}
	}
	var policyErr error
	if persisted {
		policyCtx, cancelPolicy := context.WithTimeout(
			context.WithoutCancel(runCtx),
			contextCancelSaveTimeout,
		)
		policyErr = r.applyRetentionPolicy(policyCtx, threadID)
		cancelPolicy()
		if policyErr != nil {
			result.Reason = retentionFailedReason(result.Reason)
		}
	}
	emitTerminalEvent(runCtx, sink, newRunEventContextCanceled[T, E](current, state, result.Reason))
	if policyErr != nil {
		return result, fmt.Errorf("flowy: context canceled, retention failed: %w", policyErr)
	}
	if !persisted && inv.checkpointPolicy == CheckpointPolicySkipOnSaveError {
		if consumerClose || runCtx.Err() == nil {
			return result, ErrCheckpointSkipped
		}
		return result, fmt.Errorf("flowy: %w", ctxErr)
	}
	return result, fmt.Errorf("flowy: %w", ctxErr)
}

func directiveResumePointer(current string, directive Directive) ExecutionPointer {
	if directive.resumeAt != "" {
		return directive.resumeAt
	}
	return ExecutionPointer(current)
}

//nolint:funlen,gocognit // handoff FSM: save, patch status, enqueue, retention
func (r *graphRunner[T, E]) completeHandoffTerminal(
	runCtx context.Context,
	threadID, current string,
	state T,
	meta RunMetadata,
	effects []E,
	revision uint64,
	reason string,
	resumeAt ExecutionPointer,
	sink eventSink[T, E],
	inv runInvocationOptions[T, E],
	signalSession bool,
) (*RunResult[T, E], error) {
	meta.TelemetryContext = extractTelemetryContext(runCtx)
	meta.Segment.EndTime = time.Now().UTC()
	meta.Segment.EndReason = SegmentEndHandoff
	resumePtr := resumeAt
	if resumePtr == "" {
		resumePtr = ExecutionPointer(current)
	}
	if ptrErr := r.validateExecutionPointer(resumePtr); ptrErr != nil {
		handoffErr := fmt.Errorf("flowy: handoff resume target invalid: %w", ptrErr)
		emitTerminalEvent(
			runCtx,
			sink,
			newRunEventFailed[T, E](current, state, ptrErr, ReasonHandoffResumeTargetInvalid),
		)
		return newRunResultFailed(
			state,
			effects,
			meta,
			current,
			ReasonHandoffResumeTargetInvalid,
		), handoffErr
	}
	savedPointer := string(resumePtr)
	outbox := r.resolveHandoffOutbox(inv)
	if outbox != nil {
		meta.HandoffStatus = HandoffStatusPending
		meta.HandoffPendingAt = time.Now().UTC()
	} else {
		meta.HandoffStatus = HandoffStatusNone
		meta.HandoffPendingAt = time.Time{}
	}
	result := newRunResultHandoff(state, effects, meta, savedPointer, reason)
	snapshot := Snapshot[T, E]{
		ThreadID:         threadID,
		ExecutionPointer: resumePtr,
		Revision:         0,
		State:            state,
		RunMeta:          meta,
		Effects:          append([]E(nil), effects...),
	}
	if outbox != nil {
		if txRev, done, txErr := r.tryTransactionalHandoffSave(
			runCtx, threadID, revision, snapshot, meta, inv, outbox, result,
		); done {
			if txErr != nil {
				handoffErr := fmt.Errorf("flowy: transactional handoff failed: %w", txErr)
				emitTerminalEvent(
					runCtx,
					sink,
					newRunEventFailed[T, E](savedPointer, state, txErr, ReasonHandoffSaveFailed),
				)
				failed := newRunResultFailed(
					state,
					effects,
					meta,
					savedPointer,
					ReasonHandoffSaveFailed,
				)
				clearHandoffRunMeta(&failed.RunMeta)
				return failed, handoffErr
			}
			result.ResumeToken = ResumeToken{ThreadID: threadID, SnapshotRevision: txRev}
			return r.finalizeHandoffTerminal(
				runCtx,
				threadID,
				savedPointer,
				state,
				result.Reason,
				result,
				sink,
				signalSession,
				true,
			)
		}
	}
	newRev, persisted, saveErr := r.persistSnapshot(runCtx, revision, snapshot, sink, current, state, inv)
	if saveErr != nil {
		handoffErr := fmt.Errorf("flowy: handoff save failed: %w", saveErr)
		emitTerminalEvent(
			runCtx,
			sink,
			newRunEventFailed[T, E](savedPointer, state, saveErr, ReasonHandoffSaveFailed),
		)
		failed := newRunResultFailed(
			state,
			effects,
			meta,
			savedPointer,
			ReasonHandoffSaveFailed,
		)
		clearHandoffRunMeta(&failed.RunMeta)
		return failed, handoffErr
	}
	if !persisted && inv.checkpointPolicy == CheckpointPolicySkipOnSaveError {
		result.Reason = ReasonHandoffCheckpointSkipped
		if outbox != nil {
			clearHandoffRunMeta(&result.RunMeta)
		}
	}
	if persisted {
		result.ResumeToken = ResumeToken{ThreadID: threadID, SnapshotRevision: newRev}
		if outbox != nil {
			if earlyRes, done, earlyErr := r.dispatchPersistedHandoffOutbox(
				runCtx, threadID, newRev, snapshot, meta, inv, outbox, resumePtr,
				savedPointer, state, effects, result, sink,
			); done {
				return earlyRes, earlyErr
			}
		}
	}
	return r.finalizeHandoffTerminal(
		runCtx,
		threadID,
		savedPointer,
		state,
		result.Reason,
		result,
		sink,
		signalSession,
		persisted,
	)
}

func (r *graphRunner[T, E]) handoffAfterEnqueueFailure(
	runCtx context.Context,
	threadID, savedPointer string,
	state T,
	result *RunResult[T, E],
	sink eventSink[T, E],
	enqueueErr error,
) (*RunResult[T, E], error) {
	policyCtx, cancelPolicy := context.WithTimeout(
		context.WithoutCancel(runCtx),
		contextCancelSaveTimeout,
	)
	policyErr := r.applyRetentionPolicy(policyCtx, threadID)
	cancelPolicy()

	finalReason := result.Reason
	if policyErr != nil {
		finalReason = retentionFailedReason(finalReason)
		result.Reason = finalReason
	}
	r.emitHandoffTerminalEvent(runCtx, sink, threadID, savedPointer, state, finalReason, true)

	retErr := enqueueErr
	if policyErr != nil {
		retentionErr := fmt.Errorf("flowy: handoff retention failed: %w", policyErr)
		retErr = errors.Join(enqueueErr, retentionErr)
	}
	return result, retErr
}

func (r *graphRunner[T, E]) finalizeHandoffTerminal(
	runCtx context.Context,
	threadID, savedPointer string,
	state T,
	reason string,
	result *RunResult[T, E],
	sink eventSink[T, E],
	signalSession bool,
	persisted bool,
) (*RunResult[T, E], error) {
	var policyErr error
	finalReason := reason
	if persisted {
		policyCtx, cancelPolicy := context.WithTimeout(
			context.WithoutCancel(runCtx),
			contextCancelSaveTimeout,
		)
		policyErr = r.applyRetentionPolicy(policyCtx, threadID)
		cancelPolicy()
		if policyErr != nil {
			finalReason = retentionFailedReason(reason)
			result.Reason = finalReason
		}
	}
	r.emitHandoffTerminalEvent(runCtx, sink, threadID, savedPointer, state, finalReason, persisted)
	return r.completeHandoffSession(result, policyErr, persisted, signalSession)
}

func (r *graphRunner[T, E]) emitHandoffTerminalEvent(
	runCtx context.Context,
	sink eventSink[T, E],
	threadID, savedPointer string,
	state T,
	reason string,
	persisted bool,
) {
	if !emitTerminalEvent(runCtx, sink, newRunEventHandoff[T, E](savedPointer, state, reason)) {
		msg := "flowy: handoff terminal event not delivered"
		if persisted {
			msg = "flowy: handoff persisted but terminal event not delivered"
		}
		r.logger.DebugContext(runCtx, msg, "thread_id", threadID)
	}
}

func (r *graphRunner[T, E]) completeHandoffSession(
	result *RunResult[T, E],
	policyErr error,
	persisted bool,
	signalSession bool,
) (*RunResult[T, E], error) {
	if policyErr != nil {
		return result, fmt.Errorf("flowy: handoff retention failed: %w", policyErr)
	}
	if !persisted && signalSession {
		return result, ErrCheckpointSkipped
	}
	return result, nil
}

func (r *graphRunner[T, E]) handleHandoff(
	runCtx context.Context,
	threadID, current string,
	state T,
	meta RunMetadata,
	effects []E,
	revision uint64,
	sink eventSink[T, E],
	inv runInvocationOptions[T, E],
) (*RunResult[T, E], error) {
	reason := "background_handoff"
	if cause := context.Cause(runCtx); cause != nil && !errors.Is(cause, ErrHandoffRequested) {
		reason = cause.Error()
	}
	return r.completeHandoffTerminal(
		runCtx, threadID, current, state, meta, effects, revision, reason, "", sink, inv, true,
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
		emitTerminalEvent(runCtx, sink, newRunEventFailed[T, E](current, state, err, err.Error()))
		return blankNodeStepOutcome(state, meta, effects), err
	}

	if !emitEvent(nodeCtx, sink, newRunEventNodeStarted[T, E](current, state)) {
		return canceledNodeStepOutcome(state, meta, effects), nil
	}

	nodeStart := time.Now()
	update, directive, err := node.handler(nodeCtx, state)
	nodeDuration := time.Since(nodeStart)
	if err != nil {
		emitTerminalEvent(runCtx, sink, newRunEventFailed[T, E](current, state, err, err.Error()))
		return blankNodeStepOutcome(
				state,
				meta,
				effects,
			), fmt.Errorf(
				"flowy: node %q: %w",
				current,
				err,
			)
	}

	state = r.graph.reducer(state, update)
	meta.StepCount++

	if inv.invariantValidator != nil {
		if invErr := inv.invariantValidator(state); invErr != nil {
			emitTerminalEvent(
				runCtx,
				sink,
				newRunEventFailed[T, E](current, state, invErr, invErr.Error()),
			)
			return blankNodeStepOutcome(state, meta, effects), invErr
		}
	}

	base, nodeEffects, err := UnwrapDirective[E](directive)
	if err != nil {
		emitTerminalEvent(runCtx, sink, newRunEventFailed[T, E](current, state, err, err.Error()))
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
	revision uint64,
	base Directive,
	sink eventSink[T, E],
	inv runInvocationOptions[T, E],
) directiveStep[T, E] {
	switch base.kind {
	case directiveCompleted:
		return r.applyDirectiveCompleted(
			runCtx,
			nodeCtx,
			threadID,
			current,
			state,
			meta,
			effects,
			sink,
		)
	case directiveNext:
		return r.terminalFailDirectiveStep(
			runCtx, sink, current, state, meta, effects, ErrRemovedNext,
		)
	case directiveEnd:
		return r.finishCompleted(
			runCtx,
			threadID,
			current,
			state,
			meta,
			effects,
			sink,
			SegmentEndComplete,
		)
	case directiveSuspend:
		return r.applyDirectiveSuspend(
			runCtx,
			threadID,
			current,
			state,
			meta,
			effects,
			revision,
			base,
			sink,
			inv,
		)
	case directiveHandoff:
		return r.applyDirectiveHandoff(
			runCtx,
			threadID,
			current,
			state,
			meta,
			effects,
			revision,
			base,
			sink,
			inv,
		)
	case directiveRetry:
		return r.applyDirectiveRetry(runCtx, current, state, meta, effects, base, sink)
	case directiveFail:
		return r.applyDirectiveFail(runCtx, threadID, current, state, meta, effects, base, sink)
	default:
		unsupported := errors.New("flowy: node returned unsupported directive")
		return r.terminalFailDirectiveStep(
			runCtx, sink, current, state, meta, effects, unsupported,
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
	emitTerminalEvent(
		runCtx,
		sink,
		newRunEventFailed[T, E](current, state, errors.New(base.reason), base.reason),
	)
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
		emitTerminalEvent(runCtx, sink, newRunEventFailed[T, E](current, state, err, err.Error()))
		return terminalDirectiveStep(
			failedResultWithReason(state, effects, meta, current, err.Error()),
			err,
		)
	}
	if nextNode == EndNode {
		return r.finishCompleted(
			runCtx,
			threadID,
			current,
			state,
			meta,
			effects,
			sink,
			SegmentEndComplete,
		)
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
	revision uint64,
	base Directive,
	sink eventSink[T, E],
	inv runInvocationOptions[T, E],
) directiveStep[T, E] {
	meta.TelemetryContext = extractTelemetryContext(runCtx)
	meta.Segment.EndTime = time.Now().UTC()
	meta.Segment.EndReason = SegmentEndSuspend
	resumePtr := directiveResumePointer(current, base)
	if validateErr := r.validateExecutionPointer(resumePtr); validateErr != nil {
		emitTerminalEvent(
			runCtx,
			sink,
			newRunEventFailed[T, E](current, state, validateErr, ReasonSuspendResumeTargetInvalid),
		)
		return terminalDirectiveStep(
			failedResultWithReason(
				state,
				effects,
				meta,
				current,
				ReasonSuspendResumeTargetInvalid,
			),
			fmt.Errorf("flowy: suspend resume target invalid: %w", validateErr),
		)
	}
	savedPointer := string(resumePtr)
	snapshot := Snapshot[T, E]{
		ThreadID:         threadID,
		ExecutionPointer: resumePtr,
		Revision:         0,
		State:            state,
		RunMeta:          meta,
		Effects:          append([]E(nil), effects...),
	}
	newRev, persisted, saveErr := r.persistSnapshot(runCtx, revision, snapshot, sink, current, state, inv)
	if saveErr != nil {
		emitTerminalEvent(
			runCtx,
			sink,
			newRunEventFailed[T, E](savedPointer, state, saveErr, ReasonSuspendSaveFailed),
		)
		return terminalDirectiveStep(
			failedResultWithReason(state, effects, meta, savedPointer, ReasonSuspendSaveFailed),
			fmt.Errorf("flowy: suspend save failed: %w", saveErr),
		)
	}
	suspendReason := base.reason
	if suspendReason == "" {
		suspendReason = "suspended"
	}
	if !persisted && inv.checkpointPolicy == CheckpointPolicySkipOnSaveError {
		suspendReason = ReasonSuspendedCheckpointSkipped
	}
	var policyErr error
	if persisted {
		policyCtx, cancelPolicy := context.WithTimeout(
			context.WithoutCancel(runCtx),
			contextCancelSaveTimeout,
		)
		policyErr = r.applyRetentionPolicy(policyCtx, threadID)
		cancelPolicy()
		if policyErr != nil {
			suspendReason = retentionFailedReason(suspendReason)
		}
	}
	suspendedResult := newRunResultSuspended(state, effects, meta, savedPointer, suspendReason)
	if persisted {
		suspendedResult.ResumeToken = ResumeToken{ThreadID: threadID, SnapshotRevision: newRev}
	}
	if !emitTerminalEvent(
		runCtx,
		sink,
		newRunEventSuspendedNoError[T, E](savedPointer, state, suspendReason),
	) {
		msg := "flowy: suspend terminal event not delivered"
		if persisted {
			msg = "flowy: suspend persisted but terminal event not delivered"
		}
		r.logger.DebugContext(runCtx, msg, "thread_id", threadID)
		if policyErr != nil {
			return terminalDirectiveStep(
				suspendedResult,
				fmt.Errorf("flowy: suspend retention failed: %w", policyErr),
			)
		}
		return terminalDirectiveStep(suspendedResult, nil)
	}
	if policyErr != nil {
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
	revision uint64,
	base Directive,
	sink eventSink[T, E],
	inv runInvocationOptions[T, E],
) directiveStep[T, E] {
	reason := base.reason
	if reason == "" {
		reason = string(RunStatusHandoff)
	}
	result, err := r.completeHandoffTerminal(
		runCtx,
		threadID,
		current,
		state,
		meta,
		effects,
		revision,
		reason,
		directiveResumePointer(current, base),
		sink,
		inv,
		false,
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
		return r.terminalFailDirectiveStep(runCtx, sink, current, state, meta, effects, err)
	}
	meta.RetryCounts[current]++
	if meta.RetryCounts[current] > base.maxAttempts {
		markSegmentFailed(&meta)
		emitTerminalEvent(
			runCtx,
			sink,
			newRunEventFailed[T, E](
				current,
				state,
				ErrRetryBudgetExceeded,
				ErrRetryBudgetExceeded.Error(),
			),
		)
		return terminalDirectiveStep(
			failedResultWithReason(state, effects, meta, current, ErrRetryBudgetExceeded.Error()),
			ErrRetryBudgetExceeded,
		)
	}
	fallback, ok := r.graph.retryRoutes[current]
	if !ok {
		err := fmt.Errorf("flowy: node %q returned Retry without AddRetryRoute", current)
		return r.terminalFailDirectiveStep(runCtx, sink, current, state, meta, effects, err)
	}
	return continueDirectiveStep[T, E](fallback)
}

func (r *graphRunner[T, E]) terminalFailDirectiveStep(
	runCtx context.Context,
	sink eventSink[T, E],
	current string,
	state T,
	meta RunMetadata,
	effects []E,
	err error,
) directiveStep[T, E] {
	emitTerminalEvent(runCtx, sink, newRunEventFailed[T, E](current, state, err, err.Error()))
	return terminalDirectiveStep(
		failedResultWithReason(state, effects, meta, current, err.Error()),
		err,
	)
}

// failReasonForStepErr aligns RunResult.Reason with EventFailed.Reason for node-step errors.
func failReasonForStepErr(err error) string {
	if err == nil {
		return ""
	}
	if u := errors.Unwrap(err); u != nil && strings.HasPrefix(err.Error(), "flowy: node ") {
		return u.Error()
	}
	return err.Error()
}

func failedResultWithReason[T, E any](
	state T,
	effects []E,
	meta RunMetadata,
	pointer, reason string,
) *RunResult[T, E] {
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

func (r *graphRunner[T, E]) resolveEdge(
	ctx context.Context,
	current string,
	state T,
) (string, error) {
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
		return "", fmt.Errorf(
			"flowy: conditional edge from %q returned unknown node %q",
			current,
			next,
		)
	}
	return next, nil
}

func (r *graphRunner[T, E]) validateExecutionPointer(ptr ExecutionPointer) error {
	if ptr == "" {
		return ErrInvalidSnapshot
	}
	node := string(ptr)
	if _, ok := r.graph.nodes[node]; !ok {
		return invalidResumePointerError(node)
	}
	return nil
}

func invalidResumePointerError(node string) error {
	return errors.Join(ErrInvalidSnapshot, fmt.Errorf("%w: %q", ErrResumeStartNodeNotFound, node))
}

func (r *graphRunner[T, E]) emitCheckpointFailed(
	ctx context.Context,
	sink eventSink[T, E],
	threadID, current string,
	state T,
	saveErr error,
) {
	emitCheckpointSoftError(ctx, threadID, ExecutionPointer(current))
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

func (r *graphRunner[T, E]) tryApplyTerminalPolicies(
	ctx context.Context,
	threadID string,
	status RunStatus,
) {
	if err := r.applyTerminalPolicies(ctx, threadID, status); err != nil {
		if errors.Is(err, ErrThreadLeaseBusy) {
			r.logger.DebugContext(ctx, "flowy: terminal policy skipped, thread busy",
				"thread_id", threadID, "status", status)
			return
		}
		r.logger.WarnContext(ctx, "flowy: terminal policy failed",
			"thread_id", threadID, "status", status, "err", err)
	}
}

func (r *graphRunner[T, E]) applyTerminalPolicies(
	ctx context.Context,
	threadID string,
	status RunStatus,
) error {
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
// (WithHandoffOutbox, WithCheckpointErrorPolicy).
// Suspend/Handoff inside AsNode are not resumable: inner ResumeToken is not propagated.
// For suspend/handoff continuity use SubgraphNodeWithSlot.
func (g *Graph[T, E]) AsNode() Node[T, E] {
	return func(ctx context.Context, state T) (T, Directive, error) {
		runner := g.NewRunner(newCaptureCheckpointer[T, E]())
		result, err := runner.Start(ctx, "__inline__", state)
		if err != nil {
			return state, Fail("inline graph"), err
		}
		switch result.Status {
		case RunStatusSuspended:
			return result.State, Suspend(result.Reason), nil
		case RunStatusHandoff:
			return result.State, Handoff(result.Reason), nil
		case RunStatusContextCanceled:
			// Inline cancel maps to Completed directive; error return propagates context.Canceled.
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
			return result.State, Fail("inline graph failed"), nil
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

// bumpRevisionOnLoadCP simulates inner OCC mismatch after slot seed for tests.
type bumpRevisionOnLoadCP[T, E any] struct {
	captureCheckpointer[T, E]
}

func (b *bumpRevisionOnLoadCP[T, E]) Load(
	ctx context.Context,
	threadID string,
) (Snapshot[T, E], uint64, error) {
	snap, rev, err := b.captureCheckpointer.Load(ctx, threadID)
	if err != nil {
		return snap, 0, err
	}
	snap.Revision = rev + 1
	return snap, snap.Revision, nil
}

func (f *failingCaptureCheckpointer[T, E]) Save(
	ctx context.Context,
	expectedRevision uint64,
	s Snapshot[T, E],
) (uint64, error) {
	if f.failSave {
		return 0, errors.New("subgraph seed save failed")
	}
	return f.captureCheckpointer.Save(ctx, expectedRevision, s)
}

func (f *failingCaptureCheckpointer[T, E]) Load(
	ctx context.Context,
	threadID string,
) (Snapshot[T, E], uint64, error) {
	if f.failLoad {
		var zero Snapshot[T, E]
		return zero, 0, errors.New("subgraph slot load failed")
	}
	return f.captureCheckpointer.Load(ctx, threadID)
}

func (n *captureCheckpointer[T, E]) Save(
	_ context.Context,
	expectedRevision uint64,
	s Snapshot[T, E],
) (uint64, error) {
	var current uint64
	if len(n.history) > 0 {
		current = n.history[len(n.history)-1].Revision
	}
	if current != expectedRevision {
		return 0, ErrConcurrencyConflict
	}
	newRev := expectedRevision + 1
	s.Revision = newRev
	n.history = append(n.history, s)
	return newRev, nil
}

func (n *captureCheckpointer[T, E]) Load(_ context.Context, _ string) (Snapshot[T, E], uint64, error) {
	if len(n.history) == 0 {
		var zero Snapshot[T, E]
		return zero, 0, ErrThreadNotFound
	}
	s := n.history[len(n.history)-1]
	return s, s.Revision, nil
}

func (n *captureCheckpointer[T, E]) GetHistory(
	_ context.Context,
	_ string,
	limit int,
) ([]Snapshot[T, E], error) {
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
