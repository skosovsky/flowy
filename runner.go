package flowy

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"sync"
	"time"
)

// Graph is the compiled, immutable graph.
type Graph[T any] struct {
	nodes            map[string]nodeDef[T]
	edges            map[string]string
	conditionalEdges map[string]EdgeRouter[T]
	entryPoint       string
	reducer          Reducer[T]
	defaults         runConfig
}

type graphRunner[T any] struct {
	graph        *Graph[T]
	checkpointer Checkpointer[T]
	interceptors []StateInterceptor[T]
}

type eventSink[T any] func(ctx context.Context, event RunEvent[T]) bool

type streamHandle[T any] struct {
	events chan RunEvent[T]
	stop   chan struct{}
	done   chan struct{}
	once   sync.Once
	err    error
}

func (s *streamHandle[T]) Events() <-chan RunEvent[T] {
	return s.events
}

func (s *streamHandle[T]) Close() {
	s.once.Do(func() {
		close(s.stop)
	})
}

func (s *streamHandle[T]) Done() error {
	<-s.done
	return s.err
}

func (s *streamHandle[T]) stopped() bool {
	select {
	case <-s.stop:
		return true
	default:
		return false
	}
}

// NewRunner binds a compiled graph to persistence and returns a lifecycle runner.
func (g *Graph[T]) NewRunner(checkpointer Checkpointer[T], interceptors ...StateInterceptor[T]) Runner[T] {
	return &graphRunner[T]{
		graph:        g,
		checkpointer: checkpointer,
		interceptors: append([]StateInterceptor[T](nil), interceptors...),
	}
}

func (r *graphRunner[T]) Start(ctx context.Context, threadID string, initialState T) (*RunResult[T], error) {
	meta := RunMetadata{
		SegmentStartTime: time.Now().UTC(),
		RetryCounts:      map[string]int{},
		StepCount:        0,
		TelemetryContext: nil,
	}
	return r.execute(ctx, threadID, r.graph.entryPoint, initialState, meta, nil, 0, nil)
}

func (r *graphRunner[T]) Resume(ctx context.Context, threadID string, opts ...ResumeOption[T]) (*RunResult[T], error) {
	if r.checkpointer == nil {
		return nil, errors.New("flowy: checkpointer is required for Resume")
	}
	startNode, state, meta, effects, revision, err := r.prepareResume(ctx, threadID, opts...)
	if err != nil {
		return nil, err
	}
	runCtx := injectTelemetryContext(ctx, meta.TelemetryContext)
	return r.execute(runCtx, threadID, startNode, state, meta, effects, revision, nil)
}

func (r *graphRunner[T]) Stream(ctx context.Context, threadID string, initialState T) (StreamHandle[T], error) {
	meta := RunMetadata{
		SegmentStartTime: time.Now().UTC(),
		RetryCounts:      map[string]int{},
		StepCount:        0,
		TelemetryContext: nil,
	}
	return r.startStream(ctx, func(sink eventSink[T]) error {
		_, err := r.execute(ctx, threadID, r.graph.entryPoint, initialState, meta, nil, 0, sink)
		return err
	}), nil
}

func (r *graphRunner[T]) StreamResume(
	ctx context.Context,
	threadID string,
	opts ...ResumeOption[T],
) (StreamHandle[T], error) {
	if r.checkpointer == nil {
		return nil, errors.New("flowy: checkpointer is required for StreamResume")
	}
	startNode, state, meta, effects, revision, err := r.prepareResume(ctx, threadID, opts...)
	if err != nil {
		return nil, err
	}
	runCtx := injectTelemetryContext(ctx, meta.TelemetryContext)
	return r.startStream(ctx, func(sink eventSink[T]) error {
		_, err := r.execute(runCtx, threadID, startNode, state, meta, effects, revision, sink)
		return err
	}), nil
}

func (r *graphRunner[T]) startStream(_ context.Context, runFn func(sink eventSink[T]) error) StreamHandle[T] {
	stream := &streamHandle[T]{
		events: make(chan RunEvent[T], streamEventBufferSize),
		stop:   make(chan struct{}),
		done:   make(chan struct{}),
		once:   sync.Once{},
		err:    nil,
	}
	go func() {
		defer close(stream.events)
		defer close(stream.done)
		sink := func(eventCtx context.Context, event RunEvent[T]) bool {
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

func (r *graphRunner[T]) prepareResume(
	ctx context.Context,
	threadID string,
	opts ...ResumeOption[T],
) (string, T, RunMetadata, []any, int, error) {
	snapshot, err := r.checkpointer.Load(ctx, threadID)
	if err != nil {
		var zero T
		return "", zero, RunMetadata{}, nil, 0, fmt.Errorf("%w: %w", ErrThreadNotFound, err)
	}
	state := snapshot.State
	for _, interceptor := range r.interceptors {
		if err := interceptor.AfterLoad(ctx, &state); err != nil {
			var zero T
			return "", zero, RunMetadata{}, nil, 0, fmt.Errorf("flowy: after_load interceptor: %w", err)
		}
	}
	var ropts resumeOptions[T]
	for _, opt := range opts {
		if opt != nil {
			opt.apply(&ropts)
		}
	}
	for _, patch := range ropts.patches {
		patch(&state)
	}

	meta := snapshot.RunMeta
	if meta.RetryCounts == nil {
		meta.RetryCounts = map[string]int{}
	}
	meta.SegmentStartTime = time.Now().UTC()
	return snapshot.NodeID, state, meta, append([]any(nil), snapshot.Effects...), snapshot.Revision, nil
}

func (r *graphRunner[T]) execute(
	ctx context.Context,
	threadID string,
	startNode string,
	state T,
	meta RunMetadata,
	effects []any,
	revision int,
	sink eventSink[T],
) (*RunResult[T], error) {
	current := startNode
	runCtx := ctx
	limit := r.graph.defaults.maxSteps
	if limit <= 0 {
		limit = defaultMaxSteps
	}

	for {
		if ctxErr := runCtx.Err(); ctxErr != nil {
			return r.handleContextCancellation(
				runCtx, threadID, current, state, meta, effects, revision, sink, ctxErr,
			)
		}

		nodeCtx := withNodeName(runCtx, current)
		step, stepErr := r.runNodeStep(nodeCtx, runCtx, current, state, meta, effects, sink)
		if stepErr != nil {
			return failedResult(step.state, step.effects, step.meta, current), stepErr
		}
		if step.emitCanceled {
			return failedResult(step.state, step.effects, step.meta, current), context.Canceled
		}

		state = step.state
		meta = step.meta
		effects = step.effects
		base := step.base

		if meta.StepCount > limit {
			emitTerminalEvent(runCtx, sink, newRunEventFailed(current, state, ErrMaxStepsExceeded))
			return failedResult(state, effects, meta, current), ErrMaxStepsExceeded
		}

		stepOut := r.applyDirective(
			runCtx, nodeCtx, threadID, current, state, meta, effects, revision, base, sink,
		)
		if stepOut.terminal {
			return stepOut.result, stepOut.err
		}
		current = stepOut.nextNode
	}
}

func (r *graphRunner[T]) handleContextCancellation(
	runCtx context.Context,
	threadID, current string,
	state T,
	meta RunMetadata,
	effects []any,
	revision int,
	sink eventSink[T],
	ctxErr error,
) (*RunResult[T], error) {
	revision++
	meta.TelemetryContext = extractTelemetryContext(runCtx)
	result := newRunResultContextCanceled(state, effects, meta, current)

	saveCtx, cancelSave := context.WithTimeout(context.WithoutCancel(runCtx), contextCancelSaveTimeout)
	saveErr := r.saveSnapshot(saveCtx, Snapshot[T]{
		ThreadID: threadID,
		NodeID:   current,
		Revision: revision,
		State:    state,
		RunMeta:  meta,
		Effects:  append([]any(nil), effects...),
	})
	cancelSave()
	if saveErr != nil {
		emitTerminalEvent(runCtx, sink, newRunEventFailed(current, state, saveErr))
		return result, fmt.Errorf("flowy: context canceled and save failed: %w", saveErr)
	}

	emitTerminalEvent(runCtx, sink, newRunEventSuspended(current, state, ctxErr))
	return result, fmt.Errorf("flowy: %w", ctxErr)
}

type nodeStepOutcome[T any] struct {
	state        T
	meta         RunMetadata
	effects      []any
	base         Directive
	emitCanceled bool
}

type directiveStep[T any] struct {
	nextNode string
	result   *RunResult[T]
	err      error
	terminal bool
}

func (r *graphRunner[T]) runNodeStep(
	nodeCtx, runCtx context.Context,
	current string,
	state T,
	meta RunMetadata,
	effects []any,
	sink eventSink[T],
) (nodeStepOutcome[T], error) {
	node, ok := r.graph.nodes[current]
	if !ok {
		err := fmt.Errorf("flowy: node %q not found", current)
		emitTerminalEvent(runCtx, sink, newRunEventFailed(current, state, err))
		return blankNodeStepOutcome(state, meta, effects), err
	}

	if !emitEvent(nodeCtx, sink, newRunEventNodeStarted(current, state)) {
		return canceledNodeStepOutcome(state, meta, effects), nil
	}

	nodeStart := time.Now()
	update, directive, err := node.handler(nodeCtx, state)
	nodeDuration := time.Since(nodeStart)
	if err != nil {
		emitTerminalEvent(runCtx, sink, newRunEventFailed(current, state, err))
		return blankNodeStepOutcome(state, meta, effects), fmt.Errorf("flowy: node %q: %w", current, err)
	}

	state = r.graph.reducer(state, update)
	meta.StepCount++
	base, nodeEffects, err := UnwrapDirective(directive)
	if err != nil {
		emitTerminalEvent(runCtx, sink, newRunEventFailed(current, state, err))
		return blankNodeStepOutcome(state, meta, effects), err
	}

	effects = append(effects, nodeEffects...)
	if len(nodeEffects) == 0 {
		if !emitEvent(nodeCtx, sink, newRunEventNodeCompleted(current, state, nodeDuration)) {
			return canceledNodeStepOutcome(state, meta, effects), nil
		}
	} else {
		for _, effect := range nodeEffects {
			if !emitEvent(nodeCtx, sink, newRunEventNodeCompletedWithEffect(
				current, state, effect, nodeDuration, metricsFromEffect(effect),
			)) {
				return canceledNodeStepOutcome(state, meta, effects), nil
			}
		}
	}

	out := blankNodeStepOutcome(state, meta, effects)
	out.base = base
	return out, nil
}

func (r *graphRunner[T]) applyDirective(
	runCtx, nodeCtx context.Context,
	threadID, current string,
	state T,
	meta RunMetadata,
	effects []any,
	revision int,
	base Directive,
	sink eventSink[T],
) directiveStep[T] {
	switch base.kind {
	case directiveCompleted:
		return r.applyDirectiveCompleted(runCtx, nodeCtx, current, state, meta, effects, sink)
	case directiveNext:
		return r.applyDirectiveNext(runCtx, current, state, meta, effects, base, sink)
	case directiveEnd:
		return r.finishCompleted(runCtx, current, state, meta, effects, sink)
	case directiveSuspend:
		return r.applyDirectiveSuspend(runCtx, threadID, current, state, meta, effects, revision, base, sink)
	case directiveRetry:
		return r.applyDirectiveRetry(runCtx, current, state, meta, effects, base, sink)
	default:
		unsupported := errors.New("flowy: node returned unsupported directive")
		emitTerminalEvent(runCtx, sink, newRunEventFailed(current, state, unsupported))
		return terminalDirectiveStep(
			failedResult(state, effects, meta, current),
			unsupported,
		)
	}
}

func (r *graphRunner[T]) applyDirectiveCompleted(
	runCtx, nodeCtx context.Context,
	current string,
	state T,
	meta RunMetadata,
	effects []any,
	sink eventSink[T],
) directiveStep[T] {
	nextNode, err := r.resolveEdge(nodeCtx, current, state)
	if err != nil {
		emitTerminalEvent(runCtx, sink, newRunEventFailed(current, state, err))
		return terminalDirectiveStep(failedResult(state, effects, meta, current), err)
	}
	if nextNode == EndNode {
		return r.finishCompleted(runCtx, current, state, meta, effects, sink)
	}
	return continueDirectiveStep[T](nextNode)
}

func (r *graphRunner[T]) applyDirectiveNext(
	runCtx context.Context,
	current string,
	state T,
	meta RunMetadata,
	effects []any,
	base Directive,
	sink eventSink[T],
) directiveStep[T] {
	if base.nextNodeID == "" {
		err := errors.New("flowy: Next directive requires non-empty node id")
		emitTerminalEvent(runCtx, sink, newRunEventFailed(current, state, err))
		return terminalDirectiveStep(failedResult(state, effects, meta, current), err)
	}
	if base.nextNodeID == EndNode {
		return r.finishCompleted(runCtx, current, state, meta, effects, sink)
	}
	if _, ok := r.graph.nodes[base.nextNodeID]; !ok {
		err := fmt.Errorf("flowy: Next directive target %q not found", base.nextNodeID)
		emitTerminalEvent(runCtx, sink, newRunEventFailed(current, state, err))
		return terminalDirectiveStep(failedResult(state, effects, meta, current), err)
	}
	return continueDirectiveStep[T](base.nextNodeID)
}

func (r *graphRunner[T]) finishCompleted(
	runCtx context.Context,
	current string,
	state T,
	meta RunMetadata,
	effects []any,
	sink eventSink[T],
) directiveStep[T] {
	result := newRunResultCompleted(state, effects, meta, current)
	if !emitTerminalEvent(runCtx, sink, newRunEventCompleted(current, state)) {
		return terminalDirectiveStep(result, context.Canceled)
	}
	return terminalDirectiveStep[T](result, nil)
}

func (r *graphRunner[T]) applyDirectiveSuspend(
	runCtx context.Context,
	threadID, current string,
	state T,
	meta RunMetadata,
	effects []any,
	revision int,
	base Directive,
	sink eventSink[T],
) directiveStep[T] {
	revision++
	meta.TelemetryContext = extractTelemetryContext(runCtx)
	snapshot := Snapshot[T]{
		ThreadID: threadID,
		NodeID:   current,
		Revision: revision,
		State:    state,
		RunMeta:  meta,
		Effects:  append([]any(nil), effects...),
	}
	if err := r.saveSnapshot(runCtx, snapshot); err != nil {
		emitTerminalEvent(runCtx, sink, newRunEventFailed(current, state, err))
		return terminalDirectiveStep(
			failedResult(state, effects, meta, current),
			fmt.Errorf("flowy: suspend save failed: %w", err),
		)
	}
	if !emitTerminalEvent(runCtx, sink, newRunEventSuspendedNoError(current, state)) {
		return terminalDirectiveStep(failedResult(state, effects, meta, current), context.Canceled)
	}
	return terminalDirectiveStep(newRunResultSuspended(state, effects, meta, current, base.reason), nil)
}

func (r *graphRunner[T]) applyDirectiveRetry(
	runCtx context.Context,
	current string,
	state T,
	meta RunMetadata,
	effects []any,
	base Directive,
	sink eventSink[T],
) directiveStep[T] {
	if base.maxAttempts <= 0 {
		err := errors.New("flowy: Retry directive requires maxAttempts > 0")
		emitTerminalEvent(runCtx, sink, newRunEventFailed(current, state, err))
		return terminalDirectiveStep(failedResult(state, effects, meta, current), err)
	}
	meta.RetryCounts[current]++
	if meta.RetryCounts[current] > base.maxAttempts {
		emitTerminalEvent(runCtx, sink, newRunEventFailed(current, state, ErrMaxStepsExceeded))
		return terminalDirectiveStep(failedResult(state, effects, meta, current), ErrMaxStepsExceeded)
	}
	if base.fallbackNode != "" {
		return continueDirectiveStep[T](base.fallbackNode)
	}
	return continueDirectiveStep[T](current)
}

func metricsFromEffect(effect any) map[string]any {
	metrics, ok := effect.(map[string]any)
	if ok {
		if len(metrics) == 0 {
			return nil
		}
		out := make(map[string]any, len(metrics))
		maps.Copy(out, metrics)
		return out
	}

	payload, err := json.Marshal(effect)
	if err != nil {
		return nil
	}
	var out map[string]any
	if err := json.Unmarshal(payload, &out); err != nil {
		return nil
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func failedResult[T any](state T, effects []any, meta RunMetadata, nodeID string) *RunResult[T] {
	return &RunResult[T]{
		State:   state,
		Status:  RunStatusFailed,
		Effects: append([]any(nil), effects...),
		RunMeta: meta,
		NodeID:  nodeID,
		Reason:  "",
	}
}

func (r *graphRunner[T]) resolveEdge(ctx context.Context, current string, state T) (string, error) {
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
	if next == EndNode {
		return next, nil
	}
	if _, ok := r.graph.nodes[next]; !ok {
		return "", fmt.Errorf("flowy: conditional edge from %q returned unknown node %q", current, next)
	}
	return next, nil
}

func (r *graphRunner[T]) saveSnapshot(ctx context.Context, snapshot Snapshot[T]) error {
	if r.checkpointer == nil {
		return errors.New("flowy: checkpointer is required")
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

// AsNode composes a graph as a node.
func (g *Graph[T]) AsNode() Node[T] {
	return func(ctx context.Context, state T) (T, Directive, error) {
		runner := g.NewRunner(newCaptureCheckpointer[T]())
		result, err := runner.Start(ctx, "__inline__", state)
		if err != nil {
			return state, Completed(), err
		}
		switch result.Status {
		case RunStatusSuspended:
			return result.State, Suspend(result.Reason), nil
		case RunStatusCompleted:
			return result.State, Completed(), nil
		default:
			return result.State, Completed(), errors.New("flowy: subgraph failed")
		}
	}
}

type captureCheckpointer[T any] struct {
	history []Snapshot[T]
}

func newCaptureCheckpointer[T any]() *captureCheckpointer[T] {
	return &captureCheckpointer[T]{}
}

func (n *captureCheckpointer[T]) Save(_ context.Context, s Snapshot[T]) error {
	n.history = append(n.history, s)
	return nil
}

func (n *captureCheckpointer[T]) Load(_ context.Context, _ string) (Snapshot[T], error) {
	if len(n.history) == 0 {
		var zero Snapshot[T]
		return zero, ErrThreadNotFound
	}
	return n.history[len(n.history)-1], nil
}

func (n *captureCheckpointer[T]) GetHistory(_ context.Context, _ string, limit int) ([]Snapshot[T], error) {
	if len(n.history) == 0 {
		return []Snapshot[T]{}, nil
	}
	if limit <= 0 || limit > len(n.history) {
		limit = len(n.history)
	}
	out := make([]Snapshot[T], 0, limit)
	for i := len(n.history) - 1; i >= len(n.history)-limit; i-- {
		out = append(out, n.history[i])
	}
	return out, nil
}

func (n *captureCheckpointer[T]) Prune(_ context.Context, _ string, retainCount int) error {
	if retainCount <= 0 {
		n.history = nil
		return nil
	}
	if len(n.history) <= retainCount {
		return nil
	}
	n.history = append([]Snapshot[T](nil), n.history[len(n.history)-retainCount:]...)
	return nil
}
