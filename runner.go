package flowy

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"sync"
)

// Graph is the compiled, immutable graph. Created only via GraphBuilder.Compile.
type Graph[T any] struct {
	nodes            map[string]nodeDef[T]
	edges            map[string]string
	conditionalEdges map[string]ConditionalEdge[T]
	fanOuts          map[string]*fanOutDef
	dynamicFanOuts   map[string]*dynamicFanOutDef[T]
	entryPoint       string
	finishPoints     map[string]bool
	reducer          Reducer[T]
	defaults         runConfig
	// Reuses *ExecutionChain[T] for middleware paths to avoid one heap alloc per executeNode (see borrowExecutionChain).
	executionChainPool sync.Pool
}

type suspendExecutionError[T any] struct {
	state        T
	resumeTarget string
}

func (e *suspendExecutionError[T]) Error() string {
	return ErrSuspend.Error()
}

func (e *suspendExecutionError[T]) Unwrap() error {
	return ErrSuspend
}

// Stream runs the graph starting at startNode (or the compiled entry point if startNode is empty)
// and yields (Step, nil) after each successful step. On error, it yields one final (Step{}, err) and stops.
// On ErrSuspend, it yields one final Step with state, NodeName set to the suspending node, NextNode set to
// the resume cursor, then stops. The caller persists state and NextNode outside the core.
//
// Error contract (non-suspend):
//   - [ErrMaxStepsExceeded]: after the step budget is exhausted, yields a zero [Step] and ErrMaxStepsExceeded.
//   - Context cancellation or deadline: if ctx is done before a step, yields a zero Step and
//     an error wrapping the context error.
//   - Other errors from routing or nodes: typically yields a zero Step and the error; see [Step] for
//     when fields may be partially filled.
//
// See [Step] for field guarantees on success vs ErrSuspend vs other errors.
func (g *Graph[T]) Stream(ctx context.Context, startNode string, state T) iter.Seq2[Step[T], error] {
	return func(yield func(Step[T], error) bool) {
		sn := startNode
		if sn == "" {
			sn = g.entryPoint
		}
		g.runStream(ctx, state, sn, yield)
	}
}

// Invoke runs the graph to completion. On success it returns the final merged state.
//
// On ErrSuspend, it returns the snapshot state from the last step (full state for resume) and ErrSuspend.
//
// On other errors, behavior follows the last yield from [Stream]:
//   - [ErrMaxStepsExceeded] or context cancellation: the iterator yields a zero [Step]; Invoke returns
//     the last merged state from the previous successful step (or the initial state if none) and the error.
//   - If the iterator yields a non-zero [Step].NodeName together with the error, Invoke returns
//     step.State and the error (state associated with that failing step).
//   - Otherwise it returns the last state from a successful prior step and the error.
func (g *Graph[T]) Invoke(ctx context.Context, state T) (T, error) {
	var lastState T
	lastState = state
	for step, err := range g.Stream(ctx, "", state) {
		if err != nil {
			if step.NodeName != "" {
				return step.State, err
			}
			return lastState, err
		}
		lastState = step.State
	}
	return lastState, nil
}

// runStream executes from startNode, calling yield after each successful node. On error or ErrSuspend, yields one final event and returns.
func (g *Graph[T]) runStream(
	ctx context.Context,
	state T,
	startNode string,
	yield func(Step[T], error) bool,
) {
	current := startNode
	cfg := &g.defaults
	for range cfg.maxSteps {
		if err := ctx.Err(); err != nil {
			wrapped := fmt.Errorf("flowy: %w", err)
			yield(Step[T]{}, wrapped)
			return
		}

		result, err := g.executeStep(ctx, state, current, cfg)
		if err != nil {
			if suspendState, resumeTarget, ok := unwrapSuspend(err, state, current); ok {
				yield(Step[T]{State: suspendState, NodeName: current, NextNode: resumeTarget}, ErrSuspend)
				return
			}
			yield(Step[T]{}, err)
			return
		}

		if !g.emitStep(result, yield) {
			return
		}
		if result.terminal {
			return
		}
		state = result.state
		current = result.next
	}
	yield(Step[T]{}, ErrMaxStepsExceeded)
}

type executedStep[T any] struct {
	state    T
	node     string
	next     string
	terminal bool
}

func (g *Graph[T]) executeStep(ctx context.Context, state T, current string, cfg *runConfig) (executedStep[T], error) {
	if fo, isFanOut := g.fanOuts[current]; isFanOut {
		nextState, err := g.runFanOut(ctx, current, state, fo, cfg)
		if err != nil {
			return executedStep[T]{}, err
		}
		return executedStep[T]{state: nextState, node: current, next: fo.joinNode}, nil
	}
	if dfo, isDynamicFanOut := g.dynamicFanOuts[current]; isDynamicFanOut {
		targets, err := dfo.router(ctx, state)
		if err != nil {
			return executedStep[T]{}, fmt.Errorf("flowy: dynamic fan-out router %q failed: %w", current, err)
		}
		nextState := state
		if len(targets) > 0 {
			tempFo := &fanOutDef{targets: targets, joinNode: dfo.joinNode}
			nextState, err = g.runFanOut(ctx, current, state, tempFo, cfg)
			if err != nil {
				return executedStep[T]{}, err
			}
		}
		return executedStep[T]{state: nextState, node: current, next: dfo.joinNode}, nil
	}

	if _, ok := g.nodes[current]; !ok {
		return executedStep[T]{}, fmt.Errorf("flowy: node %q not found", current)
	}

	delta, err := g.executeNode(ctx, state, current, current, cfg)
	if err != nil {
		return executedStep[T]{}, err
	}

	nextState := g.reducer(state, delta)
	if g.finishPoints[current] {
		return executedStep[T]{state: nextState, node: current, terminal: true}, nil
	}

	next, err := g.resolveNext(ctx, current, nextState)
	if err != nil {
		return executedStep[T]{}, err
	}
	return executedStep[T]{state: nextState, node: current, next: next}, nil
}

func (g *Graph[T]) emitStep(result executedStep[T], yield func(Step[T], error) bool) bool {
	next := result.next
	if result.terminal {
		next = ""
	}
	return yield(Step[T]{State: result.state, NodeName: result.node, NextNode: next}, nil)
}

// mapNodeExecutionError turns raw handler errors into executeNode results (suspend envelope, fan-out rules, wrapped errors).
func mapNodeExecutionError[T any](
	out T,
	err error,
	nodeName, suspendTarget string,
	executionKind MiddlewareExecutionKind,
) (T, error) {
	if err == nil {
		return out, nil
	}
	if errors.Is(err, ErrSuspend) {
		if executionKind == MiddlewareExecutionFanOutBranch {
			return out, fmt.Errorf(
				"flowy: ErrSuspend is not supported inside fan-out target %q; suspend before the fan-out source or after the join node",
				nodeName,
			)
		}
		return out, &suspendExecutionError[T]{state: out, resumeTarget: suspendTarget}
	}
	return out, fmt.Errorf("flowy: node %q: %w", nodeName, err)
}

func (g *Graph[T]) borrowExecutionChain() *ExecutionChain[T] {
	v := g.executionChainPool.Get()
	if v == nil {
		return new(ExecutionChain[T])
	}
	c, ok := v.(*ExecutionChain[T])
	if !ok {
		return new(ExecutionChain[T])
	}
	c.released = false
	return c
}

func (g *Graph[T]) releaseExecutionChain(c *ExecutionChain[T]) {
	c.released = true
	c.index = 0
	c.middlewares = nil
	c.handler = nil
	c.g = nil
	c.cfg = nil
	g.executionChainPool.Put(c)
}

func (g *Graph[T]) executeNode(
	ctx context.Context,
	state T,
	nodeName, suspendTarget string,
	cfg *runConfig,
) (T, error) {
	node, ok := g.nodes[nodeName]
	if !ok {
		var zero T
		return zero, fmt.Errorf("flowy: node %q not found", nodeName)
	}

	executionKind := MiddlewareExecutionNode
	canResolveNext := true
	if suspendTarget != nodeName {
		executionKind = MiddlewareExecutionFanOutBranch
		canResolveNext = false
	}

	// Fast path: no middleware — skip ExecutionChain allocation path used by Next.
	if len(node.compiledMiddlewares) == 0 {
		nodeCtx, cancel := nodeContextWithTimeout(ctx, cfg)
		defer cancel()

		out, err := node.handler(nodeCtx, state)
		return mapNodeExecutionError(out, err, nodeName, suspendTarget, executionKind)
	}

	chain := g.borrowExecutionChain()
	defer g.releaseExecutionChain(chain)
	chain.NodeName = nodeName
	chain.SuspendTarget = suspendTarget
	chain.ExecutionKind = executionKind
	chain.CanResolveNext = canResolveNext
	chain.IsFinish = g.finishPoints[nodeName]
	chain.g = g
	chain.cfg = cfg
	chain.middlewares = node.compiledMiddlewares
	chain.handler = node.handler
	out, err := chain.Next(ctx, state)
	return mapNodeExecutionError(out, err, nodeName, suspendTarget, executionKind)
}

//nolint:gocognit // Fan-out coordinates branches, semaphore, cancellation, and merge; splitting would obscure the protocol.
func (g *Graph[T]) runFanOut(ctx context.Context, from string, state T, fo *fanOutDef, cfg *runConfig) (T, error) {
	for _, targetName := range fo.targets {
		if _, ok := g.nodes[targetName]; !ok {
			return state, fmt.Errorf("flowy: fan-out target node %q not found", targetName)
		}
	}
	results := make([]T, len(fo.targets))

	runCtx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)

	var (
		wg       sync.WaitGroup
		firstErr error
		errMu    sync.Mutex
		sem      chan struct{}
	)
	if cfg.maxConcurrency > 0 {
		sem = make(chan struct{}, cfg.maxConcurrency)
	}

	recordErr := func(err error) {
		errMu.Lock()
		defer errMu.Unlock()
		if firstErr != nil {
			return
		}
		firstErr = err
		cancel(err)
	}

	for i, targetName := range fo.targets {
		wg.Go(func() {
			if sem != nil {
				select {
				case sem <- struct{}{}:
					defer func() { <-sem }()
				case <-runCtx.Done():
					return
				}
			}

			delta, err := g.executeNode(runCtx, state, targetName, from, cfg)
			if err != nil {
				cause := context.Cause(runCtx)
				if errors.Is(err, context.Canceled) && cause != nil && !errors.Is(cause, context.Canceled) {
					return
				}
				recordErr(err)
				return
			}
			results[i] = delta
		})
	}

	wg.Wait()
	if firstErr != nil {
		return state, fmt.Errorf("flowy: fan-out from %q: %w", from, firstErr)
	}
	for _, delta := range results {
		state = g.reducer(state, delta)
	}
	return state, nil
}

//nolint:nestif // Conditional routing must validate node vs fan-out vs dynamic fan-out targets in one place.
func (g *Graph[T]) resolveNext(ctx context.Context, current string, state T) (string, error) {
	if to, ok := g.edges[current]; ok {
		return to, nil
	}
	if router, ok := g.conditionalEdges[current]; ok {
		next, err := router(ctx, state)
		if err != nil {
			return "", fmt.Errorf("flowy: conditional edge from %q: %w", current, err)
		}
		if next == "" {
			return "", fmt.Errorf("flowy: conditional edge from %q returned empty node name", current)
		}
		if _, hasNode := g.nodes[next]; !hasNode {
			if _, hasFan := g.fanOuts[next]; !hasFan {
				if _, hasDyn := g.dynamicFanOuts[next]; !hasDyn {
					return "", fmt.Errorf("flowy: conditional edge from %q returned unknown node %q", current, next)
				}
			}
		}
		return next, nil
	}
	return "", fmt.Errorf("flowy: no outgoing edge from node %q", current)
}

func unwrapSuspend[T any](err error, fallbackState T, fallbackTarget string) (T, string, bool) {
	if !errors.Is(err, ErrSuspend) {
		var zero T
		return zero, "", false
	}
	var suspendErr *suspendExecutionError[T]
	if errors.As(err, &suspendErr) {
		return suspendErr.state, suspendErr.resumeTarget, true
	}
	return fallbackState, fallbackTarget, true
}

// AsNode returns a node that runs this graph (for composition when state types match).
func (g *Graph[T]) AsNode() Node[T] {
	return func(ctx context.Context, state T) (T, error) {
		final, err := g.Invoke(ctx, state)
		return final, err
	}
}
