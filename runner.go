package flowy

import (
	"context"
	"errors"
	"fmt"
	"iter"
)

// Graph is the compiled, immutable graph. Created only via GraphBuilder.Compile.
type Graph[T any] struct {
	nodes        map[string]nodeDef[T]
	edges        map[string]string
	choices      map[string]Choice[T]
	entryPoint   string
	finishPoints map[string]bool
	reducer      Reducer[T]
	defaults     runConfig
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

// mapNodeExecutionError turns raw handler errors into executeNode results (suspend envelope, wrapped errors).
func mapNodeExecutionError[T any](
	out T,
	err error,
	nodeName, suspendTarget string,
) (T, error) {
	if err == nil {
		return out, nil
	}
	if errors.Is(err, ErrSuspend) {
		return out, &suspendExecutionError[T]{state: out, resumeTarget: suspendTarget}
	}
	return out, fmt.Errorf("flowy: node %q: %w", nodeName, err)
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

	// Fast path: no middleware — skip ExecutionChain allocation path used by Next.
	if len(node.compiledMiddlewares) == 0 {
		nodeCtx, cancel := nodeContextWithTimeout(ctx, cfg)
		defer cancel()

		out, err := node.handler(nodeCtx, state)
		return mapNodeExecutionError(out, err, nodeName, suspendTarget)
	}

	chain := new(ExecutionChain[T])
	chain.NodeName = nodeName
	chain.SuspendTarget = suspendTarget
	chain.IsFinish = g.finishPoints[nodeName]
	chain.g = g
	chain.cfg = cfg
	chain.middlewares = node.compiledMiddlewares
	chain.handler = node.handler
	out, err := chain.Next(ctx, state)
	return mapNodeExecutionError(out, err, nodeName, suspendTarget)
}

func (g *Graph[T]) resolveNext(ctx context.Context, current string, state T) (string, error) {
	if to, ok := g.edges[current]; ok {
		return to, nil
	}
	if router, ok := g.choices[current]; ok {
		next, err := router(ctx, state)
		if err != nil {
			return "", fmt.Errorf("flowy: choice from %q: %w", current, err)
		}
		if next == "" {
			return "", fmt.Errorf("flowy: choice from %q returned empty node name", current)
		}
		if _, hasNode := g.nodes[next]; !hasNode {
			return "", fmt.Errorf("flowy: choice from %q returned unknown node %q", current, next)
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
