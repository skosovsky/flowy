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
	nodes             map[string]nodeDef[T]
	edges             map[string]string
	conditionalEdges  map[string]ConditionalEdge[T]
	fanOuts           map[string]*fanOutDef
	dynamicFanOuts    map[string]*dynamicFanOutDef[T]
	globalMiddlewares []Middleware[T]
	entryPoint        string
	finishPoints      map[string]bool
	reducer           Reducer[T]
	defaults          runConfig
}

type suspendExecution[T any] struct {
	state        T
	resumeTarget string
}

func (e *suspendExecution[T]) Error() string {
	return ErrSuspend.Error()
}

func (e *suspendExecution[T]) Unwrap() error {
	return ErrSuspend
}

// Stream runs the graph from the entry point and yields (Step, nil) after each successful step.
// On error, it yields one final (Step{}, err) and stops. On ErrSuspend, it yields
// one final Step with the saved state and resume target, then stops.
func (g *Graph[T]) Stream(ctx context.Context, state T) iter.Seq2[Step[T], error] {
	return func(yield func(Step[T], error) bool) {
		g.runStream(ctx, state, g.entryPoint, yield)
	}
}

// Invoke runs the graph to completion. It returns the final state on success,
// or the last consistent state together with an error.
func (g *Graph[T]) Invoke(ctx context.Context, state T) (T, error) {
	var lastState T
	lastState = state
	for step, err := range g.Stream(ctx, state) {
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

// ResumeStream continues execution from startNode with the given state. Same yielding contract as Stream.
func (g *Graph[T]) ResumeStream(ctx context.Context, state T, startNode string) iter.Seq2[Step[T], error] {
	if startNode == "" {
		return func(yield func(Step[T], error) bool) {
			yield(Step[T]{}, fmt.Errorf("flowy: start node required for Resume"))
		}
	}
	if !g.hasNodeOrRouting(startNode) {
		return func(yield func(Step[T], error) bool) {
			yield(Step[T]{}, fmt.Errorf("flowy: resume node %q not found", startNode))
		}
	}
	return func(yield func(Step[T], error) bool) {
		g.runStream(ctx, state, startNode, yield)
	}
}

// Resume continues execution from startNode with the given state.
func (g *Graph[T]) Resume(ctx context.Context, state T, startNode string) (T, error) {
	if startNode == "" {
		return state, fmt.Errorf("flowy: start node required for Resume")
	}
	if !g.hasNodeOrRouting(startNode) {
		return state, fmt.Errorf("flowy: resume node %q not found", startNode)
	}
	var lastState T
	lastState = state
	for step, err := range g.ResumeStream(ctx, state, startNode) {
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
func (g *Graph[T]) runStream(ctx context.Context, state T, startNode string, yield func(Step[T], error) bool) {
	current := startNode
	cfg := &g.defaults
	for step := 0; step < cfg.maxSteps; step++ {
		if err := ctx.Err(); err != nil {
			wrapped := fmt.Errorf("flowy: %w", err)
			yield(Step[T]{}, wrapped)
			return
		}

		if fo, isFanOut := g.fanOuts[current]; isFanOut {
			nextState, err := g.runFanOut(ctx, current, state, fo, cfg)
			if err != nil {
				yield(Step[T]{}, err)
				return
			}
			state = nextState
			if !yield(Step[T]{State: state, NodeName: current}, nil) {
				return
			}
			current = fo.joinNode
			continue
		}
		if dfo, isDynFanOut := g.dynamicFanOuts[current]; isDynFanOut {
			targets, err := dfo.router(ctx, state)
			if err != nil {
				wrapped := fmt.Errorf("flowy: dynamic fan-out router %q failed: %w", current, err)
				yield(Step[T]{}, wrapped)
				return
			}
			if len(targets) > 0 {
				tempFo := &fanOutDef{targets: targets, joinNode: dfo.joinNode}
				nextState, err := g.runFanOut(ctx, current, state, tempFo, cfg)
				if err != nil {
					yield(Step[T]{}, err)
					return
				}
				state = nextState
			}
			if !yield(Step[T]{State: state, NodeName: current}, nil) {
				return
			}
			current = dfo.joinNode
			continue
		}

		if _, ok := g.nodes[current]; !ok {
			yield(Step[T]{}, fmt.Errorf("flowy: node %q not found", current))
			return
		}
		delta, err := g.executeNode(ctx, state, current, current, cfg)
		if err != nil {
			if suspendState, resumeTarget, ok := unwrapSuspend(err, state, current); ok {
				yield(Step[T]{State: suspendState, NodeName: resumeTarget}, ErrSuspend)
				return
			}
			yield(Step[T]{}, err)
			return
		}
		state = g.reducer(state, delta)
		if !yield(Step[T]{State: state, NodeName: current}, nil) {
			return
		}
		if g.finishPoints[current] {
			return
		}
		next, err := g.resolveNext(ctx, current, state)
		if err != nil {
			yield(Step[T]{}, err)
			return
		}
		current = next
	}
	yield(Step[T]{}, ErrMaxStepsExceeded)
}

func (g *Graph[T]) executeNode(ctx context.Context, state T, nodeName, suspendTarget string, cfg *runConfig) (T, error) {
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

	meta := MiddlewareContext[T]{
		NodeName:       nodeName,
		SuspendTarget:  suspendTarget,
		ExecutionKind:  executionKind,
		CanResolveNext: canResolveNext,
		IsFinish:       g.finishPoints[nodeName],
		ApplyUpdate: func(current T, update T) T {
			return g.reducer(current, update)
		},
		ResolveNext: func(resolveCtx context.Context, postState T) (string, error) {
			if g.finishPoints[nodeName] {
				return "", nil
			}
			if !canResolveNext {
				return "", fmt.Errorf("flowy: next node resolution is unavailable for fan-out target %q", nodeName)
			}
			return g.resolveNext(resolveCtx, nodeName, postState)
		},
	}

	handler := NodeHandler[T](func(callCtx context.Context, current T) (T, error) {
		nodeCtx, cancel := nodeContextWithTimeout(callCtx, cfg)
		defer cancel()

		out, err := node.handler(nodeCtx, current)
		if errors.Is(err, ErrSuspend) {
			return current, ErrSuspend
		}
		return out, err
	})

	allMws := make([]Middleware[T], 0, len(g.globalMiddlewares)+len(node.middlewares))
	allMws = append(allMws, g.globalMiddlewares...)
	allMws = append(allMws, node.middlewares...)
	for i := len(allMws) - 1; i >= 0; i-- {
		mw := allMws[i]
		next := handler
		handler = func(callCtx context.Context, current T) (T, error) {
			return mw(callCtx, current, meta, next)
		}
	}

	out, err := handler(ctx, state)
	if err != nil {
		if errors.Is(err, ErrSuspend) {
			if executionKind == MiddlewareExecutionFanOutBranch {
				return out, fmt.Errorf("flowy: ErrSuspend is not supported inside fan-out target %q; suspend before the fan-out source or after the join node", nodeName)
			}
			return out, &suspendExecution[T]{state: out, resumeTarget: suspendTarget}
		}
		return out, fmt.Errorf("flowy: node %q: %w", nodeName, err)
	}
	return out, nil
}

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
		i := i
		targetName := targetName

		wg.Add(1)
		go func() {
			defer wg.Done()

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
		}()
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

// hasNodeOrRouting reports whether name is a registered node or a fan-out/dynamic fan-out source.
func (g *Graph[T]) hasNodeOrRouting(name string) bool {
	if _, ok := g.nodes[name]; ok {
		return true
	}
	if g.fanOuts[name] != nil {
		return true
	}
	if g.dynamicFanOuts[name] != nil {
		return true
	}
	return false
}

func unwrapSuspend[T any](err error, fallbackState T, fallbackTarget string) (T, string, bool) {
	if !errors.Is(err, ErrSuspend) {
		var zero T
		return zero, "", false
	}
	var suspendErr *suspendExecution[T]
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
