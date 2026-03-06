package flowy

import (
	"context"
	"errors"
	"fmt"
	"iter"

	"golang.org/x/sync/errgroup"
)

// Graph is the compiled, immutable graph. Created only via GraphBuilder.Compile.
type Graph[T any] struct {
	nodes            map[string]Node[T]
	edges            map[string]string
	conditionalEdges map[string]ConditionalEdge[T]
	fanOuts          map[string]*fanOutDef
	dynamicFanOuts   map[string]*dynamicFanOutDef[T]
	entryPoint       string
	finishPoints     map[string]bool
	reducer          Reducer[T]
	defaults         runConfig
}

// Stream runs the graph from the entry point and yields (Step, nil) after each successful node.
// On error or ErrSuspend, it yields one final (Step{}, err) and stops. Use: for step, err := range graph.Stream(ctx, state).
func (g *Graph[T]) Stream(ctx context.Context, state T) iter.Seq2[Step[T], error] {
	return func(yield func(Step[T], error) bool) {
		g.runStream(ctx, state, g.entryPoint, yield)
	}
}

// Invoke runs the graph to completion. It returns (finalState, nil, nil) on success, or (state, &Checkpoint{NextNode: node}, ErrSuspend) when a node suspends.
func (g *Graph[T]) Invoke(ctx context.Context, state T) (T, *Checkpoint, error) {
	var lastState T
	var lastNode string
	lastState = state
	lastNode = g.entryPoint
	for step, err := range g.Stream(ctx, state) {
		if err != nil {
			nodeForCp := step.NodeName
			if nodeForCp == "" {
				nodeForCp = lastNode
			}
			stateForReturn := lastState
			if step.NodeName != "" {
				stateForReturn = step.State
			}
			return stateForReturn, &Checkpoint{NextNode: nodeForCp}, err
		}
		lastState = step.State
		lastNode = step.NodeName
	}
	return lastState, nil, nil
}

// ResumeStream continues execution from cp.NextNode with the given state. Same yielding contract as Stream.
func (g *Graph[T]) ResumeStream(ctx context.Context, state T, cp *Checkpoint) iter.Seq2[Step[T], error] {
	if cp == nil || cp.NextNode == "" {
		return func(yield func(Step[T], error) bool) {
			yield(Step[T]{}, fmt.Errorf("flowy: checkpoint required with non-empty NextNode for Resume"))
		}
	}
	if !g.hasNodeOrRouting(cp.NextNode) {
		return func(yield func(Step[T], error) bool) {
			yield(Step[T]{}, fmt.Errorf("flowy: checkpoint node %q not found", cp.NextNode))
		}
	}
	return func(yield func(Step[T], error) bool) {
		g.runStream(ctx, state, cp.NextNode, yield)
	}
}

// Resume continues execution from cp.NextNode. Pass the state and checkpoint returned by a previous Invoke that ended with ErrSuspend.
func (g *Graph[T]) Resume(ctx context.Context, state T, cp *Checkpoint) (T, *Checkpoint, error) {
	if cp == nil || cp.NextNode == "" {
		var zero T
		return zero, nil, fmt.Errorf("flowy: checkpoint required with non-empty NextNode for Resume")
	}
	var lastState T
	var lastNode string
	lastState = state
	lastNode = cp.NextNode
	for step, err := range g.ResumeStream(ctx, state, cp) {
		if err != nil {
			nodeForCp := step.NodeName
			if nodeForCp == "" {
				nodeForCp = lastNode
			}
			stateForReturn := lastState
			if step.NodeName != "" {
				stateForReturn = step.State
			}
			return stateForReturn, &Checkpoint{NextNode: nodeForCp}, err
		}
		lastState = step.State
		lastNode = step.NodeName
	}
	return lastState, nil, nil
}

// runStream executes from startNode, calling yield after each successful node. On error or ErrSuspend, yields (Step{}, err) and returns.
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
			var err error
			state, err = g.runFanOut(ctx, current, state, fo, cfg)
			if err != nil {
				yield(Step[T]{}, err)
				return
			}
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
				state, err = g.runFanOut(ctx, current, state, tempFo, cfg)
				if err != nil {
					yield(Step[T]{}, err)
					return
				}
			}
			if !yield(Step[T]{State: state, NodeName: current}, nil) {
				return
			}
			current = dfo.joinNode
			continue
		}

		node, ok := g.nodes[current]
		if !ok {
			yield(Step[T]{}, fmt.Errorf("flowy: node %q not found", current))
			return
		}
		nodeCtx, cancel := nodeContextWithTimeout(ctx, cfg)
		delta, err := node(nodeCtx, state)
		cancel()
		if err != nil {
			if errors.Is(err, ErrSuspend) {
				yield(Step[T]{State: state, NodeName: current}, ErrSuspend)
				return
			}
			wrapped := fmt.Errorf("flowy: node %q: %w", current, err)
			yield(Step[T]{}, wrapped)
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

func (g *Graph[T]) runFanOut(ctx context.Context, from string, state T, fo *fanOutDef, cfg *runConfig) (T, error) {
	for _, targetName := range fo.targets {
		if g.nodes[targetName] == nil {
			return state, fmt.Errorf("flowy: fan-out target node %q not found", targetName)
		}
	}
	results := make([]T, len(fo.targets))
	gEg, gCtx := errgroup.WithContext(ctx)
	if cfg.maxConcurrency > 0 {
		gEg.SetLimit(cfg.maxConcurrency)
	}
	for i, targetName := range fo.targets {
		node := g.nodes[targetName]
		gEg.Go(func() error {
			nodeCtx, cancel := nodeContextWithTimeout(gCtx, cfg)
			defer cancel()
			delta, err := node(nodeCtx, state)
			if err != nil {
				return fmt.Errorf("flowy: node %q: %w", targetName, err)
			}
			results[i] = delta
			return nil
		})
	}
	if err := gEg.Wait(); err != nil {
		return state, fmt.Errorf("flowy: fan-out from %q: %w", from, err)
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
	if g.nodes[name] != nil {
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

// AsNode returns a node that runs this graph (for composition when state types match).
func (g *Graph[T]) AsNode() Node[T] {
	return func(ctx context.Context, state T) (T, error) {
		final, _, err := g.Invoke(ctx, state)
		return final, err
	}
}
