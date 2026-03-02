package flowy

import (
	"context"
	"errors"
	"fmt"

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
	interruptBefore  map[string]bool
	interruptAfter   map[string]bool
	reducer          Reducer[T]
	defaults         runConfig[T]
}

// Invoke runs the graph synchronously and returns the final state.
func (g *Graph[T]) Invoke(ctx context.Context, state T, opts ...Option[T]) (T, error) {
	cfg := applyOptions(&g.defaults, opts)
	return g.runFrom(ctx, state, g.entryPoint, cfg, nil, false)
}

func (g *Graph[T]) runFanOut(ctx context.Context, from string, state T, fo *fanOutDef, cfg *runConfig[T]) (T, error) {
	// Pre-validate all targets before starting any goroutine (avoids partial execution on invalid list).
	for _, targetName := range fo.targets {
		if g.interruptBefore[targetName] {
			return state, fmt.Errorf("flowy: interruptBefore on fan-out target %q is not supported (execution from %q)", targetName, from)
		}
		if g.interruptAfter[targetName] {
			return state, fmt.Errorf("flowy: interruptAfter on fan-out target %q is not supported (execution from %q)", targetName, from)
		}
		if g.nodes[targetName] == nil {
			return state, fmt.Errorf("flowy: fan-out target node %q not found", targetName)
		}
	}

	results := make([]T, len(fo.targets))
	gEg, gCtx := errgroup.WithContext(ctx)
	// Set limit before any gEg.Go() so fan-out never exceeds maxConcurrency goroutines.
	if cfg.maxConcurrency > 0 {
		gEg.SetLimit(cfg.maxConcurrency)
	}
	for i, targetName := range fo.targets {
		node := g.nodes[targetName] // already validated above
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

// resolveNext returns the next node name after current with the given state.
// It returns an error if the conditional router fails or if there is no outgoing edge.
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

// Stream runs the graph in a goroutine and sends events to the returned channel.
// The channel is closed when execution finishes or ctx is cancelled.
//
// The consumer MUST either drain the channel (read until it is closed) or cancel ctx.
// Otherwise the sender may block forever and the execution goroutine will leak.
func (g *Graph[T]) Stream(ctx context.Context, state T, opts ...Option[T]) <-chan Event[T] {
	ch := make(chan Event[T])
	cfg := applyOptions(&g.defaults, opts)
	go func() {
		defer close(ch)
		_, _ = g.runFrom(ctx, state, g.entryPoint, cfg, ch, false)
	}()
	return ch
}

// sendEvent sends an event to ch if non-nil, respecting context cancellation to avoid goroutine leak.
func sendEvent[T any](ctx context.Context, ch chan<- Event[T], ev Event[T]) {
	if ch == nil {
		return
	}
	select {
	case ch <- ev:
	case <-ctx.Done():
	}
}

// Resume continues execution after an interrupt. Delta is merged with the saved state via the reducer.
func (g *Graph[T]) Resume(ctx context.Context, threadID string, delta T, opts ...Option[T]) (T, error) {
	cfg := applyOptions(&g.defaults, opts)
	if cfg.checkpointer == nil {
		var zero T
		return zero, errors.New("flowy: checkpointer required for Resume")
	}
	if threadID == "" {
		var zero T
		return zero, errors.New("flowy: threadID must not be empty for Resume")
	}
	cp, err := cfg.checkpointer.Load(ctx, threadID)
	if err != nil {
		var zero T
		return zero, err
	}
	if cp.NodeName == "" {
		var zero T
		return zero, errors.New("flowy: checkpoint has empty NodeName")
	}
	if _, hasNode := g.nodes[cp.NodeName]; !hasNode {
		if _, hasFan := g.fanOuts[cp.NodeName]; !hasFan {
			if _, hasDyn := g.dynamicFanOuts[cp.NodeName]; !hasDyn {
				var zero T
				return zero, fmt.Errorf("flowy: checkpoint node %q not found in graph", cp.NodeName)
			}
		}
	}
	state := g.reducer(cp.State, delta)
	cfg.threadID = threadID

	// Run from the saved node (the one we interrupted before); skip interruptBefore on first step.
	return g.runFrom(ctx, state, cp.NodeName, cfg, nil, true)
}

// runFrom executes the graph starting at startNode (used by Invoke with entryPoint and by Resume).
// If eventCh is non-nil, events are sent via sendEvent (respecting context cancellation).
// When resumeFirst is true, interruptBefore is skipped on the first iteration (we are resuming to run that node).
func (g *Graph[T]) runFrom(ctx context.Context, state T, startNode string, cfg runConfig[T], eventCh chan<- Event[T], resumeFirst bool) (T, error) {
	current := startNode
	for step := 0; step < cfg.maxSteps; step++ {
		if err := ctx.Err(); err != nil {
			wrapped := fmt.Errorf("flowy: %w", err)
			sendEvent(ctx, eventCh, Event[T]{Type: EventError, State: state, Err: wrapped})
			return state, wrapped
		}
		skipInterruptBefore := resumeFirst && step == 0
		if !skipInterruptBefore && g.interruptBefore[current] && cfg.checkpointer != nil && cfg.threadID != "" {
			cp := Checkpoint[T]{State: state, NodeName: current}
			if err := cfg.checkpointer.Save(ctx, cfg.threadID, cp); err != nil {
				wrapped := fmt.Errorf("flowy: save checkpoint: %w", err)
				sendEvent(ctx, eventCh, Event[T]{Type: EventError, NodeName: current, State: state, Err: wrapped})
				return state, wrapped
			}
			sendEvent(ctx, eventCh, Event[T]{Type: EventInterrupt, NodeName: current, State: state})
			return state, ErrInterrupt
		}

		if fo, isFanOut := g.fanOuts[current]; isFanOut {
			sendEvent(ctx, eventCh, Event[T]{Type: EventNodeStart, NodeName: current, State: state})
			var err error
			state, err = g.runFanOut(ctx, current, state, fo, &cfg)
			if err != nil {
				sendEvent(ctx, eventCh, Event[T]{Type: EventError, NodeName: current, State: state, Err: err})
				return state, err
			}
			sendEvent(ctx, eventCh, Event[T]{Type: EventNodeEnd, NodeName: current, State: state})
			current = fo.joinNode
		} else if dfo, isDynFanOut := g.dynamicFanOuts[current]; isDynFanOut {
			sendEvent(ctx, eventCh, Event[T]{Type: EventNodeStart, NodeName: current, State: state})
			targets, err := dfo.router(ctx, state)
			if err != nil {
				wrapped := fmt.Errorf("flowy: dynamic fan-out router %q failed: %w", current, err)
				sendEvent(ctx, eventCh, Event[T]{Type: EventError, NodeName: current, State: state, Err: wrapped})
				return state, wrapped
			}
			if len(targets) > 0 {
				tempFo := &fanOutDef{targets: targets, joinNode: dfo.joinNode}
				state, err = g.runFanOut(ctx, current, state, tempFo, &cfg)
				if err != nil {
					sendEvent(ctx, eventCh, Event[T]{Type: EventError, NodeName: current, State: state, Err: err})
					return state, err
				}
			}
			sendEvent(ctx, eventCh, Event[T]{Type: EventNodeEnd, NodeName: current, State: state})
			current = dfo.joinNode
		} else {
			node, ok := g.nodes[current]
			if !ok {
				err := fmt.Errorf("flowy: node %q not found", current)
				sendEvent(ctx, eventCh, Event[T]{Type: EventError, NodeName: current, State: state, Err: err})
				return state, err
			}
			sendEvent(ctx, eventCh, Event[T]{Type: EventNodeStart, NodeName: current, State: state})
			nodeCtx, cancel := nodeContextWithTimeout(ctx, &cfg)
			delta, err := node(nodeCtx, state)
			cancel()
			if err != nil {
				wrapped := fmt.Errorf("flowy: node %q: %w", current, err)
				sendEvent(ctx, eventCh, Event[T]{Type: EventError, NodeName: current, State: state, Err: wrapped})
				return state, wrapped
			}
			state = g.reducer(state, delta)
			sendEvent(ctx, eventCh, Event[T]{Type: EventNodeEnd, NodeName: current, State: state})
			// Finish point takes precedence: complete without interrupt (no resolveNext for terminal node).
			if g.finishPoints[current] {
				return state, nil
			}
			if g.interruptAfter[current] && cfg.checkpointer != nil && cfg.threadID != "" {
				next, err := g.resolveNext(ctx, current, state)
				if err != nil {
					sendEvent(ctx, eventCh, Event[T]{Type: EventError, NodeName: current, State: state, Err: err})
					return state, err
				}
				cp := Checkpoint[T]{State: state, NodeName: next}
				if err := cfg.checkpointer.Save(ctx, cfg.threadID, cp); err != nil {
					wrapped := fmt.Errorf("flowy: save checkpoint: %w", err)
					sendEvent(ctx, eventCh, Event[T]{Type: EventError, NodeName: current, State: state, Err: wrapped})
					return state, wrapped
				}
				sendEvent(ctx, eventCh, Event[T]{Type: EventInterrupt, NodeName: current, State: state})
				return state, ErrInterrupt
			}
			next, err := g.resolveNext(ctx, current, state)
			if err != nil {
				sendEvent(ctx, eventCh, Event[T]{Type: EventError, NodeName: current, State: state, Err: err})
				return state, err
			}
			current = next
		}
	}
	sendEvent(ctx, eventCh, Event[T]{Type: EventError, State: state, Err: ErrMaxStepsExceeded})
	return state, ErrMaxStepsExceeded
}

// AsNode returns a node that runs this graph (for composition).
func (g *Graph[T]) AsNode() Node[T] {
	return func(ctx context.Context, state T) (T, error) {
		return g.Invoke(ctx, state)
	}
}
