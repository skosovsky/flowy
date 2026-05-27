package patterns

import (
	"context"
	"fmt"

	"github.com/skosovsky/flowy"
)

// RouteMap maps intents to worker node ids.
type RouteMap map[string]string

// BuildReAct creates a protected reasoning/acting loop.
func BuildReAct[T any](
	reasonNode flowy.Node[T],
	actionNode flowy.Node[T],
	hasPendingActions func(state T) bool,
	maxSteps int,
) *flowy.GraphBuilder[T] {
	builder := flowy.NewGraph(func(_ T, update T) T { return update })
	builder.AddNode("react_reason", func(ctx context.Context, state T) (T, flowy.Directive, error) {
		update, directive, err := reasonNode(ctx, state)
		if err != nil {
			return update, directive, err
		}
		base, _, unwrapErr := flowy.UnwrapDirective(directive)
		if unwrapErr != nil {
			return update, flowy.End(), unwrapErr
		}
		if !base.IsCompleted() {
			return update, directive, nil
		}
		if hasPendingActions != nil && hasPendingActions(update) {
			return update, flowy.Completed(), nil
		}
		return update, flowy.End(), nil
	})
	builder.AddNode("react_action", func(ctx context.Context, state T) (T, flowy.Directive, error) {
		update, directive, err := actionNode(ctx, state)
		if err != nil {
			return update, directive, err
		}
		base, _, unwrapErr := flowy.UnwrapDirective(directive)
		if unwrapErr != nil {
			return update, flowy.End(), unwrapErr
		}
		if !base.IsCompleted() {
			return update, directive, nil
		}
		return update, flowy.Retry(maxSteps, "react_reason"), nil
	})
	builder.AddConditionalEdge("react_reason", func(_ context.Context, state T) (string, error) {
		if hasPendingActions != nil && hasPendingActions(state) {
			return "react_action", nil
		}
		return flowy.EndNode, nil
	})
	builder.SetEntryPoint("react_reason")
	return builder
}

// BuildSupervisor creates supervisor routing graph.
func BuildSupervisor[T any](
	supervisorNode flowy.Node[T],
	workerNodes map[string]flowy.Node[T],
	routeAccessor func(state T) string,
	routes RouteMap,
) *flowy.GraphBuilder[T] {
	builder := flowy.NewGraph(func(_ T, update T) T { return update })
	builder.AddNode("supervisor", func(ctx context.Context, state T) (T, flowy.Directive, error) {
		update, directive, err := supervisorNode(ctx, state)
		if err != nil {
			return update, directive, err
		}
		base, _, unwrapErr := flowy.UnwrapDirective(directive)
		if unwrapErr != nil {
			return update, flowy.End(), unwrapErr
		}
		if !base.IsCompleted() {
			return update, directive, nil
		}
		return update, flowy.Completed(), nil
	})

	for nodeID, workerNode := range workerNodes {
		localNode := workerNode
		builder.AddNode(nodeID, func(ctx context.Context, state T) (T, flowy.Directive, error) {
			update, directive, err := localNode(ctx, state)
			if err != nil {
				return update, directive, err
			}
			base, _, unwrapErr := flowy.UnwrapDirective(directive)
			if unwrapErr != nil {
				return update, flowy.End(), unwrapErr
			}
			if !base.IsCompleted() {
				return update, directive, nil
			}
			return update, flowy.End(), nil
		})
	}

	builder.AddConditionalEdge("supervisor", func(_ context.Context, state T) (string, error) {
		intent := ""
		if routeAccessor != nil {
			intent = routeAccessor(state)
		}
		routeNode, ok := routes[intent]
		if !ok {
			return "", fmt.Errorf("flowy/patterns: unknown supervisor intent %q", intent)
		}
		return routeNode, nil
	})
	builder.SetEntryPoint("supervisor")
	return builder
}

// BuildEvaluatorOptimizer creates generate/evaluate correction loop.
func BuildEvaluatorOptimizer[T any](
	generatorNode flowy.Node[T],
	evaluatorNode flowy.Node[T],
	isValid func(state T) bool,
	maxRetries int,
) *flowy.GraphBuilder[T] {
	builder := flowy.NewGraph(func(_ T, update T) T { return update })
	builder.AddNode("generator", func(ctx context.Context, state T) (T, flowy.Directive, error) {
		update, directive, err := generatorNode(ctx, state)
		if err != nil {
			return update, directive, err
		}
		base, _, unwrapErr := flowy.UnwrapDirective(directive)
		if unwrapErr != nil {
			return update, flowy.End(), unwrapErr
		}
		if !base.IsCompleted() {
			return update, directive, nil
		}
		return update, flowy.Completed(), nil
	})
	builder.AddNode("evaluator", func(ctx context.Context, state T) (T, flowy.Directive, error) {
		update, directive, err := evaluatorNode(ctx, state)
		if err != nil {
			return update, directive, err
		}
		base, _, unwrapErr := flowy.UnwrapDirective(directive)
		if unwrapErr != nil {
			return update, flowy.End(), unwrapErr
		}
		if !base.IsCompleted() {
			return update, directive, nil
		}
		if isValid != nil && isValid(update) {
			return update, flowy.End(), nil
		}
		return update, flowy.Retry(maxRetries, "generator"), nil
	})
	builder.AddEdge("generator", "evaluator")
	builder.SetEntryPoint("generator")
	return builder
}
