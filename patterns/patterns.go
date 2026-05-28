package patterns

import (
	"context"
	"fmt"

	"github.com/skosovsky/flowy"
)

// RouteMap maps intents to worker node ids.
type RouteMap map[string]string

// BuildReAct creates a protected reasoning/acting loop.
func BuildReAct[T, E any](
	reasonNode flowy.Node[T, E],
	actionNode flowy.Node[T, E],
	hasPendingActions func(state T) bool,
	maxSteps int,
) *flowy.GraphBuilder[T, E] {
	builder := flowy.NewGraph[T, E](func(_ T, update T) T { return update })
	builder.AddNode("react_reason", func(ctx context.Context, state T) (T, flowy.Directive, error) {
		update, directive, err := reasonNode(ctx, state)
		if err != nil {
			return update, directive, err
		}
		base, _, unwrapErr := flowy.UnwrapDirective[E](directive)
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
		base, _, unwrapErr := flowy.UnwrapDirective[E](directive)
		if unwrapErr != nil {
			return update, flowy.End(), unwrapErr
		}
		if !base.IsCompleted() {
			return update, directive, nil
		}
		return update, flowy.Retry(maxSteps), nil
	})
	builder.AllowNoOutgoingRoute("react_action")
	builder.AddConditionalEdge("react_reason", func(_ context.Context, state T) (string, error) {
		if hasPendingActions != nil && hasPendingActions(state) {
			return "react_action", nil
		}
		return flowy.EndNode, nil
	}, "react_action", flowy.EndNode)
	builder.AddRetryRoute("react_action", "react_reason")
	builder.SetEntryPoint("react_reason")
	return builder
}

// BuildSupervisor creates supervisor routing graph.
//
//nolint:gocognit // supervisor wiring mirrors route table structure
func BuildSupervisor[T, E any](
	supervisorNode flowy.Node[T, E],
	workerNodes map[string]flowy.Node[T, E],
	routeAccessor func(state T) string,
	routes RouteMap,
) *flowy.GraphBuilder[T, E] {
	builder := flowy.NewGraph[T, E](func(_ T, update T) T { return update })
	builder.AddNode("supervisor", func(ctx context.Context, state T) (T, flowy.Directive, error) {
		update, directive, err := supervisorNode(ctx, state)
		if err != nil {
			return update, directive, err
		}
		base, _, unwrapErr := flowy.UnwrapDirective[E](directive)
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
		localID := nodeID
		builder.AddNode(localID, func(ctx context.Context, state T) (T, flowy.Directive, error) {
			update, directive, err := localNode(ctx, state)
			if err != nil {
				return update, directive, err
			}
			base, _, unwrapErr := flowy.UnwrapDirective[E](directive)
			if unwrapErr != nil {
				return update, flowy.End(), unwrapErr
			}
			if !base.IsCompleted() {
				return update, directive, nil
			}
			return update, flowy.End(), nil
		})
		builder.AllowNoOutgoingRoute(localID)
	}

	supervisorTargets := make([]string, 0, len(routes)+1)
	seen := make(map[string]struct{}, len(routes)+1)
	for _, routeNode := range routes {
		if _, ok := seen[routeNode]; ok {
			continue
		}
		seen[routeNode] = struct{}{}
		supervisorTargets = append(supervisorTargets, routeNode)
	}
	supervisorTargets = append(supervisorTargets, flowy.EndNode)

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
	}, supervisorTargets...)
	builder.SetEntryPoint("supervisor")
	return builder
}

// BuildEvaluatorOptimizer creates generate/evaluate correction loop.
func BuildEvaluatorOptimizer[T, E any](
	generatorNode flowy.Node[T, E],
	evaluatorNode flowy.Node[T, E],
	isValid func(state T) bool,
	maxRetries int,
) *flowy.GraphBuilder[T, E] {
	builder := flowy.NewGraph[T, E](func(_ T, update T) T { return update })
	builder.AddNode("generator", func(ctx context.Context, state T) (T, flowy.Directive, error) {
		update, directive, err := generatorNode(ctx, state)
		if err != nil {
			return update, directive, err
		}
		base, _, unwrapErr := flowy.UnwrapDirective[E](directive)
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
		base, _, unwrapErr := flowy.UnwrapDirective[E](directive)
		if unwrapErr != nil {
			return update, flowy.End(), unwrapErr
		}
		if !base.IsCompleted() {
			return update, directive, nil
		}
		if isValid != nil && isValid(update) {
			return update, flowy.End(), nil
		}
		return update, flowy.Retry(maxRetries), nil
	})
	builder.AllowNoOutgoingRoute("evaluator")
	builder.AddRetryRoute("evaluator", "generator")
	builder.AddEdge("generator", "evaluator")
	builder.SetEntryPoint("generator")
	return builder
}
