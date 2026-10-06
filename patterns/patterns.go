package patterns

import (
	"context"
	"errors"
	"fmt"
	"maps"

	"github.com/skosovsky/flowy"
)

// RouteMap maps intents to worker node ids.
type RouteMap map[string]string

// ErrConfiguration rejects a pattern before callbacks can execute.
var ErrConfiguration = errors.New("flowy/patterns: invalid configuration")

// BuildReAct uses replacement updates and fixed react_reason/react_action nodes.
// maxActionRetries bounds action fallback rounds, not total graph steps; at most
// maxActionRetries+1 action callbacks may execute. Predicates must be pure.
func BuildReAct[T, E any](
	reasonNode flowy.Node[T, E],
	actionNode flowy.Node[T, E],
	hasPendingActions func(state T) bool,
	maxActionRetries int,
) (*flowy.GraphBuilder[T, E], error) {
	if reasonNode == nil || actionNode == nil || hasPendingActions == nil || maxActionRetries <= 0 {
		return nil, ErrConfiguration
	}
	builder := flowy.NewGraph[T, E](func(_ T, update T) T { return update })
	builder.AddNode("react_reason", func(ctx context.Context, state T) (T, flowy.Directive, error) {
		update, directive, err := reasonNode(ctx, state)
		if err != nil {
			return update, directive, err
		}
		base, effects, unwrapErr := flowy.UnwrapDirective[E](directive)
		if unwrapErr != nil {
			return update, flowy.End(), unwrapErr
		}
		if !base.IsCompleted() {
			return update, directive, nil
		}
		return update, flowy.WithEffects(flowy.Completed(), effects), nil
	})
	builder.AddNode("react_action", func(ctx context.Context, state T) (T, flowy.Directive, error) {
		update, directive, err := actionNode(ctx, state)
		if err != nil {
			return update, directive, err
		}
		base, effects, unwrapErr := flowy.UnwrapDirective[E](directive)
		if unwrapErr != nil {
			return update, flowy.End(), unwrapErr
		}
		if !base.IsCompleted() {
			return update, directive, nil
		}
		return update, flowy.WithEffects(flowy.Retry(maxActionRetries), effects), nil
	})
	builder.AllowNoOutgoingRoute("react_action")
	builder.AddConditionalEdge("react_reason", func(_ context.Context, state T) (string, error) {
		if hasPendingActions(state) {
			return "react_action", nil
		}
		return flowy.EndNode, nil
	}, "react_action", flowy.EndNode)
	builder.AddRetryRoute("react_action", "react_reason")
	builder.SetEntryPoint("react_reason")
	return builder, nil
}

// BuildDispatchGraph routes once from dispatch to a terminal worker.
// Node updates replace the full state. Routes are copied at construction.
//
//nolint:gocognit // dispatch wiring mirrors route table structure
func BuildDispatchGraph[T, E any](
	dispatchNode flowy.Node[T, E],
	workerNodes map[string]flowy.Node[T, E],
	routeAccessor func(state T) string,
	routes RouteMap,
) (*flowy.GraphBuilder[T, E], error) {
	if dispatchNode == nil || routeAccessor == nil {
		return nil, ErrConfiguration
	}
	for id, worker := range workerNodes {
		if id == "dispatch" || id == flowy.EndNode || worker == nil {
			return nil, ErrConfiguration
		}
	}
	routes = maps.Clone(routes)
	builder := flowy.NewGraph[T, E](func(_ T, update T) T { return update })
	builder.AddNode("dispatch", func(ctx context.Context, state T) (T, flowy.Directive, error) {
		update, directive, err := dispatchNode(ctx, state)
		if err != nil {
			return update, directive, err
		}
		base, effects, unwrapErr := flowy.UnwrapDirective[E](directive)
		if unwrapErr != nil {
			return update, flowy.End(), unwrapErr
		}
		if !base.IsCompleted() {
			return update, directive, nil
		}
		return update, flowy.WithEffects(flowy.Completed(), effects), nil
	})

	for nodeID, workerNode := range workerNodes {
		localNode := workerNode
		localID := nodeID
		builder.AddNode(localID, func(ctx context.Context, state T) (T, flowy.Directive, error) {
			update, directive, err := localNode(ctx, state)
			if err != nil {
				return update, directive, err
			}
			base, effects, unwrapErr := flowy.UnwrapDirective[E](directive)
			if unwrapErr != nil {
				return update, flowy.End(), unwrapErr
			}
			if !base.IsCompleted() {
				return update, directive, nil
			}
			return update, flowy.WithEffects(flowy.End(), effects), nil
		})
		builder.AllowNoOutgoingRoute(localID)
	}

	dispatchTargets := make([]string, 0, len(routes)+1)
	seen := make(map[string]struct{}, len(routes)+1)
	for _, routeNode := range routes {
		if _, ok := seen[routeNode]; ok {
			continue
		}
		seen[routeNode] = struct{}{}
		dispatchTargets = append(dispatchTargets, routeNode)
	}
	dispatchTargets = append(dispatchTargets, flowy.EndNode)

	builder.AddConditionalEdge("dispatch", func(_ context.Context, state T) (string, error) {
		intent := routeAccessor(state)
		routeNode, ok := routes[intent]
		if !ok {
			return "", fmt.Errorf("flowy/patterns: unknown dispatch intent %q", intent)
		}
		return routeNode, nil
	}, dispatchTargets...)
	builder.SetEntryPoint("dispatch")
	return builder, nil
}

// BuildEvaluatorOptimizer uses full-state replacement at generator/evaluator.
// maxCorrectionRetries bounds correction fallbacks, not transport retries.
func BuildEvaluatorOptimizer[T, E any](
	generatorNode flowy.Node[T, E],
	evaluatorNode flowy.Node[T, E],
	isValid func(state T) bool,
	maxCorrectionRetries int,
) (*flowy.GraphBuilder[T, E], error) {
	if generatorNode == nil || evaluatorNode == nil || isValid == nil || maxCorrectionRetries <= 0 {
		return nil, ErrConfiguration
	}
	builder := flowy.NewGraph[T, E](func(_ T, update T) T { return update })
	builder.AddNode("generator", func(ctx context.Context, state T) (T, flowy.Directive, error) {
		update, directive, err := generatorNode(ctx, state)
		if err != nil {
			return update, directive, err
		}
		base, effects, unwrapErr := flowy.UnwrapDirective[E](directive)
		if unwrapErr != nil {
			return update, flowy.End(), unwrapErr
		}
		if !base.IsCompleted() {
			return update, directive, nil
		}
		return update, flowy.WithEffects(flowy.Completed(), effects), nil
	})
	builder.AddNode("evaluator", func(ctx context.Context, state T) (T, flowy.Directive, error) {
		update, directive, err := evaluatorNode(ctx, state)
		if err != nil {
			return update, directive, err
		}
		base, effects, unwrapErr := flowy.UnwrapDirective[E](directive)
		if unwrapErr != nil {
			return update, flowy.End(), unwrapErr
		}
		if !base.IsCompleted() {
			return update, directive, nil
		}
		if isValid(update) {
			return update, flowy.WithEffects(flowy.End(), effects), nil
		}
		return update, flowy.WithEffects(flowy.Retry(maxCorrectionRetries), effects), nil
	})
	builder.AllowNoOutgoingRoute("evaluator")
	builder.AddRetryRoute("evaluator", "generator")
	builder.AddEdge("generator", "evaluator")
	builder.SetEntryPoint("generator")
	return builder, nil
}
