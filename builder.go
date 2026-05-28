package flowy

import (
	"context"
	"errors"
	"fmt"
	"maps"
)

type nodeDef[T, E any] struct {
	handler Node[T, E]
}

// EdgeRouter returns next node id for a completed node.
type EdgeRouter[T any] func(ctx context.Context, state T) (string, error)

type conditionalEdgeDef[T any] struct {
	router  EdgeRouter[T]
	allowed map[string]struct{}
}

// GraphBuilder builds a graph before compilation. Fluent API.
type GraphBuilder[T, E any] struct {
	reducer               Reducer[T]
	nodes                 map[string]nodeDef[T, E]
	edges                 map[string]string
	conditionalEdges      map[string]conditionalEdgeDef[T]
	retryRoutes           map[string]string
	middlewares           []NodeMiddleware[T, E]
	entryPoint            string
	overwrittenNodes      map[string]bool
	overwrittenEdgeFrom   map[string]bool
	overwrittenRouterFrom map[string]bool
	overwrittenRetryFrom  map[string]bool
	noOutgoingRoute       map[string]bool
}

// NewGraph creates a new graph builder with the given reducer.
func NewGraph[T, E any](reducer Reducer[T]) *GraphBuilder[T, E] {
	return &GraphBuilder[T, E]{
		reducer:               reducer,
		nodes:                 make(map[string]nodeDef[T, E]),
		edges:                 make(map[string]string),
		conditionalEdges:      make(map[string]conditionalEdgeDef[T]),
		retryRoutes:           make(map[string]string),
		overwrittenNodes:      make(map[string]bool),
		overwrittenEdgeFrom:   make(map[string]bool),
		overwrittenRouterFrom: make(map[string]bool),
		overwrittenRetryFrom:  make(map[string]bool),
		noOutgoingRoute:       make(map[string]bool),
	}
}

// AllowNoOutgoingRoute exempts a node from compile-time outgoing route requirement.
// Use for nodes that terminate via End/Fail/Suspend/Retry without Completed routing.
func (b *GraphBuilder[T, E]) AllowNoOutgoingRoute(name string) *GraphBuilder[T, E] {
	b.noOutgoingRoute[name] = true
	return b
}

// AddNode registers a node by name. Returns the builder for chaining.
func (b *GraphBuilder[T, E]) AddNode(name string, fn Node[T, E]) *GraphBuilder[T, E] {
	if _, exists := b.nodes[name]; exists {
		b.overwrittenNodes[name] = true
	}
	b.nodes[name] = nodeDef[T, E]{handler: fn}
	return b
}

// AddEdge adds a static edge from -> to.
func (b *GraphBuilder[T, E]) AddEdge(from, to string) *GraphBuilder[T, E] {
	if _, exists := b.edges[from]; exists {
		b.overwrittenEdgeFrom[from] = true
	}
	b.edges[from] = to
	return b
}

// AddConditionalEdge sets dynamic router from node.
// allowedTargets declares every target the router may return (include EndNode when applicable).
func (b *GraphBuilder[T, E]) AddConditionalEdge(
	from string,
	router EdgeRouter[T],
	allowedTargets ...string,
) *GraphBuilder[T, E] {
	if _, exists := b.conditionalEdges[from]; exists {
		b.overwrittenRouterFrom[from] = true
	}
	def := conditionalEdgeDef[T]{router: router, allowed: nil}
	if len(allowedTargets) > 0 {
		def.allowed = make(map[string]struct{}, len(allowedTargets))
		for _, target := range allowedTargets {
			def.allowed[target] = struct{}{}
		}
	}
	b.conditionalEdges[from] = def
	return b
}

// AddRetryRoute declares a Retry fallback target for compile-time validation.
func (b *GraphBuilder[T, E]) AddRetryRoute(fromNode, fallbackNode string) *GraphBuilder[T, E] {
	if _, exists := b.retryRoutes[fromNode]; exists {
		b.overwrittenRetryFrom[fromNode] = true
	}
	b.retryRoutes[fromNode] = fallbackNode
	return b
}

// Use appends global node middlewares in declaration order.
func (b *GraphBuilder[T, E]) Use(middlewares ...NodeMiddleware[T, E]) *GraphBuilder[T, E] {
	b.middlewares = append(b.middlewares, middlewares...)
	return b
}

// SetEntryPoint sets the node where execution starts.
func (b *GraphBuilder[T, E]) SetEntryPoint(name string) *GraphBuilder[T, E] {
	b.entryPoint = name
	return b
}

// Compile validates the graph and returns an immutable Graph.
func (b *GraphBuilder[T, E]) Compile(opts ...BuildOption) (*Graph[T, E], error) {
	errs := b.collectCompileErrors()
	if len(errs) > 0 {
		return nil, errors.Join(errs...)
	}

	o := applyBuildOptions(opts)
	return b.buildGraph(o), nil
}

func (b *GraphBuilder[T, E]) collectCompileErrors() []error {
	var errs []error
	errs = append(errs, b.validateOverwriteConflicts()...)
	errs = append(errs, b.validateTopology()...)
	errs = append(errs, b.validateReferences()...)
	errs = append(errs, b.validateHandlers()...)
	errs = append(errs, b.validateRoutingConflicts()...)
	errs = append(errs, b.validateOutgoingRoutes()...)
	errs = append(errs, b.validateConditionalAllowedTargets()...)
	errs = append(errs, b.validateRetryRoutes()...)
	errs = append(errs, b.validateNoOutgoingRouteMisuse()...)
	errs = append(errs, b.validateNoOutgoingRouteReferences()...)
	errs = append(errs, b.validateRetryRouteExemptions()...)
	return errs
}

func (b *GraphBuilder[T, E]) validateOverwriteConflicts() []error {
	var errs []error
	for name := range b.overwrittenNodes {
		errs = append(errs, fmt.Errorf("flowy: node %q registered more than once", name))
	}
	for from := range b.overwrittenEdgeFrom {
		errs = append(errs, fmt.Errorf("flowy: edge from %q registered more than once", from))
	}
	for from := range b.overwrittenRouterFrom {
		errs = append(errs, fmt.Errorf("flowy: conditional edge from %q registered more than once", from))
	}
	for from := range b.overwrittenRetryFrom {
		errs = append(errs, fmt.Errorf("flowy: retry route from %q registered more than once", from))
	}
	return errs
}

func (b *GraphBuilder[T, E]) validateTopology() []error {
	var errs []error
	if b.reducer == nil {
		errs = append(errs, errors.New("flowy: reducer must not be nil"))
	}
	if b.entryPoint == "" {
		errs = append(errs, errors.New("flowy: entry point not set"))
	} else if _, ok := b.nodes[b.entryPoint]; !ok {
		errs = append(errs, fmt.Errorf("flowy: entry point %q is not a registered node", b.entryPoint))
	}
	return errs
}

func (b *GraphBuilder[T, E]) validateReferences() []error {
	referenced := make(map[string]bool)
	for from, to := range b.edges {
		referenced[from] = true
		if to != EndNode {
			referenced[to] = true
		}
	}
	for from := range b.conditionalEdges {
		referenced[from] = true
	}
	for name := range b.nodes {
		referenced[name] = true
	}

	var errs []error
	for name := range referenced {
		if name == "" {
			errs = append(errs, errors.New("flowy: node name must not be empty"))
			continue
		}
		if name == EndNode {
			continue
		}
		if _, ok := b.nodes[name]; !ok {
			errs = append(errs, fmt.Errorf("flowy: node %q not registered", name))
		}
	}
	return errs
}

func (b *GraphBuilder[T, E]) validateHandlers() []error {
	var errs []error
	for name, node := range b.nodes {
		if node.handler == nil {
			errs = append(errs, fmt.Errorf("flowy: node %q has nil handler", name))
		}
	}
	for from, def := range b.conditionalEdges {
		if def.router == nil {
			errs = append(errs, fmt.Errorf("flowy: conditional edge from %q has nil router", from))
		}
	}
	return errs
}

func (b *GraphBuilder[T, E]) validateConditionalAllowedTargets() []error {
	var errs []error
	for from, def := range b.conditionalEdges {
		if len(def.allowed) == 0 {
			errs = append(errs, fmt.Errorf(
				"flowy: conditional edge from %q requires allowedTargets at compile time",
				from,
			))
			continue
		}
		for target := range def.allowed {
			if target == EndNode {
				continue
			}
			if _, ok := b.nodes[target]; !ok {
				errs = append(errs, fmt.Errorf(
					"flowy: conditional edge from %q allows unknown target %q",
					from, target,
				))
			}
		}
	}
	return errs
}

func (b *GraphBuilder[T, E]) validateRetryRoutes() []error {
	var errs []error
	for from, fallback := range b.retryRoutes {
		if _, ok := b.nodes[from]; !ok {
			errs = append(errs, fmt.Errorf("flowy: retry route source %q is not registered", from))
		}
		if fallback == "" {
			errs = append(errs, fmt.Errorf("flowy: retry route from %q has empty fallback", from))
			continue
		}
		if _, ok := b.nodes[fallback]; !ok {
			errs = append(errs, fmt.Errorf(
				"flowy: retry route from %q references unknown fallback %q",
				from, fallback,
			))
		}
	}
	return errs
}

func (b *GraphBuilder[T, E]) validateRoutingConflicts() []error {
	var errs []error
	for from := range b.edges {
		if _, ok := b.conditionalEdges[from]; ok {
			errs = append(errs, fmt.Errorf("flowy: node %q has both edge and conditional edge", from))
		}
	}
	return errs
}

// validateOutgoingRoutes ensures nodes that use Completed() have declarative routes at compile time.
func (b *GraphBuilder[T, E]) validateOutgoingRoutes() []error {
	var errs []error
	for name := range b.nodes {
		if b.noOutgoingRoute[name] {
			continue
		}
		_, hasEdge := b.edges[name]
		_, hasConditional := b.conditionalEdges[name]
		if !hasEdge && !hasConditional {
			errs = append(errs, fmt.Errorf(
				"flowy: node %q has no outgoing edge or conditional edge (required for Completed() routing)",
				name,
			))
		}
	}
	return errs
}

func (b *GraphBuilder[T, E]) validateNoOutgoingRouteMisuse() []error {
	var errs []error
	for name := range b.noOutgoingRoute {
		if _, ok := b.edges[name]; ok {
			errs = append(errs, fmt.Errorf(
				"flowy: node %q has AllowNoOutgoingRoute but also has a static edge",
				name,
			))
		}
		if _, ok := b.conditionalEdges[name]; ok {
			errs = append(errs, fmt.Errorf(
				"flowy: node %q has AllowNoOutgoingRoute but also has a conditional edge",
				name,
			))
		}
	}
	return errs
}

func (b *GraphBuilder[T, E]) validateNoOutgoingRouteReferences() []error {
	var errs []error
	for name := range b.noOutgoingRoute {
		if _, ok := b.nodes[name]; !ok {
			errs = append(errs, fmt.Errorf(
				"flowy: AllowNoOutgoingRoute for unknown node %q",
				name,
			))
		}
	}
	return errs
}

func (b *GraphBuilder[T, E]) validateRetryRouteExemptions() []error {
	var errs []error
	for from := range b.retryRoutes {
		if !b.noOutgoingRoute[from] {
			errs = append(errs, fmt.Errorf(
				"flowy: node %q has AddRetryRoute but is not exempt via AllowNoOutgoingRoute",
				from,
			))
		}
	}
	return errs
}

func (b *GraphBuilder[T, E]) buildGraph(o buildOpts) *Graph[T, E] {
	routers := make(map[string]EdgeRouter[T], len(b.conditionalEdges))
	allowed := make(map[string]map[string]struct{}, len(b.conditionalEdges))
	for from, def := range b.conditionalEdges {
		routers[from] = def.router
		if len(def.allowed) > 0 {
			allowed[from] = maps.Clone(def.allowed)
		}
	}
	return &Graph[T, E]{
		nodes:              copyNodeDefs(b.nodes, b.middlewares),
		edges:              maps.Clone(b.edges),
		conditionalEdges:   routers,
		conditionalAllowed: allowed,
		retryRoutes:        maps.Clone(b.retryRoutes),
		entryPoint:         b.entryPoint,
		reducer:            b.reducer,
		defaults:           o.run,
	}
}

func copyNodeDefs[T, E any](m map[string]nodeDef[T, E], middlewares []NodeMiddleware[T, E]) map[string]nodeDef[T, E] {
	out := make(map[string]nodeDef[T, E], len(m))
	for k, v := range m {
		out[k] = nodeDef[T, E]{
			handler: wrapNodeWithMiddlewares(v.handler, middlewares),
		}
	}
	return out
}
