package flowy

import (
	"context"
	"errors"
	"fmt"
	"maps"
)

type nodeDef[T any] struct {
	handler Node[T]
}

// EdgeRouter returns next node id for a completed node.
type EdgeRouter[T any] func(ctx context.Context, state T) (string, error)

// GraphBuilder builds a graph before compilation. Fluent API.
type GraphBuilder[T any] struct {
	reducer               Reducer[T]
	nodes                 map[string]nodeDef[T]
	edges                 map[string]string
	conditionalEdges      map[string]EdgeRouter[T]
	middlewares           []NodeMiddleware[T]
	entryPoint            string
	overwrittenNodes      map[string]bool
	overwrittenEdgeFrom   map[string]bool
	overwrittenRouterFrom map[string]bool
}

// NewGraph creates a new graph builder with the given reducer.
func NewGraph[T any](reducer Reducer[T]) *GraphBuilder[T] {
	return &GraphBuilder[T]{
		reducer:               reducer,
		nodes:                 make(map[string]nodeDef[T]),
		edges:                 make(map[string]string),
		conditionalEdges:      make(map[string]EdgeRouter[T]),
		overwrittenNodes:      make(map[string]bool),
		overwrittenEdgeFrom:   make(map[string]bool),
		overwrittenRouterFrom: make(map[string]bool),
	}
}

// AddNode registers a node by name. Returns the builder for chaining.
func (b *GraphBuilder[T]) AddNode(name string, fn Node[T]) *GraphBuilder[T] {
	if _, exists := b.nodes[name]; exists {
		b.overwrittenNodes[name] = true
	}
	b.nodes[name] = nodeDef[T]{handler: fn}
	return b
}

// AddEdge adds a static edge from -> to.
func (b *GraphBuilder[T]) AddEdge(from, to string) *GraphBuilder[T] {
	if _, exists := b.edges[from]; exists {
		b.overwrittenEdgeFrom[from] = true
	}
	b.edges[from] = to
	return b
}

// AddConditionalEdge sets dynamic router from node.
func (b *GraphBuilder[T]) AddConditionalEdge(from string, router EdgeRouter[T]) *GraphBuilder[T] {
	if _, exists := b.conditionalEdges[from]; exists {
		b.overwrittenRouterFrom[from] = true
	}
	b.conditionalEdges[from] = router
	return b
}

// Use appends global node middlewares in declaration order.
func (b *GraphBuilder[T]) Use(middlewares ...NodeMiddleware[T]) *GraphBuilder[T] {
	b.middlewares = append(b.middlewares, middlewares...)
	return b
}

// SetEntryPoint sets the node where execution starts.
func (b *GraphBuilder[T]) SetEntryPoint(name string) *GraphBuilder[T] {
	b.entryPoint = name
	return b
}

// Compile validates the graph and returns an immutable Graph. BuildOptions set run config (e.g. WithMaxSteps).
func (b *GraphBuilder[T]) Compile(opts ...BuildOption) (*Graph[T], error) {
	errs := b.collectCompileErrors()
	if len(errs) > 0 {
		return nil, errors.Join(errs...)
	}

	o := applyBuildOptions(opts)
	return b.buildGraph(o), nil
}

func (b *GraphBuilder[T]) collectCompileErrors() []error {
	var errs []error
	errs = append(errs, b.validateOverwriteConflicts()...)
	errs = append(errs, b.validateTopology()...)
	errs = append(errs, b.validateReferences()...)
	errs = append(errs, b.validateHandlers()...)
	errs = append(errs, b.validateRoutingConflicts()...)
	return errs
}

func (b *GraphBuilder[T]) validateOverwriteConflicts() []error {
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
	return errs
}

func (b *GraphBuilder[T]) validateTopology() []error {
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

func (b *GraphBuilder[T]) validateReferences() []error {
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
		if _, ok := b.nodes[name]; !ok {
			errs = append(errs, fmt.Errorf("flowy: node %q not registered", name))
		}
	}
	return errs
}

func (b *GraphBuilder[T]) validateHandlers() []error {
	var errs []error
	for name, node := range b.nodes {
		if node.handler == nil {
			errs = append(errs, fmt.Errorf("flowy: node %q has nil handler", name))
		}
	}
	for from, router := range b.conditionalEdges {
		if router == nil {
			errs = append(errs, fmt.Errorf("flowy: conditional edge from %q has nil router", from))
		}
	}
	return errs
}

func (b *GraphBuilder[T]) validateRoutingConflicts() []error {
	var errs []error
	for from := range b.edges {
		if _, ok := b.conditionalEdges[from]; ok {
			errs = append(errs, fmt.Errorf("flowy: node %q has both edge and conditional edge", from))
		}
	}
	return errs
}

func (b *GraphBuilder[T]) buildGraph(o buildOpts) *Graph[T] {
	return &Graph[T]{
		nodes:            copyNodeDefs(b.nodes, b.middlewares),
		edges:            maps.Clone(b.edges),
		conditionalEdges: maps.Clone(b.conditionalEdges),
		entryPoint:       b.entryPoint,
		reducer:          b.reducer,
		defaults:         o.run,
	}
}

func copyNodeDefs[T any](m map[string]nodeDef[T], middlewares []NodeMiddleware[T]) map[string]nodeDef[T] {
	out := make(map[string]nodeDef[T], len(m))
	for k, v := range m {
		out[k] = nodeDef[T]{
			handler: wrapNodeWithMiddlewares(v.handler, middlewares),
		}
	}
	return out
}
