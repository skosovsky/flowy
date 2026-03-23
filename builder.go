package flowy

import (
	"errors"
	"fmt"
	"maps"
)

type nodeDef[T any] struct {
	handler             Node[T]
	middlewares         []Middleware[T] // per-node middlewares (builder state)
	compiledMiddlewares []Middleware[T] // globals + middlewares, fixed at Compile (hot path)
}

// GraphBuilder builds a graph before compilation. Fluent API.
type GraphBuilder[T any] struct {
	reducer           Reducer[T]
	nodes             map[string]nodeDef[T]
	edges             map[string]string
	choices           map[string]Choice[T]
	globalMiddlewares []Middleware[T]
	entryPoint        string
	finishPoints      map[string]bool
	// overwritten* track keys that were re-registered (overwrite); Compile reports them as errors.
	overwrittenNodes      map[string]bool
	overwrittenEdgeFrom   map[string]bool
	overwrittenChoiceFrom map[string]bool
}

// NewGraph creates a new graph builder with the given reducer.
func NewGraph[T any](reducer Reducer[T]) *GraphBuilder[T] {
	return &GraphBuilder[T]{
		reducer:               reducer,
		nodes:                 make(map[string]nodeDef[T]),
		edges:                 make(map[string]string),
		choices:               make(map[string]Choice[T]),
		finishPoints:          make(map[string]bool),
		overwrittenNodes:      make(map[string]bool),
		overwrittenEdgeFrom:   make(map[string]bool),
		overwrittenChoiceFrom: make(map[string]bool),
	}
}

// AddNode registers a node by name. Returns the builder for chaining.
func (b *GraphBuilder[T]) AddNode(name string, fn Node[T], mws ...Middleware[T]) *GraphBuilder[T] {
	if _, exists := b.nodes[name]; exists {
		b.overwrittenNodes[name] = true
	}
	nodeMws := append([]Middleware[T](nil), mws...)
	b.nodes[name] = nodeDef[T]{handler: fn, middlewares: nodeMws, compiledMiddlewares: nil}
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

// AddChoice sets a router for the given node; the router returns the next node name.
func (b *GraphBuilder[T]) AddChoice(from string, router Choice[T]) *GraphBuilder[T] {
	if _, exists := b.choices[from]; exists {
		b.overwrittenChoiceFrom[from] = true
	}
	b.choices[from] = router
	return b
}

// Use adds middlewares that wrap every node at compile time (first added runs first).
func (b *GraphBuilder[T]) Use(mw ...Middleware[T]) *GraphBuilder[T] {
	b.globalMiddlewares = append(b.globalMiddlewares, mw...)
	return b
}

// SetEntryPoint sets the node where execution starts.
func (b *GraphBuilder[T]) SetEntryPoint(name string) *GraphBuilder[T] {
	b.entryPoint = name
	return b
}

// SetFinishPoint marks a node as a valid terminal (execution stops when reached).
func (b *GraphBuilder[T]) SetFinishPoint(name string) *GraphBuilder[T] {
	b.finishPoints[name] = true
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
	errs = append(errs, b.validateFinishPoints()...)
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
	for from := range b.overwrittenChoiceFrom {
		errs = append(errs, fmt.Errorf("flowy: choice from %q registered more than once", from))
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
	if len(b.finishPoints) == 0 {
		errs = append(errs, errors.New("flowy: no finish point set"))
	}
	return errs
}

func (b *GraphBuilder[T]) validateReferences() []error {
	referenced := make(map[string]bool)
	for from, to := range b.edges {
		referenced[from] = true
		referenced[to] = true
	}
	for from := range b.choices {
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
	for from, router := range b.choices {
		if router == nil {
			errs = append(errs, fmt.Errorf("flowy: choice from %q has nil router", from))
		}
	}
	return errs
}

func (b *GraphBuilder[T]) validateRoutingConflicts() []error {
	var errs []error
	for from := range b.edges {
		if _, hasChoice := b.choices[from]; hasChoice {
			errs = append(errs, fmt.Errorf("flowy: node %q has both edge and choice", from))
		}
	}
	return errs
}

func (b *GraphBuilder[T]) validateFinishPoints() []error {
	var errs []error
	for name := range b.finishPoints {
		if _, ok := b.nodes[name]; !ok {
			errs = append(errs, fmt.Errorf("flowy: finish point %q not registered", name))
		}
	}
	return errs
}

func (b *GraphBuilder[T]) buildGraph(o buildOpts) *Graph[T] {
	return &Graph[T]{
		nodes:        copyNodeDefs(b.nodes, b.globalMiddlewares),
		edges:        maps.Clone(b.edges),
		choices:      maps.Clone(b.choices),
		entryPoint:   b.entryPoint,
		finishPoints: maps.Clone(b.finishPoints),
		reducer:      b.reducer,
		defaults:     o.run,
	}
}

func copyNodeDefs[T any](m map[string]nodeDef[T], globals []Middleware[T]) map[string]nodeDef[T] {
	out := make(map[string]nodeDef[T], len(m))
	for k, v := range m {
		nodeMws := append([]Middleware[T](nil), v.middlewares...)
		compiled := append(append([]Middleware[T]{}, globals...), nodeMws...)
		out[k] = nodeDef[T]{
			handler:             v.handler,
			middlewares:         nodeMws,
			compiledMiddlewares: compiled,
		}
	}
	return out
}
