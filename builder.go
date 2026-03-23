package flowy

import (
	"errors"
	"fmt"
	"maps"
	"sync"
)

// fanOutDef describes a fan-out: one source node, multiple parallel targets, one join node.
type fanOutDef struct {
	targets  []string
	joinNode string
}

// dynamicFanOutDef describes a dynamic fan-out: router returns target names at runtime.
type dynamicFanOutDef[T any] struct {
	router   DynamicRouter[T]
	joinNode string
}

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
	conditionalEdges  map[string]ConditionalEdge[T]
	fanOuts           map[string]*fanOutDef
	dynamicFanOuts    map[string]*dynamicFanOutDef[T]
	globalMiddlewares []Middleware[T]
	entryPoint        string
	finishPoints      map[string]bool
	// overwritten* track keys that were re-registered (overwrite); Compile reports them as errors.
	overwrittenNodes             map[string]bool
	overwrittenEdgeFrom          map[string]bool
	overwrittenConditionalFrom   map[string]bool
	overwrittenFanOutFrom        map[string]bool
	overwrittenDynamicFanOutFrom map[string]bool
}

// NewGraph creates a new graph builder with the given reducer.
func NewGraph[T any](reducer Reducer[T]) *GraphBuilder[T] {
	return &GraphBuilder[T]{
		reducer:                      reducer,
		nodes:                        make(map[string]nodeDef[T]),
		edges:                        make(map[string]string),
		conditionalEdges:             make(map[string]ConditionalEdge[T]),
		fanOuts:                      make(map[string]*fanOutDef),
		dynamicFanOuts:               make(map[string]*dynamicFanOutDef[T]),
		finishPoints:                 make(map[string]bool),
		overwrittenNodes:             make(map[string]bool),
		overwrittenEdgeFrom:          make(map[string]bool),
		overwrittenConditionalFrom:   make(map[string]bool),
		overwrittenFanOutFrom:        make(map[string]bool),
		overwrittenDynamicFanOutFrom: make(map[string]bool),
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

// AddConditionalEdge sets a router for the given node; the router returns the next node name.
func (b *GraphBuilder[T]) AddConditionalEdge(from string, router ConditionalEdge[T]) *GraphBuilder[T] {
	if _, exists := b.conditionalEdges[from]; exists {
		b.overwrittenConditionalFrom[from] = true
	}
	b.conditionalEdges[from] = router
	return b
}

// AddFanOut sets up parallel execution: when routing reaches 'from',
// all target nodes run in parallel, then results are reduced and joinNode executes.
// 'from' is a routing label, not an executable node.
// joinNode must be a registered node (AddNode); it cannot be a fan-out or dynamic fan-out source.
func (b *GraphBuilder[T]) AddFanOut(from string, targets []string, joinNode string) *GraphBuilder[T] {
	if _, exists := b.fanOuts[from]; exists {
		b.overwrittenFanOutFrom[from] = true
	}
	targetsCopy := make([]string, len(targets))
	copy(targetsCopy, targets)
	b.fanOuts[from] = &fanOutDef{targets: targetsCopy, joinNode: joinNode}
	return b
}

// Use adds middlewares that wrap every node at compile time (first added runs first).
func (b *GraphBuilder[T]) Use(mw ...Middleware[T]) *GraphBuilder[T] {
	b.globalMiddlewares = append(b.globalMiddlewares, mw...)
	return b
}

// AddDynamicFanOut sets up dynamic parallel execution: when routing reaches 'from',
// the router is called to get target node names at runtime; those nodes run in parallel,
// then results are reduced and joinNode runs. joinNode must be a registered node (AddNode).
// 'from' is a routing label, not an executable node.
func (b *GraphBuilder[T]) AddDynamicFanOut(from string, router DynamicRouter[T], joinNode string) *GraphBuilder[T] {
	if _, exists := b.dynamicFanOuts[from]; exists {
		b.overwrittenDynamicFanOutFrom[from] = true
	}
	b.dynamicFanOuts[from] = &dynamicFanOutDef[T]{router: router, joinNode: joinNode}
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
	errs = append(errs, b.validateFanOuts()...)
	errs = append(errs, b.validateDynamicFanOuts()...)
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
	for from := range b.overwrittenConditionalFrom {
		errs = append(errs, fmt.Errorf("flowy: conditional edge from %q registered more than once", from))
	}
	for from := range b.overwrittenFanOutFrom {
		errs = append(errs, fmt.Errorf("flowy: fan-out from %q registered more than once", from))
	}
	for from := range b.overwrittenDynamicFanOutFrom {
		errs = append(errs, fmt.Errorf("flowy: dynamic fan-out from %q registered more than once", from))
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
	} else if !b.hasNodeOrRouting(b.entryPoint) {
		errs = append(
			errs,
			fmt.Errorf("flowy: entry point %q is not a registered node or fan-out source", b.entryPoint),
		)
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
	for from := range b.conditionalEdges {
		referenced[from] = true
	}
	for _, fo := range b.fanOuts {
		referenced[fo.joinNode] = true
		for _, target := range fo.targets {
			referenced[target] = true
		}
	}
	for from, dfo := range b.dynamicFanOuts {
		referenced[from] = true
		referenced[dfo.joinNode] = true
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
		if !b.hasNodeOrRouting(name) {
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
		if _, hasCond := b.conditionalEdges[from]; hasCond {
			errs = append(errs, fmt.Errorf("flowy: node %q has both edge and conditional edge", from))
		}
		if _, hasFan := b.fanOuts[from]; hasFan {
			errs = append(errs, fmt.Errorf("flowy: node %q has both edge and fan-out", from))
		}
		if _, hasDyn := b.dynamicFanOuts[from]; hasDyn {
			errs = append(errs, fmt.Errorf("flowy: node %q has both edge and dynamic fan-out", from))
		}
	}
	for from := range b.conditionalEdges {
		if _, hasFan := b.fanOuts[from]; hasFan {
			errs = append(errs, fmt.Errorf("flowy: node %q has both conditional edge and fan-out", from))
		}
		if _, hasDyn := b.dynamicFanOuts[from]; hasDyn {
			errs = append(errs, fmt.Errorf("flowy: node %q has both conditional edge and dynamic fan-out", from))
		}
	}
	for from := range b.fanOuts {
		if _, hasDyn := b.dynamicFanOuts[from]; hasDyn {
			errs = append(errs, fmt.Errorf("flowy: node %q has both fan-out and dynamic fan-out", from))
		}
	}
	for from := range b.dynamicFanOuts {
		if _, hasNode := b.nodes[from]; hasNode {
			errs = append(errs, fmt.Errorf("flowy: name %q used as both node and dynamic fan-out source", from))
		}
	}
	return errs
}

func (b *GraphBuilder[T]) validateFanOuts() []error {
	var errs []error
	for name, fo := range b.fanOuts {
		if len(fo.targets) == 0 {
			errs = append(errs, fmt.Errorf("flowy: fan-out %q has no targets", name))
		}
		for _, target := range fo.targets {
			if _, ok := b.nodes[target]; !ok {
				errs = append(errs, fmt.Errorf("flowy: fan-out %q target %q is not a registered node", name, target))
			}
		}
		if _, ok := b.nodes[fo.joinNode]; !ok {
			errs = append(errs, fmt.Errorf("flowy: fan-out %q joinNode %q is not a registered node", name, fo.joinNode))
		} else if _, isFanOut := b.fanOuts[fo.joinNode]; isFanOut {
			errs = append(
				errs,
				fmt.Errorf("flowy: fan-out %q joinNode %q cannot be a fan-out source", name, fo.joinNode),
			)
		} else if _, isDyn := b.dynamicFanOuts[fo.joinNode]; isDyn {
			errs = append(
				errs,
				fmt.Errorf("flowy: fan-out %q joinNode %q cannot be a dynamic fan-out source", name, fo.joinNode),
			)
		}
		if _, hasNode := b.nodes[name]; hasNode {
			errs = append(errs, fmt.Errorf("flowy: name %q used as both node and fan-out source", name))
		}
	}
	return errs
}

func (b *GraphBuilder[T]) validateDynamicFanOuts() []error {
	var errs []error
	for name, dfo := range b.dynamicFanOuts {
		if dfo.router == nil {
			errs = append(errs, fmt.Errorf("flowy: dynamic fan-out %q has nil router", name))
		}
		if _, ok := b.nodes[dfo.joinNode]; !ok {
			errs = append(
				errs,
				fmt.Errorf("flowy: dynamic fan-out %q joinNode %q is not a registered node", name, dfo.joinNode),
			)
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

func (b *GraphBuilder[T]) hasNodeOrRouting(name string) bool {
	if _, ok := b.nodes[name]; ok {
		return true
	}
	if _, ok := b.fanOuts[name]; ok {
		return true
	}
	if _, ok := b.dynamicFanOuts[name]; ok {
		return true
	}
	return false
}

func (b *GraphBuilder[T]) buildGraph(o buildOpts) *Graph[T] {
	return &Graph[T]{
		nodes:            copyNodeDefs(b.nodes, b.globalMiddlewares),
		edges:            maps.Clone(b.edges),
		conditionalEdges: maps.Clone(b.conditionalEdges),
		fanOuts:          copyFanOuts(b.fanOuts),
		dynamicFanOuts:   copyDynamicFanOuts(b.dynamicFanOuts),
		entryPoint:       b.entryPoint,
		finishPoints:     maps.Clone(b.finishPoints),
		reducer:          b.reducer,
		defaults:         o.run,
		executionChainPool: sync.Pool{
			New: func() any {
				return new(ExecutionChain[T])
			},
		},
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

func copyFanOuts(m map[string]*fanOutDef) map[string]*fanOutDef {
	out := make(map[string]*fanOutDef, len(m))
	for k, v := range m {
		targets := make([]string, len(v.targets))
		copy(targets, v.targets)
		out[k] = &fanOutDef{targets: targets, joinNode: v.joinNode}
	}
	return out
}

func copyDynamicFanOuts[T any](m map[string]*dynamicFanOutDef[T]) map[string]*dynamicFanOutDef[T] {
	out := make(map[string]*dynamicFanOutDef[T], len(m))
	for k, v := range m {
		out[k] = &dynamicFanOutDef[T]{router: v.router, joinNode: v.joinNode}
	}
	return out
}
