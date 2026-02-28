package flowy

import (
	"errors"
	"fmt"
	"maps"
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

// GraphBuilder builds a graph before compilation. Fluent API.
type GraphBuilder[T any] struct {
	reducer          Reducer[T]
	nodes            map[string]Node[T]
	edges            map[string]string
	conditionalEdges map[string]ConditionalEdge[T]
	fanOuts          map[string]*fanOutDef
	dynamicFanOuts   map[string]*dynamicFanOutDef[T]
	middlewares      []Middleware[T]
	entryPoint       string
	finishPoints     map[string]bool
	interruptBefore  map[string]bool
	interruptAfter   map[string]bool
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
		nodes:                        make(map[string]Node[T]),
		edges:                        make(map[string]string),
		conditionalEdges:             make(map[string]ConditionalEdge[T]),
		fanOuts:                      make(map[string]*fanOutDef),
		dynamicFanOuts:               make(map[string]*dynamicFanOutDef[T]),
		finishPoints:                 make(map[string]bool),
		interruptBefore:              make(map[string]bool),
		interruptAfter:               make(map[string]bool),
		overwrittenNodes:             make(map[string]bool),
		overwrittenEdgeFrom:          make(map[string]bool),
		overwrittenConditionalFrom:   make(map[string]bool),
		overwrittenFanOutFrom:        make(map[string]bool),
		overwrittenDynamicFanOutFrom: make(map[string]bool),
	}
}

// AddNode registers a node by name. Returns the builder for chaining.
func (b *GraphBuilder[T]) AddNode(name string, fn Node[T]) *GraphBuilder[T] {
	if _, exists := b.nodes[name]; exists {
		b.overwrittenNodes[name] = true
	}
	b.nodes[name] = fn
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
	b.middlewares = append(b.middlewares, mw...)
	return b
}

// AddDynamicFanOut sets up dynamic parallel execution: when routing reaches 'from',
// the router is called to get target node names at runtime; those nodes run in parallel,
// then results are reduced and joinNode runs. joinNode must be a registered node (AddNode).
// Nodes executed as dynamic targets do not support InterruptBefore/InterruptAfter.
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

// InterruptBefore marks a node to suspend execution before running it (HITL).
// Interrupt is only triggered when both Checkpointer and ThreadID are configured at run time.
// Without them, the interrupt point is silently skipped.
func (b *GraphBuilder[T]) InterruptBefore(name string) *GraphBuilder[T] {
	b.interruptBefore[name] = true
	return b
}

// InterruptAfter marks a node to suspend execution after running it (HITL).
// Interrupt is only triggered when both Checkpointer and ThreadID are configured at run time.
// Without them, the interrupt point is silently skipped.
func (b *GraphBuilder[T]) InterruptAfter(name string) *GraphBuilder[T] {
	b.interruptAfter[name] = true
	return b
}

// Compile validates the graph and returns an immutable Graph. Options set default run config.
func (b *GraphBuilder[T]) Compile(opts ...Option[T]) (*Graph[T], error) {
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

	if b.reducer == nil {
		errs = append(errs, errors.New("flowy: reducer must not be nil"))
	}
	if b.entryPoint == "" {
		errs = append(errs, errors.New("flowy: entry point not set"))
	} else if _, hasNode := b.nodes[b.entryPoint]; !hasNode {
		if _, hasFan := b.fanOuts[b.entryPoint]; !hasFan {
			if _, hasDyn := b.dynamicFanOuts[b.entryPoint]; !hasDyn {
				errs = append(errs, fmt.Errorf("flowy: entry point %q is not a registered node or fan-out source", b.entryPoint))
			}
		}
	}
	if len(b.finishPoints) == 0 {
		errs = append(errs, errors.New("flowy: no finish point set"))
	}

	// All nodes referenced in edges/conditionalEdges/fanOuts must exist
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
		for _, t := range fo.targets {
			referenced[t] = true
		}
	}
	for from, dfo := range b.dynamicFanOuts {
		referenced[from] = true
		referenced[dfo.joinNode] = true
	}
	for name := range b.nodes {
		referenced[name] = true
	}

	for name := range referenced {
		if name == "" {
			errs = append(errs, errors.New("flowy: node name must not be empty"))
			continue
		}
		if _, ok := b.nodes[name]; !ok {
			if _, isFanOut := b.fanOuts[name]; !isFanOut {
				if _, isDyn := b.dynamicFanOuts[name]; !isDyn {
					errs = append(errs, fmt.Errorf("flowy: node %q not registered", name))
				}
			}
		}
	}

	// All nodes must have a non-nil handler to avoid panic at runtime.
	for name, node := range b.nodes {
		if node == nil {
			errs = append(errs, fmt.Errorf("flowy: node %q has nil handler", name))
		}
	}

	// Conditional edge router must not be nil (would panic in resolveNext at runtime).
	for from, router := range b.conditionalEdges {
		if router == nil {
			errs = append(errs, fmt.Errorf("flowy: conditional edge from %q has nil router", from))
		}
	}

	// Conflict: same node cannot have both edge and conditionalEdge, or edge and fanOut, etc.
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

	for name, fo := range b.fanOuts {
		if len(fo.targets) == 0 {
			errs = append(errs, fmt.Errorf("flowy: fan-out %q has no targets", name))
		}
		for _, t := range fo.targets {
			if _, ok := b.nodes[t]; !ok {
				errs = append(errs, fmt.Errorf("flowy: fan-out %q target %q is not a registered node", name, t))
			}
		}
		// joinNode must be an executable node, not a routing label (fan-out/dynamic fan-out source).
		if _, ok := b.nodes[fo.joinNode]; !ok {
			errs = append(errs, fmt.Errorf("flowy: fan-out %q joinNode %q is not a registered node", name, fo.joinNode))
		} else if _, isFanOut := b.fanOuts[fo.joinNode]; isFanOut {
			errs = append(errs, fmt.Errorf("flowy: fan-out %q joinNode %q cannot be a fan-out source", name, fo.joinNode))
		} else if _, isDyn := b.dynamicFanOuts[fo.joinNode]; isDyn {
			errs = append(errs, fmt.Errorf("flowy: fan-out %q joinNode %q cannot be a dynamic fan-out source", name, fo.joinNode))
		}
		if _, hasNode := b.nodes[name]; hasNode {
			errs = append(errs, fmt.Errorf("flowy: name %q used as both node and fan-out source", name))
		}
	}

	// Dynamic fan-out: joinNode must exist, router must not be nil.
	for name, dfo := range b.dynamicFanOuts {
		if dfo.router == nil {
			errs = append(errs, fmt.Errorf("flowy: dynamic fan-out %q has nil router", name))
		}
		if _, ok := b.nodes[dfo.joinNode]; !ok {
			errs = append(errs, fmt.Errorf("flowy: dynamic fan-out %q joinNode %q is not a registered node", name, dfo.joinNode))
		}
	}

	// InterruptBefore/After must reference registered nodes and cannot be fan-out targets (runFanOut does not check interrupts).
	fanOutTargets := make(map[string]string) // target name -> fan-out source name
	for foName, fo := range b.fanOuts {
		for _, t := range fo.targets {
			fanOutTargets[t] = foName
		}
	}
	for name := range b.interruptBefore {
		if _, ok := b.nodes[name]; !ok {
			errs = append(errs, fmt.Errorf("flowy: interruptBefore node %q not registered", name))
		} else if foSource, isTarget := fanOutTargets[name]; isTarget {
			errs = append(errs, fmt.Errorf("flowy: interruptBefore on fan-out target %q (fan-out %q) is not supported", name, foSource))
		}
	}
	for name := range b.interruptAfter {
		if _, ok := b.nodes[name]; !ok {
			errs = append(errs, fmt.Errorf("flowy: interruptAfter node %q not registered", name))
		} else if foSource, isTarget := fanOutTargets[name]; isTarget {
			errs = append(errs, fmt.Errorf("flowy: interruptAfter on fan-out target %q (fan-out %q) is not supported", name, foSource))
		}
	}

	// Finish points must reference registered nodes
	for name := range b.finishPoints {
		if _, ok := b.nodes[name]; !ok {
			errs = append(errs, fmt.Errorf("flowy: finish point %q not registered", name))
		}
	}

	// Reachable set is reserved for a future optional warning about unreachable nodes.
	reachable := make(map[string]bool)
	reachable[b.entryPoint] = true
	for _, to := range b.edges {
		reachable[to] = true
	}
	for _, fo := range b.fanOuts {
		reachable[fo.joinNode] = true
		for _, t := range fo.targets {
			reachable[t] = true
		}
	}
	for _, dfo := range b.dynamicFanOuts {
		reachable[dfo.joinNode] = true
	}
	_ = reachable

	if len(errs) > 0 {
		return nil, errors.Join(errs...)
	}

	// Wrap all nodes in middlewares (first added runs first: iterate in reverse).
	compiledNodes := maps.Clone(b.nodes)
	for name, node := range compiledNodes {
		wrapped := node
		for i := len(b.middlewares) - 1; i >= 0; i-- {
			wrapped = b.middlewares[i](name, wrapped)
		}
		compiledNodes[name] = wrapped
	}

	cfg := applyOptions[T](nil, opts)
	g := &Graph[T]{
		nodes:            compiledNodes,
		edges:            maps.Clone(b.edges),
		conditionalEdges: maps.Clone(b.conditionalEdges),
		fanOuts:          copyFanOuts(b.fanOuts),
		dynamicFanOuts:   copyDynamicFanOuts(b.dynamicFanOuts),
		entryPoint:       b.entryPoint,
		finishPoints:     maps.Clone(b.finishPoints),
		interruptBefore:  maps.Clone(b.interruptBefore),
		interruptAfter:   maps.Clone(b.interruptAfter),
		reducer:          b.reducer,
		defaults:         cfg,
	}
	return g, nil
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
