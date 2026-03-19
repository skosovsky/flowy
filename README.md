# Flowy

[![Go Reference](https://pkg.go.dev/badge/github.com/skosovsky/flowy.svg)](https://pkg.go.dev/github.com/skosovsky/flowy)

`flowy` is a Go library for building reliable, stateful AI agents and workflows as directed graphs. It gives you typed state, conditional routing, fan-out/fan-in, middleware-based cross-cutting concerns, iterator streaming, and graph composition.

## Features

- Strictly typed state with generics
- Conditional edges for state-driven routing
- Static and dynamic fan-out with reducer-based merge
- Global and per-node middlewares
- Suspend/resume with `ErrSuspend` and `Resume(ctx, state, startNode)`
- Step streaming via `iter.Seq2`
- Mermaid export for graph visualization
- Graph composition with `AsNode()` and `SubgraphNode(...)`

## Requirements

- Go 1.26+

## Installation

```bash
go get github.com/skosovsky/flowy
```

## Quick Start

```go
package main

import (
	"context"
	"fmt"
	"log"

	"github.com/skosovsky/flowy"
)

func main() {
	reducer := func(_, update string) string { return update }

	b := flowy.NewGraph[string](reducer)
	b.AddNode("a", func(_ context.Context, s string) (string, error) { return s + "a", nil })
	b.AddNode("b", func(_ context.Context, s string) (string, error) { return s + "b", nil })
	b.AddEdge("a", "b")
	b.SetEntryPoint("a")
	b.SetFinishPoint("b")

	graph, err := b.Compile()
	if err != nil {
		log.Fatal(err)
	}

	out, err := graph.Invoke(context.Background(), "")
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println(out)
}
```

## Key Concepts

### State and Reducer

Each node returns an update of type `T`, and the reducer merges that update into the current state:

```go
type Reducer[T any] func(current T, update T) T
```

For simple state like `string`, the reducer often just returns `update`. For complex state, prefer delta-style updates where nodes return only changed fields and the reducer merges them.

### Nodes

A node is the basic executable unit:

```go
type Node[T any] func(ctx context.Context, state T) (T, error)
```

The runner calls the node, merges the returned update with the reducer, then routes to the next step.

### Edges and Routing

- `AddEdge(from, to)` defines a fixed next step
- `AddConditionalEdge(from, router)` chooses the next step from `(ctx, state)`
- `AddFanOut(from, targets, joinNode)` runs multiple targets in parallel, merges the results, then continues at `joinNode`
- `AddDynamicFanOut(from, router, joinNode)` chooses parallel targets at runtime

## Middlewares & Cross-cutting Concerns

Middleware is the official extension point for logging, tracing, metrics, RBAC, retries, fallbacks, memory, and persistence:

```go
type NodeHandler[T any] = Node[T]

type MiddlewareContext[T any] struct {
	NodeName      string
	SuspendTarget string
	ExecutionKind flowy.MiddlewareExecutionKind
	CanResolveNext bool
	IsFinish      bool
	ApplyUpdate   func(current T, update T) T
	ResolveNext   func(ctx context.Context, postState T) (string, error)
}

type Middleware[T any] func(ctx context.Context, state T, meta MiddlewareContext[T], next NodeHandler[T]) (T, error)
```

There are two levels:

- Global middleware via `Use(mws...)`
- Local middleware per node via `AddNode(name, node, mws...)`
- Middleware wraps executable nodes only. Routing labels used as fan-out or dynamic fan-out sources do not trigger middleware themselves, but their target nodes do.

Order is onion-style:

- global middleware wraps local middleware
- local middleware wraps the node
- the first middleware added runs first on the way in and last on the way out

Example:

```go
logMw := func(ctx context.Context, state string, meta flowy.MiddlewareContext[string], next flowy.NodeHandler[string]) (string, error) {
	log.Println("before", meta.NodeName)
	out, err := next(ctx, state)
	log.Println("after", meta.NodeName)
	return out, err
}

fallbackNode := func(_ context.Context, _ string) (string, error) {
	return "[fallback]", nil
}

fallbackMw := func(ctx context.Context, state string, meta flowy.MiddlewareContext[string], next flowy.NodeHandler[string]) (string, error) {
	out, err := next(ctx, state)
	if err == nil {
		return out, nil
	}

	// Important: use the original state, not the failed node output.
	return fallbackNode(ctx, state)
}

b.Use(logMw)
b.AddNode("unstable", unstableNode, fallbackMw)
```

## State Persistence & Checkpoints

`flowy` does not include a built-in database, checkpoint store, thread registry, or checkpointer abstraction. If you need persistence between steps, implement it in middleware.

`MiddlewareContext` exists so middleware can work at the graph-step level instead of only looking at raw node output:

- `ApplyUpdate(current, update)` computes the merged post-step state
- `ResolveNext(ctx, postState)` resolves the next node for sequential executable steps
- `SuspendTarget` tells pause/resume middleware which resumable node or routing label to persist
- `ExecutionKind` and `CanResolveNext` tell you whether the current invocation is a normal node step or a fan-out branch

For fan-out branches:

- `ExecutionKind == flowy.MiddlewareExecutionFanOutBranch`
- `CanResolveNext == false`
- `ResolveNext(...)` is unsupported by contract
- `ErrSuspend` is not supported; pause before the fan-out source or after the join node

Typical HITL/persistence flow:

1. Middleware sees a node that should pause, for example `approve`
2. Middleware stores `(state, meta.SuspendTarget)` in your own storage
3. Middleware returns `ErrSuspend`
4. External code later loads `(state, startNode)` and calls `Resume(ctx, state, startNode)`

For post-step memory or checkpoints under merge reducers, persist the merged state rather than the raw node output. This recipe is for sequential executable nodes only:

```go
memoryMw := func(ctx context.Context, state string, meta flowy.MiddlewareContext[string], next flowy.NodeHandler[string]) (string, error) {
	out, err := next(ctx, state)
	if err != nil {
		return out, err
	}

	if !meta.CanResolveNext {
		// Fan-out branch: no generic checkpoint target is available here.
		return out, nil
	}

	postState := meta.ApplyUpdate(state, out)
	nextNode, err := meta.ResolveNext(ctx, postState)
	if err != nil {
		return out, err
	}

	_ = saveCheckpoint(postState, nextNode)
	return out, nil
}
```

Example:

```go
type Store[T any] interface {
	Save(ctx context.Context, key string, state T, startNode string) error
	Load(ctx context.Context, key string) (state T, startNode string, ok bool)
}

persistMw := func(store Store[string]) flowy.Middleware[string] {
	return func(ctx context.Context, state string, meta flowy.MiddlewareContext[string], next flowy.NodeHandler[string]) (string, error) {
		if meta.NodeName == "approve" {
			if err := store.Save(ctx, "session-1", state, meta.SuspendTarget); err != nil {
				return state, err
			}
			return state, flowy.ErrSuspend
		}
		return next(ctx, state)
	}
}

state, err := graph.Invoke(ctx, initial)
if errors.Is(err, flowy.ErrSuspend) {
	loaded, startNode, ok := store.Load(ctx, "session-1")
	if ok {
		state, err = graph.Resume(ctx, loaded, startNode)
	}
}
```

This same pattern is how you implement memory/checkpointing, human approval pauses, and other pause/resume workflows.

For production persistence, key saved state by thread/session identity plus the resume target. Using only `nodeName` is fine for a toy example, but not for a real multi-run store.

## Invoke, Resume, and Streaming

Use `Invoke` to run from the graph entry point:

```go
finalState, err := graph.Invoke(ctx, initialState)
```

Use `Resume` to continue from a specific node or routing label:

```go
finalState, err := graph.Resume(ctx, savedState, "approve")
```

`Resume` accepts:

- a registered executable node
- a fan-out source label
- a dynamic fan-out source label

Streaming is available through iterators:

```go
for step, err := range graph.Stream(ctx, state) {
	if err != nil {
		if errors.Is(err, flowy.ErrSuspend) {
			fmt.Println("resume target:", step.NodeName)
		}
		return
	}
	fmt.Println(step.NodeName, step.State)
}
```

`ResumeStream(ctx, state, startNode)` provides the same iterator contract starting from an arbitrary node.

## Build Options

Compile-time run defaults:

| Option | Description |
| --- | --- |
| `WithMaxSteps(n)` | Maximum number of steps before returning `ErrMaxStepsExceeded` |
| `WithNodeTimeout(d)` | Per-node timeout via derived context |
| `WithMaxConcurrency(n)` | Maximum goroutines used during fan-out; `n <= 0` means unlimited |

Example:

```go
graph, err := b.Compile(
	flowy.WithMaxSteps(50),
	flowy.WithMaxConcurrency(4),
)
```

## Fan-out Notes

- Fan-out target updates are merged in target order
- `joinNode` must be a registered executable node
- global middlewares also wrap fan-out targets
- `Resume` can start from a fan-out or dynamic fan-out routing label

## State Management Patterns

For non-trivial state, prefer delta updates over full replacement:

```go
type AgentState struct {
	Messages []string
	Query    string
	Tokens   int
}

func mergeReducer(current, update AgentState) AgentState {
	if len(update.Messages) > 0 {
		current.Messages = append(current.Messages, update.Messages...)
	}
	if update.Query != "" {
		current.Query = update.Query
	}
	if update.Tokens > 0 {
		current.Tokens += update.Tokens
	}
	return current
}
```

This keeps state immutable at the engine boundary while still giving you efficient updates.

## Mermaid Export

```go
graph, _ := b.Compile()
fmt.Println(graph.ExportMermaid())
```

Use Mermaid export for debugging and documentation.

## Examples

- [examples/hitl_agent/main.go](./examples/hitl_agent/main.go) shows middleware-based pause/resume
- [examples/middleware_agent/main.go](./examples/middleware_agent/main.go) shows logging, memory, and fallback middleware
- [examples/react_agent/main.go](./examples/react_agent/main.go) shows a small ReAct loop
- [examples/semantic_cache_agent/main.go](./examples/semantic_cache_agent/main.go) shows semantic-cache short-circuiting
