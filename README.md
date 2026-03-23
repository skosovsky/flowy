# Flowy

[![Go Reference](https://pkg.go.dev/badge/github.com/skosovsky/flowy.svg)](https://pkg.go.dev/github.com/skosovsky/flowy)

`flowy` is a Go library for building reliable, stateful AI agents and workflows as directed graphs. It gives you typed state, conditional routing, fan-out/fan-in, middleware-based cross-cutting concerns, iterator streaming, and graph composition.

## Features

- Strictly typed state with generics
- Conditional edges for state-driven routing
- Static and dynamic fan-out with reducer-based merge
- Global and per-node middlewares
- Suspend with `ErrSuspend`; resume by the caller using `Stream` + `checkpoint` storage
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

Middleware is the official extension point for logging, tracing, metrics, RBAC, retries, fallbacks, memory, and persistence. The pipeline uses an **execution chain**: call `chain.Next(ctx, state)` to run the next layer or the node.

```go
type Middleware[T any] func(ctx context.Context, state T, chain *ExecutionChain[T]) (T, error)

// ExecutionChain exposes NodeName, SuspendTarget, ExecutionKind, CanResolveNext, IsFinish,
// plus ApplyUpdate / ResolveNext methods, and Next to continue the pipeline.
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
logMw := func(ctx context.Context, state string, chain *flowy.ExecutionChain[string]) (string, error) {
	log.Println("before", chain.NodeName)
	out, err := chain.Next(ctx, state)
	log.Println("after", chain.NodeName)
	return out, err
}

fallbackNode := func(_ context.Context, _ string) (string, error) {
	return "[fallback]", nil
}

fallbackMw := func(ctx context.Context, state string, chain *flowy.ExecutionChain[string]) (string, error) {
	out, err := chain.Next(ctx, state)
	if err == nil {
		return out, nil
	}

	// Important: use the original state, not the failed node output.
	return fallbackNode(ctx, state)
}

b.Use(logMw)
b.AddNode("unstable", unstableNode, fallbackMw)
```

### Migrating to ExecutionChain middleware

Older sketches used a `next` callback to continue the pipeline. The compiled runner uses **`ExecutionChain`**: call **`chain.Next(ctx, state)`** and read step metadata from **`chain`** (`NodeName`, `SuspendTarget`, `ExecutionKind`, `CanResolveNext`, `IsFinish`, plus `ApplyUpdate` / `ResolveNext` when you need reducer or routing outside the default step).

**Before (conceptual — callback-style “next”):**

```go
// Not the current API — illustrative only.
type MiddlewareContext[T any] struct { /* … */ }

func logMw(ctx context.Context, state string, mw *MiddlewareContext[string]) (string, error) {
	log.Println("before", mw.NodeName)
	out, err := mw.Next(ctx, state) // hypothetical
	log.Println("after", mw.NodeName)
	return out, err
}
```

**After (current `flowy` API):**

```go
func logMw(ctx context.Context, state string, chain *flowy.ExecutionChain[string]) (string, error) {
	log.Println("before", chain.NodeName)
	out, err := chain.Next(ctx, state)
	log.Println("after", chain.NodeName)
	return out, err
}
```

There is no compatibility adapter in the library: migrate call sites to `*ExecutionChain` and `chain.Next` as in the examples above.

## State Persistence & Checkpoints

### Layout vs. original task spec

The micro-spec once referenced `adapters/checkpointer` as the home for DTOs and storage. The implementation uses a **first-class Go subpackage** `github.com/skosovsky/flowy/checkpoint` for persistence types (`Checkpoint`, serializers) and keeps optional **adapters** (e.g. filesystem or DB) beside or under `adapters/`. That keeps the **root `flowy` package** free of imports from persistence while still giving users a single `go get` and clear `import "…/checkpoint"`. Same goal as the spec: **core stays stateless**; all I/O lives outside `package flowy`. Formal decision: [ADR 0001](docs/adr/0001-persistence-package-layout.md).

The execution core is **stateless**. Types and storage live in `github.com/skosovsky/flowy/checkpoint` and adapters (`adapters/checkpointer/...`):

- `checkpoint.Checkpoint` — persisted DTO (`Node`, `Next`, `StateData`, …)
- `checkpoint.Checkpointer` — `Save` / `LoadLatest` / `GetHistory`
- `checkpoint.JSONSerializer[T]` and `checkpoint.EncodeStateData` / `DecodeStateData` for the JSON envelope

**Typical flow:** iterate `for step, err := range graph.Stream(ctx, startNode, state)`. Use `startNode == ""` to use the compiled entry point. After each successful step, persist `step.State`, `step.NodeName`, and `step.NextNode` (your `Checkpoint.Node` / `Next`). On `ErrSuspend`, `step` contains the snapshot and `NextNode` is the resume cursor; **you** call `Save`, then later `Stream(ctx, loaded.Next, decodedState)` to continue.

Runnable examples: [`ExampleGraph_statefulClientWithCheckpoint`](./persistence_examples_test.go), [`examples/hitl_agent/main.go`](./examples/hitl_agent/main.go).

`Checkpointer.Save` must be idempotent by `Checkpoint.ID`.

### Middleware Persistence

`ExecutionChain` exposes step-level metadata (`ApplyUpdate`, `ResolveNext`, `SuspendTarget`, fan-out branch rules). See [`ExampleExecutionChain_persistenceRecipe`](./persistence_examples_test.go).

## Invoke and Streaming

Use `Invoke` to run from the graph entry point:

```go
finalState, err := graph.Invoke(ctx, initialState)
```

Streaming:

```go
for step, err := range graph.Stream(ctx, "", initialState) {
	if err != nil {
		if errors.Is(err, flowy.ErrSuspend) {
			fmt.Println("suspending node:", step.NodeName, "resume:", step.NextNode)
		}
		return
	}
	fmt.Println(step.NodeName, step.NextNode, step.State)
}
```

On `ErrSuspend`, the suspending node or middleware must return the full snapshot state. Treat suspend as “return snapshot and stop”, not as “return delta and let the reducer finish the step”.

## Development

Hot-path benchmarks (expect `0 allocs/op` for both after pool warmup; benchmarks pre-warm the `ExecutionChain` pool before measuring):

```bash
make bench-hotpath
```

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
- client-driven resume can continue through fan-out and dynamic fan-out routing labels when `Checkpoint.Next` stores the routing label or join node

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

- [examples/hitl_agent/main.go](./examples/hitl_agent/main.go) shows checkpoint-backed Human-in-the-Loop resume
- [examples/middleware_agent/main.go](./examples/middleware_agent/main.go) shows logging, memory, and fallback middleware
- [examples/react_agent/main.go](./examples/react_agent/main.go) shows a small ReAct loop
- [examples/semantic_cache_agent/main.go](./examples/semantic_cache_agent/main.go) shows semantic-cache short-circuiting
