# Flowy

[![Go Reference](https://pkg.go.dev/badge/github.com/skosovsky/flowy.svg)](https://pkg.go.dev/github.com/skosovsky/flowy)

`flowy` is a Go library for building reliable, stateful AI agents and workflows as **directed graphs with linear topology** (static edges and optional **choices**). Concurrency lives **inside** a node via the **`Parallel`** helper, not as special graph edges. The library provides typed state, middleware, iterator streaming, **`ErrSuspend`** with caller-driven resume, and graph composition.

## Features

- Strictly typed state with generics
- `AddEdge` for fixed transitions and `AddChoice` for state-driven routing to a **single** next node
- **`Parallel` / `ParallelDynamic`** to run named targets concurrently inside one step, with an explicit **`merge`** function (fold order matches `targets` / router output order)
- Global and per-node middlewares
- Suspend with `ErrSuspend`; resume using `Stream` + `checkpoint` storage
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

### Edges, Choices, and Parallel

- `AddEdge(from, to)` defines a fixed next step.
- `AddChoice(from, router)` chooses the **one** next node from `(ctx, state)`.
- **`Parallel(graph, nodeName, merge, targets...)`** returns a `Node` that runs the listed targets concurrently, then folds branch results with **`merge`** in **target order** on the goroutine that owns the step (deterministic, race-free merge on `T`).
- **`ParallelDynamic`** selects targets at runtime via a `pick` function; merge order follows the returned slice order.

After a parallel step, route to a join or continuation with a normal edge:

```go
var g *flowy.Graph[string]
b.AddNode("work", flowy.Parallel(&g, "work", merge, "a", "b"))
b.AddEdge("work", "join")
g, err = b.Compile()
```

You must assign the compiled graph to the **same** `*Graph` variable whose address was passed into `Parallel` / `ParallelDynamic`.

## Middlewares

Middleware uses an **execution chain**: call `chain.Next(ctx, state)` to run the next layer or the node.

```go
type Middleware[T any] func(ctx context.Context, state T, chain *ExecutionChain[T]) (T, error)
```

`ExecutionChain` exposes `NodeName`, `SuspendTarget`, `IsFinish`, plus `ApplyUpdate` and `ResolveNext` when you need reducer or routing outside the default step.

- Global middleware via `Use(mws...)`
- Local middleware per node via `AddNode(name, node, mws...)`

Order is onion-style: global middleware wraps local middleware wraps the node; the first middleware added runs first on the way in and last on the way out.

Parallel branch targets are normal nodes: they go through the same middleware stack when executed.

## State Persistence & Checkpoints

The execution core is **stateless**. Types and storage live in `github.com/skosovsky/flowy/checkpoint`:

- `checkpoint.Checkpoint` — persisted DTO (`Node`, `Next`, `StateData`, …)
- `checkpoint.Checkpointer` — `Save` / `LoadLatest` / `GetHistory`
- `checkpoint.JSONSerializer[T]` and `checkpoint.EncodeStateData` / `DecodeStateData` for the JSON envelope

**Typical flow:** iterate `for step, err := range graph.Stream(ctx, startNode, state)`. Use `startNode == ""` for the compiled entry point. After each successful step, persist `step.State`, `step.NodeName`, and `step.NextNode`. On `ErrSuspend`, `step` contains the snapshot and `NextNode` is the resume cursor; call `Save`, then later `Stream(ctx, loaded.Next, decodedState)` to continue.

Runnable examples: [`ExampleGraph_statefulClientWithCheckpoint`](./persistence_examples_test.go), [`examples/hitl_agent/main.go`](./examples/hitl_agent/main.go).

`Checkpointer.Save` must be idempotent by `Checkpoint.ID`.

### Middleware and persistence

See [`ExampleExecutionChain_persistenceRecipe`](./persistence_examples_test.go) for applying `ApplyUpdate` / `ResolveNext` inside middleware for checkpoint-style saves.

## Invoke and Streaming

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

On `ErrSuspend`, the suspending node or middleware must return the full snapshot state.

## Development

Hot-path benchmarks:

```bash
make bench-hotpath
```

## Build Options

| Option | Description |
| --- | --- |
| `WithMaxSteps(n)` | Maximum number of steps before returning `ErrMaxStepsExceeded` |
| `WithNodeTimeout(d)` | Per-node timeout via derived context |
| `WithMaxConcurrency(n)` | Maximum concurrent branch goroutines inside **`Parallel`** / **`ParallelDynamic`**; `n <= 0` means unlimited |

Example:

```go
graph, err := b.Compile(
	flowy.WithMaxSteps(50),
	flowy.WithMaxConcurrency(4),
)
```

## Parallel merge order

Branch results are folded as `acc := state` then `acc = merge(acc, results[i])` for each target in order. Choose **`merge`** so this matches your intent (often the same as the graph reducer, but it is a separate parameter).

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

The diagram reflects **edges** and **choices** only; work done inside a `Parallel` node is not expanded as separate Mermaid branches.

## Examples

- [examples/hitl_agent/main.go](./examples/hitl_agent/main.go) — checkpoint-backed Human-in-the-Loop resume
- [examples/middleware_agent/main.go](./examples/middleware_agent/main.go) — logging, memory, and fallback middleware
- [examples/react_agent/main.go](./examples/react_agent/main.go) — small ReAct loop
- [examples/semantic_cache_agent/main.go](./examples/semantic_cache_agent/main.go) — semantic-cache short-circuiting
