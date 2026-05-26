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

### Typed state for late prompt rendering

When a graph drives an LLM pipeline, the canonical `flowy` contract is to carry **typed render input** through the graph instead of pre-rendered prompt messages. Use [PromptRenderContext] in state and render prompt messages only in the final LLM node.

```go
type AgentRunState[T any] struct {
	RenderContext flowy.PromptRenderContext[T]
	Tools         []Tool
	History       []Message
}
```

Here `History` means conversation turns that already happened, not a transport for the current system prompt. The prompt-system slice should be rendered on demand in the final LLM node and should not live in graph state as a parallel or fallback path.

Intermediate nodes and middleware can mutate `Tools` or other state freely. The final LLM node should then:

1. derive `allowedTools` from the current `state.Tools`,
2. inject them into `state.RenderContext.Input`,
3. render prompt messages once,
4. call the LLM client with the rendered messages plus the filtered tool list.

This keeps `flowy` focused on typed state transitions and avoids string sanitizers, regex patching, or map-based back-conversion in the graph core. `flowy` does not document or preserve a dual-path with early-bound prompt messages as an alternative API, and rendered prompt artifacts should not be carried through state between nodes.

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

| Option                  | Description                                                                                                  |
| ----------------------- | ------------------------------------------------------------------------------------------------------------ |
| `WithMaxSteps(n)`       | Maximum number of steps before returning `ErrMaxStepsExceeded`                                               |
| `WithMaxConcurrency(n)` | Maximum concurrent branch goroutines inside **`Parallel`** / **`ParallelDynamic`**; `n <= 0` means unlimited |

`flowy` does **not** attach per-node deadlines: nodes receive the same `context.Context` you pass to `Invoke` / `Stream`. Use cancellation and timeouts at the call site or inside the node (see below).

Example:

```go
graph, err := b.Compile(
	flowy.WithMaxSteps(50),
	flowy.WithMaxConcurrency(4),
)
```

## Timeouts and resilience (caller-controlled)

**Macro (whole run):** wrap the graph entry with a deadline on the context you pass to `Invoke` or `Stream`:

```go
ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
defer cancel()
final, err := graph.Invoke(ctx, initialState)
```

**Micro (single node):** derive a shorter-lived context inside the node when calling flaky I/O; the graph engine stays unaware:

```go
b.AddNode("fetch", func(ctx context.Context, state MyState) (MyState, error) {
	nodeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	return callUpstream(nodeCtx, state)
})
```

For retries, circuit breakers, or richer policies, compose ordinary Go functions or an external executor package in user code; `flowy` does not import such libraries.

### Optional: executor package (e.g. [routery](https://github.com/skosovsky/routery))

`flowy` stays free of that dependency; install it only in **your** module:

```bash
go get github.com/skosovsky/routery
```

**Micro-resiliency (harden one node):** adapt the node body to an executor contract, then wrap with policies before returning to the graph:

```go
import (
	"context"
	"time"

	"github.com/skosovsky/routery"
)

// inside AddNode("fetch_data", ...)
exec := func(innerCtx context.Context) (MyState, error) {
	return riskyNetworkCall(innerCtx, state)
}
safeExec := routery.Timeout(
	routery.Retry(exec, routery.WithMaxAttempts(3)),
	5*time.Second,
)
return safeExec(ctx)
```

**Macro-resiliency (budget for the whole run):** wrap `Invoke` (or `Stream` driver) as a single callable:

```go
graph, _ := builder.Compile()

graphExec := func(ctx context.Context) (MyState, error) {
	return graph.Invoke(ctx, initialState)
}
safeGraph := routery.Timeout(graphExec, 1*time.Minute)

finalState, err := safeGraph(context.Background())
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

- [examples/README.md](./examples/README.md) — index and resilience pointers
- [examples/context_deadline/main.go](./examples/context_deadline/main.go) — caller `WithTimeout` around `Invoke` (stdlib)
- [examples/late_prompt_agent/main.go](./examples/late_prompt_agent/main.go) — typed state + late prompt rendering in the final LLM node
- [examples/hitl_agent/main.go](./examples/hitl_agent/main.go) — checkpoint-backed Human-in-the-Loop resume
- [examples/middleware_agent/main.go](./examples/middleware_agent/main.go) — logging, memory, and fallback middleware
- [examples/react_agent/main.go](./examples/react_agent/main.go) — small ReAct loop
- [examples/semantic_cache_agent/main.go](./examples/semantic_cache_agent/main.go) — semantic-cache short-circuiting
