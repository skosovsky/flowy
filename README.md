# Flowy

[![Go Reference](https://pkg.go.dev/badge/github.com/skosovsky/flowy.svg)](https://pkg.go.dev/github.com/skosovsky/flowy)

**TL;DR** — flowy is a library for building reliable, stateful AI agents and workflows in Go as directed graphs. It supports multi-agent flows, progress persistence (checkpoints), Human-in-the-Loop (HITL), and Mermaid diagram export for visualization and debugging.

## Features

- **Generics** — strictly typed state `T`, no `interface{}` or `map[string]any`
- **Conditional edges** — routing based on state (e.g. LLM decides next step)
- **Fan-out / fan-in** — parallel execution with reducer-based merge (static `AddFanOut` or dynamic `AddDynamicFanOut` at runtime)
- **Middlewares** — wrap nodes for logging, tracing, metrics without touching business logic
- **Checkpointing** — persist and resume execution (HITL, long sessions)
- **Streaming** — observe execution via events (node start/end, interrupt, error)
- **Composition** — use a graph as a node (`AsNode()`) or call another graph’s `Invoke` from a node to adapt state

## Requirements

- Go 1.26+

## Installation

```bash
go get github.com/skosovsky/flowy
```

## Quick start

Minimal linear graph: state is a string, two nodes append to it, then run.

```go
package main

import (
    "context"
    "github.com/skosovsky/flowy"
)

func main() {
    reducer := func(_, update string) string { return update }
    b := flowy.NewGraph[string](reducer)
    b.AddNode("a", func(ctx context.Context, s string) (string, error) { return s + "a", nil })
    b.AddNode("b", func(ctx context.Context, s string) (string, error) { return s + "b", nil })
    b.AddEdge("a", "b")
    b.SetEntryPoint("a")
    b.SetFinishPoint("b")

    graph, err := b.Compile()
    if err != nil {
        panic(err)
    }
    ctx := context.Background()
    out, err := graph.Invoke(ctx, "")
    if err != nil {
        panic(err)
    }
    // out == "ab"
}
```

## State Management Patterns

The `Reducer` in flowy has a simple signature: `func(current, update T) T`. How you implement it depends on the complexity of your state.

### 1. Simple State (Full Replace)

If your state is a simple primitive (like a `string` or `int`), your nodes can just return the new absolute value, and your reducer simply returns the update:

```go
func reducer(current, update string) string { return update }
```

### 2. Complex State (Delta Updates / Merge) — Recommended

For real-world agents, your state will likely be a complex struct containing chat history, token counters, and pending tool calls. **Do not return a full copy of the state from your nodes.** This often leads to bugs where one node accidentally overwrites another's data.

Instead, nodes should return **only the fields that changed (a delta)**. The reducer is then responsible for safely merging these changes into the current state.

**Example of a Merge Reducer:**

```go
type Message struct{ Text string }
type ToolCall struct{ Name string }

type AgentState struct {
    Messages    []Message
    ToolCalls   []ToolCall
    TotalTokens int
}

func mergeReducer(current, update AgentState) AgentState {
    // 1. Append slices instead of replacing
    if len(update.Messages) > 0 {
        current.Messages = append(current.Messages, update.Messages...)
    }

    // 2. Replace slices only if explicitly needed (e.g., clearing queue)
    if update.ToolCalls != nil {
        current.ToolCalls = update.ToolCalls
    }

    // 3. Sum counters
    if update.TotalTokens > 0 {
        current.TotalTokens += update.TotalTokens
    }

    return current
}
```

In this pattern, an LLM node that only generates a new message just returns `AgentState{Messages: []Message{newMsg}}`, and the reducer safely appends it without clearing the `TotalTokens` counter.

## Key concepts

### State

State has type `T` and is passed between nodes. Each node returns a **delta** (update); the **reducer** merges current state with that delta to produce the next state. Choose full replace for simple types or merge/delta for complex state (see [State Management Patterns](#state-management-patterns)); see also [Advanced State Management](#advanced-state-management-mutation-slice-pattern) for a mutator pattern.

### Nodes

A node is a function `func(ctx context.Context, state T) (T, error)`: it receives context and current state, and returns the **delta** and an error. The runner applies the reducer to merge the delta into state and passes the result along the graph. On error, execution stops and the error is returned (or sent as `EventError` when using `Stream`).

### Edges and conditional edges

- **Edges** (`AddEdge(from, to)`) define a fixed next node.
- **Conditional edges** (`AddConditionalEdge(from, router)`) let a router function decide the next node from `(ctx, state)`; the router returns the next node name.

### Checkpointers (memory / HITL)

A **checkpointer** (interface: `Save(ctx, threadID, checkpoint)` and `Load(ctx, threadID)`) stores where execution stopped (state + node name). Use `WithCheckpointer(cp)` and `WithThreadID(id)` at `Compile` time. Mark interrupt points with `InterruptBefore(node)` or `InterruptAfter(node)`. When execution hits one, the runner saves a checkpoint, returns `ErrInterrupt`, and you continue later with `Resume(ctx, threadID, delta)` — the delta is merged with the saved state via the reducer.

## Run options

Options can be set at `Compile(opts...)` (defaults for all runs) or overridden per `Invoke` / `Stream` / `Resume`:

| Option | Description |
|--------|-------------|
| `WithMaxSteps(n)` | Max steps per run (prevents infinite loops; default 25). Returns `ErrMaxStepsExceeded` when exceeded. |
| `WithNodeTimeout(d)` | Timeout for each node execution; context is cancelled after `d`. |
| `WithCheckpointer(cp)` | Required for HITL; provides Save/Load by threadID. |
| `WithThreadID(id)` | Thread/session ID for checkpointing. |
| `WithMaxConcurrency(n)` | Max concurrent goroutines in fan-out; `n <= 0` means no limit. |

## Visualization (Mermaid)

You can export the compiled graph to Mermaid flowchart syntax for diagrams and debugging:

```go
graph, _ := b.Compile()
mermaid := graph.ExportMermaid()
fmt.Println(mermaid) // flowchart TD\n  a --> b ...
```

Use this to log or inspect the graph structure before running it.

## Errors and interrupts

- **Panics** — not recovered by the runner; a panic in a node will terminate execution.
- **ErrInterrupt** — returned when execution is suspended at an interrupt point (HITL). State is saved in the checkpointer; continue with `Resume(ctx, threadID, delta)`.
- **ErrMaxStepsExceeded** — returned when the step limit (`WithMaxSteps`) is reached (e.g. infinite loop in the graph).
- **ErrThreadNotFound** — returned by `Resume` when the given `threadID` has no saved checkpoint in the checkpointer.

## Streaming events

`Stream(ctx, state, opts...)` returns `<-chan Event[T]`. Event types:

- `EventNodeStart` — before a node runs
- `EventNodeEnd` — after a node completes successfully
- `EventInterrupt` — execution suspended (HITL)
- `EventError` — a node returned an error (or another run error)

The channel is closed when execution finishes (success, error, or interrupt). Drain the channel to avoid leaking the sender goroutine.

## Middlewares

Use `Use(mw...)` to wrap every node (including fan-out targets) with cross-cutting logic. The first middleware added runs first (outermost in the chain).

```go
b := flowy.NewGraph[string](reducer)
b.AddNode("a", nodeA)
b.Use(func(name string, next flowy.Node[string]) flowy.Node[string] {
    return func(ctx context.Context, s string) (string, error) {
        log.Println("before", name)
        out, err := next(ctx, s)
        log.Println("after", name)
        return out, err
    }
})
```

### Invocation middlewares

Global middlewares wrap the **entire** graph execution (each `Invoke`, `Resume`, or `Stream` call), not individual nodes. Use them for:

- A single root span for the whole run (e.g. OpenTelemetry)
- Recovery from panics (convert panic to error)
- Logging or metrics for system errors (`ErrMaxStepsExceeded`, checkpointer errors)
- Propagating trace IDs or other metadata for the full invocation

Register with `UseInvocation(mw...)`. The handler signature is `func(next InvocationHandler[T]) InvocationHandler[T]`; the first middleware added runs first (outermost in the chain). **Using invocation middlewares is optional:** if you never call `UseInvocation`, behavior and performance are unchanged.

```go
b.UseInvocation(func(next flowy.InvocationHandler[string]) flowy.InvocationHandler[string] {
    return func(ctx context.Context, state string, opts ...flowy.Option[string]) (string, error) {
        // e.g. start root span, defer span.End()
        return next(ctx, state, opts...)
    }
})
```

## Fan-out (static and dynamic)

**Static fan-out:** `AddFanOut(from, targets, joinNode)` runs all nodes in `targets` in parallel, merges their results with the reducer in order, then continues at `joinNode`. `joinNode` must be a registered node (not a fan-out source).

**Dynamic fan-out:** when the set of branches is known only at runtime (e.g. from an LLM), use `AddDynamicFanOut(from, router, joinNode)`. The router receives `(ctx, state)` and returns target node names. If it returns an empty list, execution goes straight to `joinNode`.

**Note:** `InterruptBefore` and `InterruptAfter` are not supported on fan-out target nodes. For static fan-out, `Compile()` fails if a target has them; for dynamic fan-out, `Invoke` returns an error at runtime if the router returns such a node.

**Limit concurrency:** to avoid rate limits (e.g. HTTP 429) or resource exhaustion when running many branches, use `WithMaxConcurrency(n)` (e.g. 5). It applies to `Invoke`, `Stream`, and `Resume` whenever a fan-out runs.

```go
out, err := graph.Invoke(ctx, state, flowy.WithMaxConcurrency(5))
for e := range graph.Stream(ctx, state, flowy.WithMaxConcurrency(5)) { /* ... */ }
out, err = graph.Resume(ctx, threadID, delta, flowy.WithMaxConcurrency(5))
```

## Advanced State Management (Mutation Slice Pattern)

To avoid a single giant reducer, you can keep state and apply small mutators returned by nodes:

```go
type State struct {
    Messages []string
    Query    string
}
type StateUpdate func(*State)

reducer := func(c State, update StateUpdate) State {
    if update != nil {
        update(&c)
    }
    return c
}
b.AddNode("append", func(ctx context.Context, s State) (StateUpdate, error) {
    return func(st *State) { st.Messages = append(st.Messages, s.Query) }, nil
})
```

## Development

```bash
make test    # run tests with race detector
make lint    # golangci-lint
make cover   # coverage report
```

## License

See [LICENSE](LICENSE).
