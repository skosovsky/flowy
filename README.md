# Flowy

[![Go Reference](https://pkg.go.dev/badge/github.com/skosovsky/flowy.svg)](https://pkg.go.dev/github.com/skosovsky/flowy)

**TL;DR** — flowy is a library for building reliable, stateful AI agents and workflows in Go as directed graphs. It supports multi-agent flows, Suspend/Resume (HITL), state streaming via iterators, and Mermaid diagram export.

## Features

- **Generics** — strictly typed state `T`, no `interface{}` or `map[string]any`
- **Conditional edges** — routing based on state (e.g. LLM decides next step)
- **Fan-out / fan-in** — parallel execution with reducer-based merge (static `AddFanOut` or dynamic `AddDynamicFanOut` at runtime)
- **Middlewares** — wrap nodes for logging, tracing, metrics without touching business logic
- **Suspend / Resume** — a node returns `ErrSuspend`; caller persists state and `Checkpoint`; `Resume(ctx, state, cp)` continues
- **State streaming** — `Stream(ctx, state)` returns `iter.Seq2[Step[T], error]` (Go 1.23+); consume with `for step, err := range graph.Stream(...)`
- **Composition** — use a graph as a node (`AsNode()` for same state type, or `SubgraphNode` with `mapIn`/`mapOut` for nested state)

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
    out, _, err := graph.Invoke(ctx, "")
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

A node is a function `func(ctx context.Context, state T) (T, error)`: it receives context and current state, and returns the **delta** and an error. The runner applies the reducer to merge the delta into state and passes the result along the graph. On error, execution stops and the error is returned (or yielded as the second value when using `Stream`).

### Edges and conditional edges

- **Edges** (`AddEdge(from, to)`) define a fixed next node.
- **Conditional edges** (`AddConditionalEdge(from, router)`) let a router function decide the next node from `(ctx, state)`; the router returns the next node name.

### Suspend / Resume (v2)

Execution is **suspended** when a node returns `ErrSuspend` (e.g. human-in-the-loop). `Invoke` returns `(state, checkpoint, ErrSuspend)`; the **caller** persists the state and the `Checkpoint` (e.g. in a DB). To continue, call `Resume(ctx, state, cp)` with the saved state and checkpoint. The `Checkpoint` holds only `NextNode` (the node to run next); state is kept by the caller.

```go
state, cp, err := graph.Invoke(ctx, initial)
if errors.Is(err, flowy.ErrSuspend) {
    // Persist state and cp (e.g. store.Save(ctx, "session_1", state, cp))
    // Later:
    loaded, cpLoaded, _ := store.Load(ctx, "session_1")
    final, _, err := graph.Resume(ctx, loaded, cpLoaded)
}
```

## Build options

Options are set at `Compile(opts...)` and apply to all runs of that graph:

| Option | Description |
|--------|-------------|
| `WithMaxSteps(n)` | Max steps per run (prevents infinite loops; default 1000 if &lt;= 0). Returns `ErrMaxStepsExceeded` when exceeded. |
| `WithNodeTimeout(d)` | Timeout for each node execution; context is cancelled after `d`. |
| `WithMaxConcurrency(n)` | Max concurrent goroutines in fan-out; `n <= 0` means no limit. |

## Visualization (Mermaid)

You can export the compiled graph to Mermaid flowchart syntax for diagrams and debugging:

```go
graph, _ := b.Compile()
mermaid := graph.ExportMermaid()
fmt.Println(mermaid) // flowchart TD\n  a --> b ...
```

Use this to log or inspect the graph structure before running it.

## Errors

- **Panics** — not recovered by the runner; a panic in a node will terminate execution.
- **ErrSuspend** — returned when a node suspends execution (HITL). `Invoke` returns `(state, checkpoint, ErrSuspend)`; continue with `Resume(ctx, state, cp)`.
- **ErrMaxStepsExceeded** — returned when the step limit (`WithMaxSteps`) is reached (e.g. infinite loop in the graph).

## State streaming (Go 1.26+)

`Stream(ctx, state)` returns `iter.Seq2[Step[T], error]`. Each successful node yields a `Step{State: state, NodeName: name}`; on error or `ErrSuspend` the iterator yields one final `(Step{}, err)` and stops.

```go
for step, err := range graph.Stream(ctx, state) {
    if err != nil {
        if errors.Is(err, flowy.ErrSuspend) { /* save state and step.NodeName for Resume */ }
        return err
    }
    fmt.Println(step.NodeName, step.State)
}
```

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

## Fan-out (static and dynamic)

**Static fan-out:** `AddFanOut(from, targets, joinNode)` runs all nodes in `targets` in parallel, merges their results with the reducer in order, then continues at `joinNode`. `joinNode` must be a registered node (not a fan-out source).

**Dynamic fan-out:** when the set of branches is known only at runtime (e.g. from an LLM), use `AddDynamicFanOut(from, router, joinNode)`. The router receives `(ctx, state)` and returns target node names. If it returns an empty list, execution goes straight to `joinNode`.

**Limit concurrency:** to avoid rate limits (e.g. HTTP 429) or resource exhaustion when running many branches, use `WithMaxConcurrency(n)` at compile time: `Compile(flowy.WithMaxConcurrency(5))`. It applies to `Invoke`, `Stream`, and `Resume` whenever a fan-out runs.

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
