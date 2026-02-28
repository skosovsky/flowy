# Flowy

Type-safe directed graph engine for orchestrating AI agents in Go, with support for conditional routing, parallel execution (fan-out/fan-in), checkpointing, and human-in-the-loop interrupts.

## Features

- **Generics** — strictly typed state `T`, no `interface{}` or `map[string]any`
- **Conditional edges** — routing based on state (e.g. LLM decides next step)
- **Fan-out / fan-in** — parallel execution of nodes with reducer-based merge (static or dynamic at runtime)
- **Middlewares** — wrap nodes for logging, tracing, metrics without touching business logic
- **Checkpointing** — persist and resume execution (HITL, long sessions)
- **Streaming** — observe execution via events (node start/end, interrupt, error)
- **Composition** — use a graph as a node in another graph (`AsNode()` when state types match; or call `Invoke` from a node to adapt state)

## Requirements

- Go 1.26+

## Installation

```bash
go get github.com/skosovsky/flowy
```

## Quick start

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

## Dynamic fan-out

When the set of parallel branches is known only at runtime (e.g. from an LLM), use `AddDynamicFanOut(from, router, joinNode)`. The router receives context and state and returns target node names. If it returns an empty list, execution goes straight to `joinNode`. For both static (`AddFanOut`) and dynamic fan-out, `joinNode` must be a registered executable node, not a fan-out or dynamic fan-out source.

**Note:** `InterruptBefore` and `InterruptAfter` are not supported on nodes that run as fan-out targets (static or dynamic). For static fan-out, `Compile()` fails if such a node is a target; for dynamic fan-out, `Invoke` returns an error at runtime if the router returns such a node name.

**Best practice — limit concurrency:** When using dynamic fan-out, the LLM (or router) can return many targets at runtime (e.g. 50 search queries). Running all of them at once can trigger API rate limits (HTTP 429) or exhaust connections. Use `flowy.WithMaxConcurrency(n)` so that at most `n` fan-out branches run concurrently; the rest are scheduled as slots free up. Example: if the router returns 50 targets and you call `Invoke(ctx, state, flowy.WithMaxConcurrency(5))`, only 5 requests run in parallel at any time, reducing the risk of 429s while still making progress.

**Scope:** `WithMaxConcurrency` applies to every execution path that runs a fan-out: `Invoke`, `Stream`, and `Resume`. When you resume into a step that is a fan-out (or that leads to one), the same limit is enforced for that fan-out.

```go
// Limit to 5 concurrent fan-out branches (e.g. 50 search tasks run in batches of 5)
out, err := graph.Invoke(ctx, state, flowy.WithMaxConcurrency(5))
// Or with Stream:
for e := range graph.Stream(ctx, state, flowy.WithMaxConcurrency(5)) {
    // ...
}
// Or with Resume (limit applies when continuation runs a fan-out):
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
// Node returns a StateUpdate instead of full state.
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
