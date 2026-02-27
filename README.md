# Flowy

Type-safe directed graph engine for orchestrating AI agents in Go, with support for conditional routing, parallel execution (fan-out/fan-in), checkpointing, and human-in-the-loop interrupts.

## Features

- **Generics** — strictly typed state `T`, no `interface{}` or `map[string]any`
- **Conditional edges** — routing based on state (e.g. LLM decides next step)
- **Fan-out / fan-in** — parallel execution of nodes with reducer-based merge
- **Checkpointing** — persist and resume execution (HITL, long sessions)
- **Streaming** — observe execution via events (node start/end, interrupt, error)
- **Composition** — use a graph as a node in another graph (`AsNode()`)

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

## Development

```bash
make test    # run tests with race detector
make lint    # golangci-lint
make cover   # coverage report
```

## License

See [LICENSE](LICENSE).
