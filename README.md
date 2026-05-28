# Flowy

Flowy — production-ready framework для управляемых agentic state machine на Go.

## Core Concept: `[T, E]` Generics

`flowy` не знает структуру состояния и тип эффектов. Приложение задаёт оба параметра:

```go
type AgentState struct { /* ... */ }
type AgentEffect struct { Kind string; Payload any }

type Node = flowy.Node[AgentState, AgentEffect]
type Builder = flowy.GraphBuilder[AgentState, AgentEffect]
```

Без typed effects используйте `flowy.NoEffect`:

```go
b := flowy.NewGraph[MyState, flowy.NoEffect](reducer)
```

## Migration from v1

| v1                                | Current API                                                           |
| --------------------------------- | --------------------------------------------------------------------- |
| `flowy.Next(nodeID)`              | **Удалено** — только `Completed()` + `AddEdge` / `AddConditionalEdge` |
| `Graph[T]`, `Runner[T]`           | `Graph[T, E]`, `Runner[T, E]`                                         |
| `Effect(base, any)`               | `Effect[E](base, payload E)`                                          |
| `RunEvent.Metrics map[string]any` | `RunEvent.Effect E` + `HasEffect bool`                                |
| ad-hoc resume mutations           | `WithStateOverlay` + `WithResumeReconciler` + `WithBindings`          |
| Manual checkpoint cleanup         | `WithDeleteOnSuccess`, `WithRetentionLimit` на `Compile()`            |
| Global `maxSteps` only            | + `WithNamedBudget(name, limit)` + `UseBudget(ctx, name, n)`          |

### Migration example (routing)

**Before (v1):** узел возвращал target id через `flowy.Next("heavy_llm")`.

**After:** узел возвращает `flowy.Completed()`, маршрут объявляется в builder:

```go
return s, flowy.Completed(), nil

b.AddConditionalEdge("check_cache", func(_ context.Context, s State) (string, error) {
    if s.CacheHit {
        return "output", nil
    }
    return "heavy_llm", nil
}, "output", "heavy_llm")
```

### Resume pipeline (order)

1. `Checkpointer.Load`
2. `WithStateOverlay` (optional, deterministic merge)
3. `WithResumeReconciler` (optional, remap start node)
4. `execute`

## Directives

- `flowy.Completed()` — делегирует маршрутизацию графу
- `flowy.End()` / `flowy.Fail(reason)` / `flowy.Suspend(reason)`
- `flowy.Retry(maxAttempts)` — fallback через `AddRetryRoute(from, to)`
- `flowy.Effect[E](base, payload)`

Узлы **не** возвращают target node id. Для терминальных узлов без `Completed()` используйте `AllowNoOutgoingRoute(name)`.

## Agentic Patterns

```go
b := patterns.BuildReAct[AgentState, AgentEffect](reasonNode, actionNode, hasPending, 8)
g, err := b.Compile(flowy.WithNamedBudget("reflection", 5))
```

## Persistence, Bindings & Resume

```go
bindings := flowy.NewRunBindings()
bindings.Set("db", dbPool)

runner := graph.NewRunnerWithOptions(cp, []flowy.RunnerOption[State, Effect]{
    flowy.WithLeaseManager[State, Effect](leaseMgr),
})

res, err := runner.Resume(ctx, threadID,
    flowy.WithBindings[State, Effect](bindings),
    flowy.WithStateOverlay[State, Effect](overlay, mergeFn),
    flowy.WithResumeReconciler[State, Effect](reconcileFn),
    flowy.WithInvariantValidator[State, Effect](validateFn),
    flowy.WithRunLease[State, Effect]("worker-1", 30*time.Second),
)
```

Ephemeral bindings **не** попадают в `Snapshot`.

## Storage Layer

```go
type Checkpointer[T, E any] interface {
    Save(...)
    Load(...)
    GetHistory(...)
    Prune(...)
    Delete(...)
}
```

Compile-time policies: `WithDeleteOnSuccess(true)`, `WithRetentionLimit(n)`.

## DX Recommendations

- Локальные aliases: `type Node = flowy.Node[State, Effect]`
- Type inference: `NewGraph[State](reducer)` → укажите `E` явно при неоднозначности: `NewGraph[State, Effect](...)`
- Handoff: foreground run завершается с `RunStatusHandoff` + checkpoint; background worker вызывает `Resume` (без передачи горутин/каналов между воркерами).
- Lease: при `WithLeaseManager` всегда указывайте `WithRunLease(owner, ttl)`; `MemoryLeaseManager` только для dev/tests

## Quality Gates

```bash
go test ./...
golangci-lint run ./...
```
