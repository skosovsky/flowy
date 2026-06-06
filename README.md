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

| v1                                | Current API                                                              |
| --------------------------------- | ------------------------------------------------------------------------ |
| `flowy.Next(nodeID)`              | **Удалено** — только `Completed()` + `AddEdge` / `AddConditionalEdge`    |
| `Graph[T]`, `Runner[T]`           | `Graph[T, E]`, `Runner[T, E]`                                            |
| `Effect(base, any)`               | `Effect[E](base, payload E)`                                             |
| `RunEvent.Metrics map[string]any` | `RunEvent.Effect E` + `HasEffect bool`                                   |
| string bindings `Set("k", v)`     | `BindingKey[T]` + `Bind` / `BindingFromContext`                          |
| `WithResumeReconciler`            | `ResumableState.Reconcile()` + conditional edges                         |
| ad-hoc resume mutations           | `WithStateOverlay` + `WithBindings` + `WithRunMetadata`                  |
| Manual checkpoint cleanup         | `WithDeleteOnSuccess`, `WithRetentionLimit` на `Compile()`               |
| Global `maxSteps` only            | + `WithNamedBudget(name, limit)` + `UseBudget` / `BudgetUsed(ctx, name)` |
| —                                 | + `ContextWithRunMetadata` for isolated node execution outside Runner    |

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

1. `Checkpointer.Load` → `ExecutionPointer` из snapshot (без внешнего start node)
2. `StateInterceptor.AfterLoad` (optional)
3. `WithStateOverlay` (optional, deterministic merge)
4. `resetSegmentCounters` — новый segment, `BudgetCounts` из snapshot сохраняются
5. `WithRunMetadata` merge (optional)
6. `ResumableState.Reconcile()` (optional, если state реализует интерфейс)
7. `execute` с сохранённого `ExecutionPointer`

`DeleteIfIdle` и delete-on-success применяются **после** `execute` и `releaseLease` (`postRunCleanup`). `Prune` (retention) — **in-loop** при suspend/handoff/cancel, до release.
Prod: paired `adapters/checkpointer/*` + `adapters/lease/*` с одним store/prefix.

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
var DBPoolKey flowy.BindingKey[*sql.DB]

bindings := flowy.NewRunBindings()
flowy.Bind(bindings, DBPoolKey, dbPool)

runner := graph.NewRunnerWithOptions(cp, []flowy.RunnerOption[State, Effect]{
    flowy.WithLeaseManager[State, Effect](leaseMgr),
})

res, err := runner.Resume(ctx, threadID,
    flowy.WithBindings[State, Effect](bindings),
    flowy.WithStateOverlay[State, Effect](overlay, mergeFn),
    flowy.WithRunMetadata[State, Effect](flowy.RunMetadataInput{
        BudgetCounts: map[string]int{"tokens": 100},
    }),
    flowy.WithInvariantValidator[State, Effect](validateFn),
    flowy.WithRunLease[State, Effect]("worker-1", 30*time.Second),
)
```

Для нескольких зависимостей одного типа используйте distinct wrapper types в `BindingKey[...]` (как в stdlib `context`).

Ephemeral bindings **не** попадают в `Snapshot`.

## Storage Layer

```go
type Checkpointer[T, E any] interface {
    Save(...)
    Load(...)
    GetHistory(...)
    Prune(...)
    Delete(...)
    DeleteIfIdle(...) // ErrThreadBusy when lease held by another owner
}
```

Compile-time policies: `WithDeleteOnSuccess(true)` (использует `DeleteIfIdle`), `WithRetentionLimit(n)`.

Postgres/Redis adapters: атомарный `DeleteIfIdle` в storage. Prod: используйте пару `adapters/checkpointer/{postgres,redis}` + `adapters/lease/{postgres,redis}` с одним prefix/store. In-process dev — auto-wrap `NewLeaseGuardCheckpointer` (только для non-native checkpointer).

Redis: `Options.LeasePrefix` checkpointer должен совпадать с `Options.Prefix` lease manager (иначе `DeleteIfIdle` обходит lease guard). Postgres использует общую таблицу `flowy_leases`.

## DX Recommendations

- Локальные aliases: `type Node = flowy.Node[State, Effect]`
- Type inference: `NewGraph[State](reducer)` → укажите `E` явно при неоднозначности: `NewGraph[State, Effect](...)`
- Handoff: foreground run завершается с `RunStatusHandoff` + checkpoint; background worker вызывает `Resume` (без передачи горутин/каналов между воркерами).
- Lease: при `WithLeaseManager` всегда указывайте `WithRunLease(owner, ttl)`; `MemoryLeaseManager` только для dev/tests

## Quality Gates

Проект содержит несколько Go-модулей (корень + adapters). Корневой `go test ./...` не покрывает adapter submodules.

```bash
make test          # все go.mod modules (рекомендуется)
golangci-lint run ./...
```

Опционально integration-тесты paired lease + checkpointer:

```bash
go test -tags=integration ./adapters/checkpointer/redis/...
FLOWY_TEST_DATABASE_URL=postgres://... go test -tags=integration ./adapters/checkpointer/postgres/...
```
