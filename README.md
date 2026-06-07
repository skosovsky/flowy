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

| v1                                | Current API                                                                             |
| --------------------------------- | --------------------------------------------------------------------------------------- |
| `flowy.Next(nodeID)`              | **Удалено** — только `Completed()` + `AddEdge` / `AddConditionalEdge`                   |
| `Graph[T]`, `Runner[T]`           | `Graph[T, E]`, `Runner[T, E]`                                                           |
| `Effect(base, any)`               | `Effect[E](base, payload E)`                                                            |
| `RunEvent.Metrics map[string]any` | `RunEvent.Effect E` + `HasEffect bool`                                                  |
| string bindings `Set("k", v)`     | `BindingKey[T]` + `Bind` / `BindingFromContext`                                         |
| `WithResumeReconciler`            | state implements `ResumeReconciler.ReconcileResume()`                                   |
| `Runner.Resume(ctx, threadID)`    | `Runner.Resume(ctx, flowy.ResumeToken{ThreadID, SnapshotRevision})`                     |
| Checkpointer pointer rewrite      | `WithSuspendPointerResolver` (save-path, до `Checkpointer.Save`)                        |
| Manual handoff queue + rollback   | `WithHandoffOutbox` (save → `EnqueueIntent`; snapshot сохраняется, `HandoffStatus` FSM) |
| context error collectors          | `WithCheckpointErrorPolicy(CheckpointPolicySkipOnSaveError)` + `EventCheckpointFailed`  |
| ad-hoc resume mutations           | `WithStateOverlay` + `WithBindings` + `WithRunMetadata`                                 |
| Manual checkpoint cleanup         | `WithDeleteOnSuccess`, `WithRetentionLimit` на `Compile()`                              |
| Global `maxSteps` only            | + `WithNamedBudget(name, limit)` + `UseBudget` / `BudgetUsed(ctx, name)`                |
| —                                 | + `ContextWithRunMetadata` for isolated node execution outside Runner                   |

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

1. `ResumeToken` validation (`ThreadID` non-empty)
2. `Checkpointer.Load` → OCC: `token.SnapshotRevision` must equal `snapshot.Revision`
3. `StateInterceptor.AfterLoad` (optional)
4. `WithStateOverlay` (optional, deterministic merge)
5. `resetSegmentCounters` — новый segment, `BudgetCounts` из snapshot сохраняются
6. `WithRunMetadata` merge (optional)
7. `ResumeReconciler.ReconcileResume()` (optional pointer rewind после overlay)
8. validate active `ExecutionPointer` (non-empty, узел в графе)
9. `execute` с активного (post-reconcile) `ExecutionPointer`

`WithInvariantValidator` — in-loop в `execute`, не в `prepareResume`.

### Pointer rewind

Когда overlay делает сохранённый wait-узел stale (например, HITL: вместо ответа пользователя пришёл новый маршрут), реализуйте `ResumeReconciler` на state и верните другой `ExecutionPointer` из `ReconcileResume`. Движок валидирует, что узел существует в графе, и стартует execute с возвращённого pointer.

```go
func (s *MyState) ReconcileResume(currentPtr flowy.ExecutionPointer) (flowy.ExecutionPointer, error) {
    if currentPtr == "wait_user" && s.RouteReady {
        return "router", nil
    }
    return currentPtr, nil
}
```

См. `examples/conditional_routing` (сценарий 4 — rewind), `examples/hitl_agent` (default overlay resume без rewind) и unit-тест `TestResumeReconcilerPointerRewind` в `runner_resume_overlay_test.go`.

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

// После Suspend/Handoff используйте result.ResumeToken (или ResumeTokenFromSnapshot после Load).
res, err := runner.Resume(ctx, suspended.ResumeToken,
    flowy.WithBindings[State, Effect](bindings),
    flowy.WithStateOverlay[State, Effect](overlay, mergeFn),
    flowy.WithRunMetadata[State, Effect](flowy.RunMetadataInput{
        BudgetCounts: map[string]int{"tokens": 100},
    }),
    flowy.WithInvariantValidator[State, Effect](validateFn),
    flowy.WithRunLease[State, Effect]("worker-1", 30*time.Second),
)
```

### Lifecycle contracts (Task18 + Task19)

- **Save-path pointer:** `WithSuspendPointerResolver` нормализует `ExecutionPointer` до `Save` на Suspend/Handoff (Checkpointer остаётся dumb CRUD).
- **Strict OCC Checkpointer:** `Save(ctx, expectedRevision, snapshot) (newRevision, error)` и `Load(ctx, threadID) (snapshot, revision, error)`. Конфликт Save или несовпадение `ResumeToken.SnapshotRevision` при `Resume` → `ErrConcurrencyConflict` (воркер: Load → свежий `ResumeToken` → retry).
- **Handoff Outbox FSM:** `WithHandoffOutbox` — 3-phase: Save `pending` → `EnqueueIntent` (token rev = pending) → patch `enqueued` / `orphaned`. При ошибке enqueue snapshot **сохраняется**; terminal reason `handoff_orphaned` только если patch в `orphaned` успешен (иначе directive reason, БД может остаться `pending`). `RunResult.ResumeToken` для retry (`ErrHandoffEnqueueFailed`). **Phase B:** при `TransactionalCheckpointer` (postgres `SaveWithOutbox`) initial handoff сохраняет `enqueued` в одной TX с `EnqueueIntent` (без `pending`); tx через `ContextWithOutboxTx` / `postgres.PgxTxFromContext`. `WithRunLease` + memory/redis CP: lease guard делегирует TX только при inner `TransactionalCheckpointer`, иначе 3-phase FSM. At-least-once: consumer идемпотентен по `thread_id`.
- **Recovery:** `RecoverStaleHandoff` для `orphaned` и stale `pending` (TTL `WithHandoffStaleAfter`, default 5m) — всегда 3-phase FSM, даже с postgres TX adapter. `WithRecoverForceReenqueue(true)` — reconcile `enqueued` без сообщения в outbox. Cron recovery — **single-leader** или worker с `WithRunLease`. Свежий `pending` → `ErrHandoffPending`; уже `enqueued` → `ErrHandoffAlreadyEnqueued`; `HandoffStatusNone`/unknown → `ErrHandoffNotRecoverable`; direct Resume на `orphaned` → `ErrHandoffOrphaned`. Пустой `HandoffPendingAt` на legacy rows = stale сразу.
- **Worst-case runbook:** enqueue OK + оба patch fail → БД `pending@N`, сообщение в outbox; consumer получает stale token → `Load` → `RecoverStaleHandoff` после TTL. Query: `handoff_status='pending' AND handoff_pending_at < now() - interval '5 min'` OR `handoff_status='orphaned'`.
- **LifecycleObserver:** process-wide hook (`SetLifecycleObserver`); production bootstrap: `ext/otel.InstallLifecycleObserver()` (+ optional `InstallLifecycleObserverWithTracing()`). Метрики `handoff_enqueued_total{status}`: `success`, `enqueue_failed`, `patch_enqueued_failed`, `patch_orphan_failed`, `save_failed`, `commit_failed`.
- **Skip-on-save-error checkpoint policy:** `WithCheckpointErrorPolicy(CheckpointPolicySkipOnSaveError)` эмитит `EventCheckpointFailed` в stream без прерывания terminal flow; reason suffixes `*_checkpoint_skipped` при неуспешном persist. `EventCheckpointFailed.ExecutionPointer` совпадает с resolved pointer в snapshot (после `WithSuspendPointerResolver`), как и terminal events.
- **Retention / cancel reasons:** `*_retention_failed` при ошибке Prune после save; `context_canceled_save_failed` при HardFail cancel save; Stream `Event.Reason` совпадает с `RunResult.Reason`.
- **Dual retention:** in-loop Prune (suspend/handoff/cancel) возвращает ошибку caller; `postRunCleanup` Prune (Completed/Failed) — log only.
- **Event==Result invariant:** на Stream terminal `Event.Reason` и sync `RunResult.Reason` совпадают (включая retention suffix до emit). `StreamHandle.Wait()` не возвращает `RunResult` — только ошибку завершения goroutine (`nil` после RequestStop+persist, `ErrCheckpointSkipped` при SkipOnSaveError skip, `context.Canceled` при ctx cancel, wrapped error при retention/enqueue fail).
- **RequestLocalHandoff return matrix:** `nil` = persisted handoff; `ErrCheckpointSkipped` = SkipOnSaveError skip (no snapshot); `ErrHandoffEnqueueFailed` = enqueue fail after persist (snapshot + `ResumeToken` for Outbox retry); wrapped retention/save errors otherwise. Stream `RequestLocalHandoff` mirrors the same errors on `Wait()`.
- **Persist-vs-event / consumer stop:** после `RequestStop` terminal event может не дойти до consumer; **snapshot/checkpointer — source of truth**. Не вызывайте `RequestLocalHandoff` после `RequestStop` (`ErrNoActiveExecution`).
- **Stream consumer helpers:** `CollectEventsAndWait`, `ConsumeEventsAndWait`, `BeginStreamCollect` + `AwaitStreamCollect` — безопасный drain+`Wait` без дедлока. Примеры:

```go
// run-to-completion
events, err := flowy.CollectEventsAndWait(ctx, handle)

// early stop из callback (false → RequestStop + silent drain)
err := flowy.ConsumeEventsAndWait(ctx, handle, func(ev flowy.RunEvent[S, E]) bool {
    return ev.Type != flowy.EventSuspended
})

// concurrent stop / handoff (BeginStreamCollect до RequestStop или cancel)
out := flowy.BeginStreamCollect(handle)
handle.RequestStop() // или RequestLocalHandoff / ctx cancel
result, err := flowy.AwaitStreamCollect(ctx, handle, out)

// Handoff/HITL: snapshot + ResumeToken после await
result, err := flowy.AwaitStreamCollectWithSnapshot(ctx, handle, out, cp, threadID)
```

См. `examples/stream_request_stop` и `examples/streaming_agent`.

Для нескольких зависимостей одного типа используйте distinct wrapper types в `BindingKey[...]` (как в stdlib `context`).

Ephemeral bindings **не** попадают в `Snapshot`.

## Storage Layer

```go
type Checkpointer[T, E any] interface {
    Save(ctx context.Context, expectedRevision uint64, snapshot Snapshot[T, E]) (newRevision uint64, err error)
    Load(ctx context.Context, threadID string) (snapshot Snapshot[T, E], revision uint64, err error)
    GetHistory(ctx context.Context, threadID string, limit int) ([]Snapshot[T, E], error)
    Prune(ctx context.Context, threadID string, retainCount int) error
    Delete(ctx context.Context, threadID string) error
    DeleteIfIdle(ctx context.Context, threadID string) error // ErrThreadLeaseBusy when lease held by another owner
}
```

Compile-time policies: `WithDeleteOnSuccess(true)` (использует `DeleteIfIdle`), `WithRetentionLimit(n)`.

Postgres/Redis adapters: атомарный `DeleteIfIdle` в storage. Prod: используйте пару `adapters/checkpointer/{postgres,redis}` + `adapters/lease/{postgres,redis}` с одним prefix/store. In-process dev — auto-wrap `NewLeaseGuardCheckpointer` (только для non-native checkpointer).

Redis: `Options.LeasePrefix` checkpointer должен совпадать с `Options.Prefix` lease manager (иначе `DeleteIfIdle` обходит lease guard). Postgres использует общую таблицу `flowy_leases`.

## DX Recommendations

- **Node authoring:** respect `ctx` in all I/O and loops — see [docs/node_authoring.md](docs/node_authoring.md) and `examples/context_deadline`.
- Локальные aliases: `type Node = flowy.Node[State, Effect]`
- Type inference: `NewGraph[State](reducer)` → укажите `E` явно при неоднозначности: `NewGraph[State, Effect](...)`
- Handoff: foreground run завершается с `RunStatusHandoff` + checkpoint + `ResumeToken`; background worker вызывает `Resume(token)` (без передачи горутин/каналов между воркерами).
- Lease: при `WithLeaseManager` всегда указывайте `WithRunLease(owner, ttl)`; `MemoryLeaseManager` только для dev/tests

## Quality Gates

Проект содержит несколько Go-модулей (корень + adapters). Корневой `go test ./...` не покрывает adapter submodules.

```bash
make test          # все go.mod modules (рекомендуется)
make test-race && make test-goleak && make lint
go test -count=20 -run 'Close|Stop|Wait|Handoff|Lease|Checkpoint|ResumeStream|StreamCollect|ConsumeEvents' .
```

Опционально integration-тесты paired lease + checkpointer (включая runner handoff E2E на postgres adapter):

```bash
go test -tags=integration ./adapters/checkpointer/redis/...
FLOWY_TEST_DATABASE_URL=postgres://... go test -tags=integration ./adapters/checkpointer/postgres/...
```

Runner postgres E2E (`TestRunnerHandoffPostgresTransactional*`, `TestRecoverStaleHandoffPostgres*`) живут в `adapters/checkpointer/postgres/integration_test.go` — не добавляйте root `//go:build integration` с импортом `checkpoint`/adapter (import cycle).

Stress gate для handoff/resume/orphan контрактов:

```bash
make verify-stress
```
