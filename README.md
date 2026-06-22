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

## Migration from Previous API

| Previous API                      | Current API                                                                             |
| --------------------------------- | --------------------------------------------------------------------------------------- |
| `flowy.Next(nodeID)`              | **Удалено** — только `Completed()` + `AddEdge` / `AddConditionalEdge`                   |
| `Graph[T]`, `Runner[T]`           | `Graph[T, E]`, `Runner[T, E]`                                                           |
| `Effect(base, any)`               | `Effect[E](base, payload E)`                                                            |
| `RunEvent.Metrics map[string]any` | `RunEvent.Effect E` + `HasEffect bool`                                                  |
| string bindings `Set("k", v)`     | `BindingKey[T]` + `Bind` / `BindingFromContext`                                         |
| implicit state resume hook        | explicit `WithResumeTargetPolicy` returning `ResumePlan`                                |
| `Runner.Resume(ctx, threadID)`    | `Runner.Resume(ctx, flowy.ResumeToken{ThreadID, SnapshotRevision})`                     |
| Checkpointer pointer rewrite      | `Suspend(..., ResumeAt(node))` / `Handoff(..., ResumeAt(node))`                         |
| Manual handoff queue + rollback   | `WithHandoffOutbox` with canonical `HandoffIntent`                                      |
| context error collectors          | `WithCheckpointErrorPolicy(CheckpointPolicySkipOnSaveError)` + `EventCheckpointFailed`  |
| ad-hoc resume mutations           | `WithStateOverlay` + `WithBindings` + `WithRunMetadata`                                 |
| Manual checkpoint cleanup         | `WithDeleteOnSuccess`, `WithRetentionLimit` на `Compile()`                              |
| Global `maxSteps` only            | + `WithNamedBudget(name, limit)` + `UseBudget` / `BudgetUsed(ctx, name)`                |
| —                                 | + `ContextWithRunMetadata` for isolated node execution outside Runner                   |

### Migration example (routing)

**Before:** узел возвращал target id через `flowy.Next("heavy_llm")`.

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
7. `WithResumeTargetPolicy` (optional explicit state-aware `ResumePlan` после overlay)
8. validate active `ExecutionPointer` (non-empty, узел в графе)
9. `execute` с активного `ExecutionPointer`

`WithInvariantValidator` — in-loop в `execute`, не в `prepareResume`.

### State-aware resume target

Когда overlay делает сохранённый wait-узел stale (например, HITL: вместо ответа пользователя пришёл новый маршрут), передайте явную `WithResumeTargetPolicy`. Policy возвращает typed `ResumePlan`; движок валидирует, что target существует в графе, и стартует execute с выбранной точки.

```go
res, err := runner.Resume(ctx, token,
    flowy.WithStateOverlay[State, Effect](overlay, mergeFn),
    flowy.WithResumeTargetPolicy[State, Effect](
        func(ctx context.Context, state State, current flowy.ExecutionPointer) (State, flowy.ResumePlan, error) {
            if current == "wait_user" && state.RouteReady {
                return state, flowy.ResumeTo("router"), nil
            }
            return state, flowy.ResumeCurrent(), nil
        },
    ),
)
```

См. `examples/conditional_routing` и unit-тест `TestResumeTargetPolicyRewritesPointer` в `runner_resume_overlay_test.go`.

`DeleteIfIdle` и delete-on-success применяются **после** `execute` и `releaseLease` (`postRunCleanup`). `Prune` (retention) — **in-loop** при suspend/handoff/cancel, до release.
Prod: pair native checkpointer and lease adapters in one coordination domain.

## Directives

- `flowy.Completed()` — делегирует маршрутизацию графу
- `flowy.End()` / `flowy.Fail(reason)` / `flowy.Suspend(reason, flowy.ResumeAt(node))`
- `flowy.Handoff(reason, flowy.ResumeAt(node))`
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

// После Suspend/Handoff используйте только result.ResumeToken.
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

### Lifecycle Contracts

- **Resume target:** `ResumeAt(node)` на `Suspend`/`Handoff` задает persisted `ExecutionPointer` до `Save`; target валидируется ядром.
- **Resume planning:** `WithResumeTargetPolicy` возвращает `ResumePlan` (`ResumeCurrent()` или `ResumeTo(node)`), а не raw pointer. Пустой plan отклоняется как `ErrInvalidResumePlan`; unknown node отклоняется до execute.
- **Strict OCC Checkpointer:** `Save(ctx, expectedRevision, snapshot) (newRevision, error)` и `Load(ctx, threadID) (snapshot, revision, error)`. Конфликт Save или несовпадение `ResumeToken.SnapshotRevision` при `Resume` → `ErrConcurrencyConflict`; orchestration code вызывает `EvaluateResume` и получает typed decision с текущим core-issued token, когда snapshot уже продвинулся.
- **Resume preflight:** `EvaluateResume` и `EvaluateHandoffRecovery` возвращают typed `ResumeDecision`; `Resume`, `ResumeStream` и `RecoverStaleHandoff` используют тот же normalized path.
- **Checkpoint record envelope:** adapters use `checkpoint.Record`, `checkpoint.EncodeRecord`, and `checkpoint.DecodeRecord` with `DecodeRecordOptions`. Decode returns a validated `Snapshot` or `ErrSnapshotEnvelopeInvalid`; applications should not compare storage metadata and payload envelope by hand.
- **Handoff Outbox FSM:** `WithHandoffOutbox` — 3-phase: Save `pending` → patch `enqueued` → `EnqueueIntent(HandoffIntent)`; если enqueue падает, core патчит `orphaned`. `HandoffIntent` carries `PendingSnapshotRevision`, `CommittedSnapshotRevision`, `SnapshotRevision`, `ResumeToken`, `HandoffStatus`, reason, and execution pointer; normal consumers receive the committed enqueued revision and do not guess `revision` vs `revision+1`. При ошибке enqueue snapshot **сохраняется**; terminal reason `handoff_orphaned` только если patch в `orphaned` успешен (иначе directive reason, snapshot может остаться `enqueued`). `RunResult.ResumeToken` для retry (`ErrHandoffEnqueueFailed`). Transactional path требует `TransactionalCheckpointer.SaveWithOutbox` и `TransactionalHandoffOutbox.EnqueueIntentTx(ctx, tx, intent)`: checkpointer callback передает explicit transaction handle и saved revision, а core строит authoritative enqueued `HandoffIntent`; context-carried transaction state не используется. Lease guard делегирует TX только при inner `TransactionalCheckpointer`, иначе используется 3-phase FSM. At-least-once consumer начинает с `EvaluateResume`: stale-token decision возвращает текущий core-issued token, pending/orphaned уводит в recovery contract.
- **Recovery:** `RecoverStaleHandoff` для `orphaned` и stale `pending` (TTL `WithHandoffStaleAfter`, default 5m) — всегда 3-phase FSM. Возвращает `HandoffRecoveryResult` + error; result содержит typed `Decision`, `ResumeToken`, snapshot revision, recovered status и persisted handoff status. `WithRecoverForceReenqueue(true)` — force re-enqueue для `enqueued` без сообщения в outbox. Cron recovery должен быть **single-leader** или защищен external lock; `RecoverStaleHandoff` сам не берет run lease. Свежий `pending` → `ErrHandoffPending`; уже `enqueued` → `ErrHandoffAlreadyEnqueued`; `HandoffStatusNone`/unknown → `ErrHandoffNotRecoverable`; direct Resume на `orphaned` → `ErrHandoffOrphaned`. Пустой `HandoffPendingAt` считается stale сразу.
- **Worst-case runbook:** patch `enqueued` OK + enqueue fail + orphan patch fail → snapshot остается `enqueued`, но outbox message отсутствует. Recovery scanner can use `RecoverStaleHandoff(..., WithRecoverForceReenqueue(true))` for known false-enqueued rows. Pending checkpoints remain recoverable after TTL when the run crashed before the enqueued patch.
- **LifecycleObserver:** process-wide hook (`SetLifecycleObserver`) receives handoff/recovery/checkpoint-soft events. Метрики `handoff_enqueued_total{status}`: `success`, `enqueue_failed`, `patch_enqueued_failed`, `patch_orphan_failed`, `save_failed`, `commit_failed`.
- **Skip-on-save-error checkpoint policy:** `WithCheckpointErrorPolicy(CheckpointPolicySkipOnSaveError)` эмитит `EventCheckpointFailed` в stream без прерывания terminal flow; reason suffixes `*_checkpoint_skipped` при неуспешном persist. `EventCheckpointFailed.ExecutionPointer` совпадает с persisted pointer в snapshot, как и terminal events.
- **Retention / cancel reasons:** `*_retention_failed` при ошибке Prune после save; `context_canceled_save_failed` при HardFail cancel save; Stream `Event.Reason` совпадает с `RunResult.Reason`.
- **Dual retention:** in-loop Prune (suspend/handoff/cancel) возвращает ошибку caller; `postRunCleanup` Prune (Completed/Failed) — log only.
- **Event==Result invariant:** на Stream terminal `Event.Reason` и sync `RunResult.Reason` совпадают (включая retention suffix до emit). `StreamHandle.WaitResult()` возвращает terminal `RunResult` + error; `Wait()` оставлен как error-only helper.
- **RequestLocalHandoff return matrix:** `nil` = persisted handoff; `ErrCheckpointSkipped` = SkipOnSaveError skip (no snapshot); `ErrHandoffEnqueueFailed` = enqueue fail after persist (snapshot + `ResumeToken` for Outbox retry); wrapped retention/save errors otherwise. Stream `RequestLocalHandoff` mirrors the same errors on `Wait()`.
- **Persist-vs-event / consumer stop:** terminal event может не дойти до consumer; terminal `RunResult` из `WaitResult()` остается source of truth для run outcome, а checkpoint нужен для durable resume/recovery. Не вызывайте `RequestLocalHandoff` после `RequestStop` (`ErrNoActiveExecution`).
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
outcome := result.Outcome

// Handoff/HITL: terminal outcome owns ResumeToken; optional snapshot load is diagnostic only
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

Adapters should persist `checkpoint.Record` values through `checkpoint.EncodeRecord`. On load, call `checkpoint.DecodeRecord` with the expected thread id, revision, or execution pointer when those values came from storage columns. A mismatch is a typed envelope error; do not duplicate split-brain checks in application code.

Compile-time policies: `WithDeleteOnSuccess(true)` (использует `DeleteIfIdle`), `WithRetentionLimit(n)`.

Native adapters should pair checkpointer and lease records in the same coordination domain. In-process dev auto-wraps `NewLeaseGuardCheckpointer` for non-native checkpointers.

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

Adapter-specific integration tests live with their adapter modules; keep adapter imports out of root integration tests to avoid import cycles.

Stress gate для handoff/resume/orphan контрактов:

```bash
make verify-stress
```
