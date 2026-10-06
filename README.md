# Flowy

Flowy — BYOT-библиотека исполнения графов состояния и durable orchestration на Go.

## Границы возможностей

| Область | Контракт |
| --- | --- |
| Ordinary runner | Typed snapshots, OCC, optional leases, suspend/resume и handoff outbox. Snapshot не является journal внешних действий |
| Durable runner | Sealed execution aggregate, fencing, activity journal, structured children, waits и явное recovery. Store должен поддерживать соответствующие capabilities |
| PostgreSQL | Snapshot/lease adapters и отдельный durable ExecutionStore; opt-in wait profile, indexed wait/retry discovery и lifecycle maintenance |
| Redis | Snapshot/lease adapters для standalone server. Durable ExecutionStore не поставляется; Cluster/Ring отклоняются |
| Inline subgraph | Композиция узлов и state/effects с parent execution; сама по себе не создаёт изолированный durable child |
| Structured children | Отдельные execution identities, BYOT input/result, persisted launch/outcome/join/cancellation; host выполняет child dispatch и подтверждает unknown outcomes |
| Stream | События исполнения и typed effects; terminal authority — `WaitResult()`. Token/chunk streaming model provider реализует host |
| Budget | `UseBudget` учитывает заявленные host units; declared limits проверяются до следующего dispatch. Это не резервирование денег или downstream resources |
| Supervisor helper | Supervisor выбирает route; worker с `Completed()` завершает граф. Planner, автоматический возврат к supervisor и агентный scheduler не встроены |

Host задаёт state/effects, model/tool adapters, авторизацию, правдивость usage,
downstream idempotency, transport и scheduling. Lease expiry и timeout не
доказывают отсутствие внешнего действия. Telemetry не является journal или
подтверждением commit.

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
- **LifecycleObserver:** `SetLifecycleObserver` installs one synchronous `ObserveLifecycle` callback receiving value-only operation/stage observations. Panic is contained; blocking callbacks block their caller. Default OTel metrics use `flowy.lifecycle_total{operation,stage}` with bounded dimensions; runtime IDs and diagnostic codes belong to traces. See [observation contract](docs/runtime-observation-contract.md).
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
    DeleteIfIdle(ctx context.Context, threadID string) error // ErrThreadLeaseBusy while any active lease exists
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

Lease mutations use an acquisition handle, not an owner label:

```go
lease, err := leaseMgr.Acquire(ctx, threadID, owner, ttl)
if err != nil { return err }
lease, err = leaseMgr.Renew(ctx, lease, ttl)
if err != nil { return err }
return leaseMgr.Release(ctx, lease)
```

Each acquisition gets a new incarnation, including when the owner text is reused. Expiry/release must not reset its storage counter. A stale handle returns `ErrLeaseLost` and cannot renew or release its successor. The runner carries its handle through `ExecutionLeaseFromContext`; owner-only context helpers were removed. `WithRunLease` without a configured lease manager is rejected.

This is a clear break of the lease API and persisted lease schema. Stop old workers before changing the schema/protocol; old owner-only workers cannot safely participate in the new coordination domain. Preserve fence counters independently of checkpoint retention. Existing PostgreSQL lease tables require an explicit offline schema migration, not an assumption that `CREATE TABLE IF NOT EXISTS` upgrades them. Redis lease values now carry owner and incarnation; old string values are not inferred as the new format.

Native Redis/PostgreSQL snapshot writes also check the live incarnation atomically with OCC. A supplied stale or expired handle is rejected even if no successor has advanced the checkpoint revision. Without a handle, writes are rejected while a lease is active. PostgreSQL `SaveWithOutbox` checks again before commit; enqueue callbacks must use the supplied transaction for rollback guarantees, not perform external I/O.

Redis checkpoint JSON now stores revision as a canonical decimal string to preserve the entire uint64 range in Lua. Numeric legacy revisions require explicit offline conversion; they are not silently accepted. Corrupt head metadata is rejected without overwriting history, and exhausted revisions cannot wrap to zero.

Redis adapters support one standalone server. Constructors now return an error
and reject known ClusterClient/Ring configurations. Checkpoint, lease and fence
keys use injective namespace/ID encoding in key schema v2; drain old workers and
perform an explicit offline transition preserving fence maxima. Positive
checkpoint TTL rounds up to milliseconds, TTL zero removes expiry, and negative
TTL is rejected. See the [storage adapter contract](docs/storage-adapter-contract.md).


Durable execution aggregates carry a SHA-256 seal computed after assigning the committed revision, inside the fenced/OCC write. Reads verify the exact execution identity, revision, pointer and content digest; the runtime rechecks custom-store results before codecs, migration callbacks or nodes. Missing seals and corrupt payloads return `ErrExecutionCorrupt`, never “not found” or an implicit upgrade. Unsealed legacy data needs explicit import. The seal detects content corruption; it does not establish semantic compatibility or authenticate writers who can replace both content and digest.

Runtime also validates activity journal structure and attempt/state consistency before recovery, migration or domain decoding. A valid seal does not make a malformed or inconsistent journal executable. Empty journal bytes mean no activities; JSON `null` does not. Reconciliation and manual completion retain the original unknown attempt rather than pretending the remote dispatch completed successfully.

Activity records persist their run/node/activation/key address. Runtime recomputes identity and rejects journals copied from a different execution or containing a future activation. Historical node addresses remain unchanged when migration moves the current cursor. Old unaddressed activity records need explicit conversion/import; a hash alone cannot reconstruct their address.

When migration moves a cursor with unresolved activities, its pure transform must bind current logical keys to the original identities in `MigrationState.JournalReferences`. Missing, dangling or wrong-activation bindings reject the target before commit. CallActivity and manual resolution retain the source identity and contract checks; migration does not fabricate a new external effect. All unresolved activities in the current activation block step commit. Bindings survive same-node retry and are cleared when the activation advances.

Migration validates detached target state/effects with the selected target codecs before committing the new descriptor. Invalid encoding returns `ErrMigrationInvalid`, leaving latest and source history unchanged. Target codecs and transforms must be pure: successful preparation may decode again during execution. No target codec is used to guess an incompatible source format without an explicit migration chain.

Runtime identity strings and compatibility labels must be valid UTF-8; invalid bytes reject before identity hashing or dispatch, rather than silently normalize to the replacement character. Host payload bytes remain opaque. `ErrActivityJournalUnavailable` exposes an unconfirmed journal transition and preserves the original storage cause through `errors.Is`; it is not proof of rollback. An unconfirmed intent prevents dispatch, while an unconfirmed outcome after dispatch requires authoritative recovery rather than blind retry.

Operator decisions persist the addressed attempt number, source revision, incarnation, action and host evidence. Runtime rejects inconsistent/duplicate provenance and manual outcomes that disagree with the recorded action. Starting a new dispatch clears the previous outcome origin, but retains the operator decision history. Old decisions without attempt addresses require explicit conversion/import.

The PostgreSQL lease adapter exports its own `SchemaSQL()` and can be used without the checkpoint adapter. Paired native adapters must still share the same database and lease tables.

### Explicit legacy execution import

`DurableRunner.Import(ctx, targetID, source, importer)` creates a new durable execution and returns a `ResumeToken`; it does not execute nodes or activities. Supply a `LegacyExecutionSource` with an explicit format label, source ID/revision, frozen raw payload and SHA-256 digest, plus a named pure `ExecutionImporter` for that format. The converter provides target state/effect bytes and cursor. Runtime validates target codecs/cursor before committing, retains the source artifact in `ImportProvenance`, and rejects an existing target.

The host must capture a consistent legacy source, choose its converter or compatible worker, and enforce access permissions. There is no automatic JSON guessing, synthetic activity history or assumption that old side effects were crash-safe. Resume rechecks retained source integrity before decoding/executing the target. Conversion/codec failures, cancellation and fencing/OCC errors leave no partial execution. Resume from the returned token is a separate explicit action.

Import validates target runtime collections before publication. Imported journal/child-group bindings cannot refer to nonexistent runtime records: they return `ErrExecutionImportInvalid` without a token or checkpoint, and a valid import can subsequently reuse the target ID. Host-managed inline child cursors do not fabricate distributed groups.

### Structured children

`PrepareChildren` persists a compatibility-labelled plan with stable child IDs, isolated encoded inputs, a concurrency bound, failure/cancel policy and fixed named allocations. `RunChildren` persists running intent before each host dispatch and saves sibling outcomes independently. `JoinChildren` accepts an exact group/revision assertion, merges settled results in child-ID order and commits a cached result; replay does not rerun the merge or children. Typed helpers compose host input/result codecs without imposing domain schemas.

Waiting children are resolved through `ResolveChildWait` using the original parent/node/activation/group/child/revision/wait address. Cancellation commits a request before at-least-once notification; acknowledgement alone does not confirm termination. Use `ConfirmChildCancellation` only with host evidence. Abandoned running work becomes unknown and cannot be blindly relaunched. `ReturnChildBudget` returns unused named units once under an addressed usage claim; unknown work keeps its allocation. Monetary reservations and downstream cancellation/idempotency remain host responsibilities.

When a migration moves a cursor with unjoined groups, supply `MigrationState.ChildGroupReferences` mapping current logical group keys to their original identities. Child outcomes, original wait/cancel addresses, capacity and usage history remain unchanged. Missing bindings reject before migration commit. Normal advancement clears bindings; same-activation retry preserves them. See [child migration contract](docs/child-migration-contract.md) and [budget contract](docs/child-budget-contract.md).

### Exact historical inspection

`ExecutionHistoryStore` is an optional capability, separate from `ExecutionStore`; ordinary durable `Start`/`Resume` do not require history. `InspectExecutionCheckpoint(ctx, store, HistoricalCheckpointReference{ExecutionID: id, Revision: revision, Digest: expectedSeal})` loads that exact immutable revision and returns detached raw data without host codecs, migration, lease acquisition or node calls. Missing/pruned checkpoints return `ErrExecutionCheckpointUnavailable`; unsupported stores return `ErrExecutionHistoryUnsupported`; source digest mismatch returns `ErrForkSourceDigest`. Zero revision never selects latest. Host owns authorization and dependency-safe retention.

`DurableRunner.Fork(ctx, request)` saves a fresh target boundary and immutable `ForkLineage`; it does not run nodes, copy source journals/effects or change source. Default mode is fake: creation without an execution policy is read-only until `DurableOptions.ForkPolicy` supplies matching labels/mode and a fake activity dispatcher. Fake outcomes carry `ActivitySimulated`; a live activity dispatcher is never used in fake mode. Live forks require explicit policy and a separately labelled opaque-state projection; current host authorization is checked on creation, recovery and external operations. Armed waits, unresolved activities and unjoined descendants block creation. Memory/PostgreSQL stores retain an independent creation-lineage anchor, so resealing a modified payload cannot erase or replace lineage. Existing fork heads without that anchor need an explicit offline migration; there is no inference from payload/history. See [fork contract](docs/execution-fork-contract.md).

### Durable waits and recovery ownership

Durable wait registration is opt-in: configure a `WaitCapabilityProfile` with explicit journal, lease, timer, clock, retry and recovery owners on the same capable execution store and pass it through `DurableOptions.WaitProfile`. `Await(spec)` is separate from ordinary `Suspend`: it atomically persists the reduced state/effects and armed wait before registration acknowledgement. An armed `Resume` retries registration of the same generation without rerunning the node. `InspectExecutionWaits` reads sealed metadata without host codecs. The optional PostgreSQL `NewWaitExecutionStore` provides registration and bounded due discovery; discovery is not timer acceptance.

`DeliverWait(ctx, executionID, delivery, contract)` checks persisted matcher/payload/continuation labels before pure host callbacks. It commits accepted decision, transformed state and selected event/timeout cursor together under OCC/fencing, then returns a recovery token without executing a node. Duplicate replay calls no matcher/codec/transition; a loser adds only its durable decision. Callback/codec/write errors acknowledge no acceptance. Host owns payload decoding, permission checks and redelivery on explicit `ErrWaitNotArmed`/ownership failures.

`CancelWait(ctx, token, request)` atomically stores cancellation ID/reason/evidence and a failed terminal (`durable_wait_canceled`), preserving state and effects without node/codec/remote-cancel calls. The same addressed cancellation replays without a write. Late deliveries record canceled rejection and cannot clear the terminal; Resume returns the persisted failure. This does not prove that remote work stopped. PostgreSQL indexed due discovery exposes waits and prepared activity retries through a profile/clock-bound deadline keyset cursor and authoritative head revalidation; discovery neither accepts a timer nor acknowledges/dispatches work. The host supplies polling/scheduling, maps transport identities and resumes only from committed recovery tokens. Configure one recovery/retry owner; do not add a competing scheduler for the same execution. See [wait contract](docs/durable-wait-contract.md).

### Durable terminal failures

Durable descriptors require an explicit graph-wide replay declaration:

```go
ReplayPolicy: flowy.StepReplayPolicy{
    Label: "host-replay-safe-steps",
    Mode:  flowy.StepReplaySafe,
},
```

The host attests that node computation/routing may repeat after an uncommitted step and external effects use `CallActivity`. Runtime does not prove purity or intercept arbitrary I/O. Empty/unsupported policies reject; label/mode changes require an explicit descriptor migration before codec/node calls. Legacy envelopes without this field need explicit import. Stop old durable workers when changing the persisted protocol; do not mix workers that ignore replay policy with workers that enforce it. Ordinary runners do not acquire these durable guarantees.

Definitive node/directive failures persist their terminal state, reason and original error message. `Resume`/`ResumeStream` return that stored outcome without rerunning the terminal node. Live errors retain their original Go identity; recovered errors are `*PersistedExecutionError`, matching `ErrExecutionFailed`. Do not infer a host sentinel/type from its error text.

Failure events follow the terminal commit, just like completion events. A failed write exposes an error and the current recovery token, not a committed-failure event. Pending retries, unknown/running activities, cancellation and lost ownership do not become definitive terminal failures. Invalid terminal metadata rejects before state codecs or node execution.

### Manual activity resolution

`DurableRunner.ResolveActivity(ctx, token, resolution)` records an operator decision for one current-activation activity. Supply its identity, expected input digest/implementation, unique decision ID, reason and host evidence reference. Use `ActivityResolveComplete` with confirmed output, `ActivityResolveFail` for a definitive failed outcome, or `ActivityResolveRetry` with the exact already-persisted safe-retry contract. Retry cannot reset attempts or bypass the original limit; it commits a deadline and releases the worker rather than dispatching or sleeping.

The operation checks latest revision and a live fenced lease, and never calls nodes, codecs, reconciliation or dispatcher. Completed/manual provenance and abandoned attempts remain separate: an operator-confirmed result does not turn an unknown dispatch attempt into a successful one. Stale/duplicate decisions, changed inputs/implementation and unsafe/exhausted retries reject without changing aggregate history. Host owns authorization and the meaning of evidence. Resume from the returned token is a separate action; completion replays the stored output, failure stays definitive, and retry waits for its committed deadline.

### Runnable durable examples and migration guidance

Run `go run ./examples/durable_runtime` for self-verifying BYOT examples covering activity unknown/reconciliation/replay, bounded retry/deadlines, explicit migration/import, typed children/join/budgets, armed wait delivery/dedup and fake historical fork. The examples use memory storage and a demonstration registration capability, not a production persistent backend or scheduler. Their AAA test is included in the root suite.

The separate [PostgreSQL blueprint](examples/durable_agent/README.md) verifies lost
commit replies, approval arbitration, isolated child recovery and final replay on
six fresh workers/pools with host-owned types and deterministic external fakes.
Its integration test requires PostgreSQL; memory smoke is a separate gate.
[Performance gates](docs/performance-gates.md) require actual measured workloads
and fixed allocation ceilings, including negative fixtures for missing benchmarks.

See [consumer migration guide](docs/durable-migration-guide.md) for replacing owner-only lease APIs, converting stored formats explicitly, assigning descriptor/codecs, moving external writes to activities, configuring child/wait ownership and avoiding inherited fork authority. Core/runtime and backend guarantees must be checked separately; successful memory examples do not prove persistence after process loss.

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
