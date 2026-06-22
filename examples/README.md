# Flowy Cookbook

Runnable-примеры на API `Graph[T,E]`, `Runner.Start/Resume/Stream/ResumeStream`, декларативного роутинга (`Completed` + edges) и typed effects.

## Lifecycle API

| Entry point    | Sync result              | Stream events | Terminal authority      |
| -------------- | ------------------------ | ------------- | ----------------------- |
| `Start`        | `RunResult` + error      | —             | returned error / result |
| `Resume`       | `RunResult` + error      | —             | returned error / result |
| `Stream`       | handle opens immediately | `Events()`    | `WaitResult()`          |
| `ResumeStream` | handle opens immediately | `Events()`    | `WaitResult()`          |

- `StreamHandle.RequestStop()` — consumer-initiated stop (cancels run context + closes event sink). Do not call `RequestLocalHandoff` after `RequestStop`.
- `StreamHandle.WaitResult()` — authoritative terminal result and error (`nil`, `ErrCheckpointSkipped`, retention/save errors, `context.Canceled`).
- **Stream helpers:** `CollectEventsAndWait`, `ConsumeEventsAndWait`, `BeginStreamCollect` + `AwaitStreamCollect(ctx, handle, out)` — safe drain+Wait; `StreamCollectResult.Outcome` carries the terminal `RunResult`.
- `RequestLocalHandoff` — in-process active-run handoff on **this** `Runner` instance only; cross-process continuation uses checkpoint + lease + `Resume`.

## Матрица сценариев

| Сценарий                           | Каталог                | Команда                                              |
| ---------------------------------- | ---------------------- | ---------------------------------------------------- |
| ReAct loop                         | `react_agent`          | `cd examples/react_agent && go run main.go`          |
| Stream + typed Effect + Suspend    | `streaming_agent`      | `cd examples/streaming_agent && go run main.go`      |
| Stream RequestStop (anti-deadlock) | `stream_request_stop`  | `cd examples/stream_request_stop && go run main.go`  |
| Human-in-the-Loop                  | `hitl_agent`           | `cd examples/hitl_agent && go run main.go`           |
| Middleware + panic recovery        | `middleware_agent`     | `cd examples/middleware_agent && go run main.go`     |
| Supervisor / multi-agent           | `multi_agent`          | `cd examples/multi_agent && go run main.go`          |
| Context deadline + emergency save  | `context_deadline`     | `cd examples/context_deadline && go run main.go`     |
| Cache routing + late binding       | `conditional_routing`  | `cd examples/conditional_routing && go run main.go`  |
| Parent + subgraph suspend/resume   | `subgraph_agent`       | `cd examples/subgraph_agent && go run main.go`       |
| Subgraph slot suspend/resume       | `subgraph_slot_agent`  | `cd examples/subgraph_slot_agent && go run main.go`  |
| Lease + WithRunLease               | `lease_agent`          | `cd examples/lease_agent && go run main.go`          |
| Typed BindingKey                   | `bindings_agent`       | `cd examples/bindings_agent && go run main.go`       |
| Semantic cache routing             | `semantic_cache_agent` | `cd examples/semantic_cache_agent && go run main.go` |
| Late prompt policy (compile-time)  | `late_prompt_agent`    | `cd examples/late_prompt_agent && go run main.go`    |

## Покрытие API

| Возможность                                    | Runnable example                                             | Unit tests                                                                                                                |
| ---------------------------------------------- | ------------------------------------------------------------ | ------------------------------------------------------------------------------------------------------------------------- |
| Declarative routing (`Completed` + edges)      | все examples                                                 | `builder_test.go`, `runner_test.go`                                                                                       |
| Resume overlay                                 | `hitl_agent`, `conditional_routing`, `subgraph_agent`        | `runner_lifecycle_test.go`                                                                                                |
| Stream + typed effects                         | `streaming_agent`                                            | `stream_test.go`                                                                                                          |
| Stream consumer helpers                        | `streaming_agent`, `middleware_agent`, `stream_request_stop` | `stream_helpers_test.go`, `stream_test.go`                                                                                |
| Handoff lifecycle                              | — (unit tests)                                               | `runner_handoff_contracts_test.go`, `compose_test.go`, `stream_test.go`                                                   |
| Lease / `WithRunLease` (in-memory happy path)  | `lease_agent`                                                | `runner_lifecycle_test.go`, `runner_reason_parity_test.go`                                                                |
| Policies (`DeleteOnSuccess`, `RetentionLimit`) | `lease_agent` (`WithDeleteOnSuccess(true)`)                  | `runner_lifecycle_test.go`                                                                                                |
| Typed `BindingKey` + `Bind`                    | `bindings_agent`                                             | `runner_lifecycle_test.go`                                                                                                |
| `WithResumeTargetPolicy`                       | `conditional_routing` (typed `ResumePlan` after overlay)      | `runner_lifecycle_test.go`, `runner_resume_overlay_test.go`                                                               |
| `WithRunMetadata`                              | —                                                            | `runner_lifecycle_test.go`, `stream_test.go` (Stream + ResumeStream)                                                      |
| `BudgetUsed` / `ContextWithRunMetadata`        | — (isolated node pattern)                                    | `run_options_test.go`                                                                                                     |
| `DeleteIfIdle` / lease-aware delete            | `lease_agent` (`WithDeleteOnSuccess` demo)                   | `runner_lifecycle_test.go`, adapter tests                                                                                 |
| Subgraph slot resume                           | `subgraph_slot_agent`                                        | `compose_test.go`                                                                                                         |
| `ResumeToken` + OCC                            | `hitl_agent`, `subgraph_slot_agent`                          | `runner_lifecycle_test.go`, `runner_resume_token_contracts_test.go`                                                       |
| `ResumeAt` on `Suspend` / `Handoff`            | —                                                            | `runner_resume_target_contracts_test.go`                                                                                  |
| `WithHandoffOutbox` (`HandoffIntent` + status) | —                                                            | `runner_handoff_contracts_test.go`, `runner_handoff_recovery_test.go`                                                     |
| `WithCheckpointErrorPolicy(SkipOnSaveError)`   | — (use Stream)                                               | `runner_checkpoint_policy_test.go`, `stream_test.go` (`TestStreamClosePersistVsEventDroppedTerminalEventSkipOnSaveError`) |
| Session guard / duplicate active run           | —                                                            | `runner_session_guard_test.go`                                                                                            |
| Reason parity (RunResult vs RunEvent)          | —                                                            | `runner_reason_parity_test.go`                                                                                            |
| Named budgets                                  | —                                                            | `runner_lifecycle_test.go`                                                                                                |
| Lease + checkpoint handoff (in-memory, unit)   | —                                                            | `runner_lifecycle_test.go`, `lease.go`                                                                                    |

Общие правила:

- Роутинг только через `AddEdge` / `AddConditionalEdge` с обязательными `allowedTargets` (без `Next(nodeID)`).
- Resume pipeline: `ResumeToken` validation → `Load` → **OCC** (`token.SnapshotRevision == snapshot.Revision`) → `AfterLoad` → `WithStateOverlay` → `resetSegmentCounters` → `WithRunMetadata` → `WithResumeTargetPolicy` returns `ResumePlan` → validate pointer → `Execute` from active `ExecutionPointer`. `DeleteIfIdle` / delete-on-success — после `releaseLease` (`postRunCleanup`). `Prune` (retention) — in-loop при suspend/handoff/cancel, до release.
- Lifecycle contracts: `ResumeAt`, typed `ResumePlan`, `HandoffIntent`, checkpoint `Record`, `EvaluateResume`, `EvaluateHandoffRecovery`, `RecoverStaleHandoff`, `WithCheckpointErrorPolicy(CheckpointPolicySkipOnSaveError)`.
- Handoff: `Handoff(reason)` или `RequestLocalHandoff` (только активный run **в том же процессе** runner); другой воркер — `Resume` + новый lease по checkpoint. Directive `Handoff()` skip-on-save-error → `(result, nil)` + `*_checkpoint_skipped`; `RequestLocalHandoff` skip → `ErrCheckpointSkipped`.
- Lease: `WithRunLease` обязателен при `WithLeaseManager`; повторный acquire → `ErrThreadLeaseBusy`. In-process duplicate → `ErrThreadAlreadyRunning`. Для `Stream`/`ResumeStream` дубликат виден на `Wait()` второго handle.
- Stream: `RequestStop()` + `WaitResult()`. Strict terminal-event tests требуют доставку event; persist-vs-event тесты (`*PersistVsEvent*`) допускают drop terminal event — authoritative terminal outcome is `WaitResult()`.
- `RequestLocalHandoff`: `nil` при persist, `ErrCheckpointSkipped` при skip-on-save-error, `ErrHandoffEnqueueFailed` при enqueue fail после persist.
- `handoff_outbox`: `WithHandoffOutbox` + `RecoverStaleHandoff` для фонового resume.

## Smoke-валидация

Список examples совпадает с [`examples_smoke_test.go`](../examples_smoke_test.go) (15 каталогов, `go run .`).

```bash
go test ./... -run TestExamplesSmoke -count=1
make test
```
