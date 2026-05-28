# Flowy Cookbook

Runnable-примеры на API `Graph[T,E]`, `Runner.Start/Resume/Stream`, декларативного роутинга (`Completed` + edges) и typed effects.

## Матрица сценариев

| Сценарий                          | Каталог                | Команда                                              |
| --------------------------------- | ---------------------- | ---------------------------------------------------- |
| ReAct loop                        | `react_agent`          | `cd examples/react_agent && go run main.go`          |
| Stream + typed Effect + Suspend   | `streaming_agent`      | `cd examples/streaming_agent && go run main.go`      |
| Human-in-the-Loop                 | `hitl_agent`           | `cd examples/hitl_agent && go run main.go`           |
| Middleware + panic recovery       | `middleware_agent`     | `cd examples/middleware_agent && go run main.go`     |
| Supervisor / multi-agent          | `multi_agent`          | `cd examples/multi_agent && go run main.go`          |
| Context deadline + emergency save | `context_deadline`     | `cd examples/context_deadline && go run main.go`     |
| Cache routing + late binding      | `conditional_routing`  | `cd examples/conditional_routing && go run main.go`  |
| Parent + subgraph suspend/resume  | `subgraph_agent`       | `cd examples/subgraph_agent && go run main.go`       |
| Semantic cache routing            | `semantic_cache_agent` | `cd examples/semantic_cache_agent && go run main.go` |
| Late prompt policy (compile-time) | `late_prompt_agent`    | `cd examples/late_prompt_agent && go run main.go`    |

## Покрытие API

| Возможность                                    | Runnable example                                      | Unit tests                                                                                                         |
| ---------------------------------------------- | ----------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------ |
| Declarative routing (`Completed` + edges)      | все examples                                          | `builder_test.go`, `runner_test.go`                                                                                |
| Resume overlay                                 | `hitl_agent`, `conditional_routing`, `subgraph_agent` | `runner_lifecycle_test.go`                                                                                         |
| Stream + typed effects                         | `streaming_agent`                                     | `stream_test.go`                                                                                                   |
| Handoff lifecycle                              | —                                                     | `runner_lifecycle_test.go`, `compose_test.go`, `stream_test.go` (`TestStreamHandoffToBackgroundEmitsHandoffEvent`) |
| Lease / `ErrLeaseLost` / TTL takeover          | —                                                     | `runner_lifecycle_test.go`                                                                                         |
| Policies (`DeleteOnSuccess`, `RetentionLimit`) | —                                                     | `runner_lifecycle_test.go`                                                                                         |
| `WithResumeReconciler`                         | —                                                     | `runner_lifecycle_test.go`                                                                                         |
| Named budgets                                  | —                                                     | `runner_lifecycle_test.go`                                                                                         |
| Lease + checkpoint handoff (in-memory, unit)   | —                                                     | `runner_lifecycle_test.go`, `lease.go`                                                                             |

Общие правила:

- Роутинг только через `AddEdge` / `AddConditionalEdge` с обязательными `allowedTargets` (без `Next(nodeID)`).
- Resume pipeline: `Load` → опционально `WithStateOverlay` → опционально `WithResumeReconciler` → `Execute`.
- Handoff: `Handoff(reason)` или `HandoffToBackground` (только активный run **в том же процессе** runner); другой воркер — `Resume` + новый lease по checkpoint.
- Lease: `WithRunLease` обязателен при `WithLeaseManager`; повторный `Start` на занятый thread → `ErrThreadBusy`; после TTL другой owner может `Acquire`.

## Smoke-валидация

```bash
for d in react_agent streaming_agent hitl_agent middleware_agent multi_agent context_deadline conditional_routing subgraph_agent semantic_cache_agent late_prompt_agent; do
  (cd "examples/$d" && go run main.go) || exit 1
done
go test ./...
golangci-lint run ./...
```
