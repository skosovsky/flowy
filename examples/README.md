# Flowy v1 Cookbook

Runnable-примеры на директивном API (`Node`, `Directive`, `Runner.Start/Resume/Stream`). Legacy-символы (`ErrSuspend`, `Invoke`, старый stream) не используются.

## Матрица сценариев

| Сценарий                          | Каталог               | Команда                                             |
| --------------------------------- | --------------------- | --------------------------------------------------- |
| ReAct loop                        | `react_agent`         | `cd examples/react_agent && go run main.go`         |
| Stream + Effect + Suspend         | `streaming_agent`     | `cd examples/streaming_agent && go run main.go`     |
| Human-in-the-Loop                 | `hitl_agent`          | `cd examples/hitl_agent && go run main.go`          |
| Middleware + panic recovery       | `middleware_agent`    | `cd examples/middleware_agent && go run main.go`    |
| Supervisor / multi-agent          | `multi_agent`         | `cd examples/multi_agent && go run main.go`         |
| Context deadline + emergency save | `context_deadline`    | `cd examples/context_deadline && go run main.go`    |
| Cache routing + late binding      | `conditional_routing` | `cd examples/conditional_routing && go run main.go` |
| Parent + subgraph suspend/resume  | `subgraph_agent`      | `cd examples/subgraph_agent && go run main.go`      |

## Migration map (legacy → v1)

| Legacy example         | v1 cookbook           | Ключевая замена                                                |
| ---------------------- | --------------------- | -------------------------------------------------------------- |
| `hitl_agent`           | `hitl_agent`          | `ErrSuspend` → `Suspend()` + `Resume(WithStatePatch(...))`     |
| `middleware_agent`     | `middleware_agent`    | execution chain → `Use(..., RecoverMiddleware)`                |
| `multi_agent`          | `multi_agent`         | ручная маршрутизация → `patterns.BuildSupervisor` + `RouteMap` |
| `context_deadline`     | `context_deadline`    | отмена ctx → emergency `Save` + `RunStatusSuspended`           |
| `semantic_cache_agent` | `conditional_routing` | ветвление → `Next("output")` / `Next("heavy_llm")`             |
| `late_prompt_agent`    | `conditional_routing` | runtime config → `WithStatePatch` на `Resume`                  |
| `subgraph_agent`       | `subgraph_agent`      | parent `SubgraphNode` + suspend/resume через parent snapshot   |

## Smoke-валидация

```bash
for d in react_agent streaming_agent hitl_agent middleware_agent multi_agent context_deadline conditional_routing subgraph_agent; do
  (cd "examples/$d" && go run main.go) || exit 1
done
go test ./...
```
