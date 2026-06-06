# Conditional Routing / Semantic Cache

Демонстрация v2 декларативного роутинга и late binding через `WithStateOverlay`.

## Сценарии

1. **Cache miss** — `check_cache` → `heavy_llm` → `output`.
2. **Cache hit** — `check_cache` → `output` (без LLM).
3. **Late binding** — `Resume(..., WithStateOverlay(...))` добавляет `AllowedTools` до выполнения узлов.

## Запуск

```bash
cd examples/conditional_routing
go run main.go
```

## ResumableState.Reconcile

`agentState` реализует `ResumableState`: после overlay `Reconcile()` нормализует `AllowedTools` перед execute (см. `main.go`).

## v2 routing

| Старый подход                                 | v2                                                           |
| --------------------------------------------- | ------------------------------------------------------------ |
| `Next("output")` / `Next("heavy_llm")` в узле | `Completed()` + `AddConditionalEdge(..., allowedTargets...)` |
| runtime-only ветвление                        | compile-time allowed targets + conditional router            |
