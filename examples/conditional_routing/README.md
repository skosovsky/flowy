# Conditional Routing / Semantic Cache

Демонстрация v2 декларативного роутинга и late binding через `WithStateOverlay`.

## Сценарии

1. **Cache miss** — `check_cache` → `heavy_llm` → `output`.
2. **Cache hit** — `check_cache` → `output` (без LLM).
3. **Late binding** — overlay применяется до execute; `prepare` выполняется повторно с `AllowedTools` (без rewind). Для пропуска stale-узла — сценарий 4.
4. **Pointer rewind** — suspend на `prepare`, overlay с `SkipPrepare=true`, `ReconcileResume("prepare")` → `check_cache` (узел `prepare` не выполняется повторно; `prepare_runs=1`).

## Запуск

```bash
cd examples/conditional_routing
go run main.go
```

## ResumeReconciler.ReconcileResume

`routeState` реализует `ResumeReconciler`: после overlay `ReconcileResume` нормализует `AllowedTools` и при `SkipPrepare` перематывает с `prepare` на `check_cache` (сценарий 4 в `main.go`).

## v2 routing

| Старый подход                                 | v2                                                           |
| --------------------------------------------- | ------------------------------------------------------------ |
| `Next("output")` / `Next("heavy_llm")` в узле | `Completed()` + `AddConditionalEdge(..., allowedTargets...)` |
| runtime-only ветвление                        | compile-time allowed targets + conditional router            |
