# Conditional Routing / Semantic Cache

Демонстрация декларативного роутинга и late binding через `WithStateOverlay`.

## Сценарии

1. **Cache miss** — `check_cache` → `heavy_llm` → `output`.
2. **Cache hit** — `check_cache` → `output` (без LLM).
3. **Late binding** — overlay применяется до execute; `prepare` выполняется повторно с `AllowedTools`.
4. **State-aware resume target** — suspend на `prepare`, overlay с `SkipPrepare=true`, `WithResumeTargetPolicy` возвращает `ResumeTo("check_cache")` (узел `prepare` не выполняется повторно; `prepare_runs=1`).

## Запуск

```bash
cd examples/conditional_routing
go run main.go
```

## WithResumeTargetPolicy

`main.go` передает explicit policy в `Resume`: после overlay policy нормализует `AllowedTools` и при `SkipPrepare` возвращает typed `ResumePlan` для `check_cache` (сценарий 4). В обычном случае policy возвращает `ResumeCurrent()`.

## Current routing

| Старый подход                                 | Current API                                                  |
| --------------------------------------------- | ------------------------------------------------------------ |
| `Next("output")` / `Next("heavy_llm")` в узле | `Completed()` + `AddConditionalEdge(..., allowedTargets...)` |
| runtime-only ветвление                        | compile-time allowed targets + conditional router            |
