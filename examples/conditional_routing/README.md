# Conditional Routing / Semantic Cache

Замена legacy `semantic_cache_agent` и `late_prompt_agent`.

## Сценарии

1. **Cache miss** — `check_cache` → `heavy_llm` → `output`.
2. **Cache hit** — `check_cache` → `output` (без LLM).
3. **Late binding** — `Start(..., WithStatePatch(...))` добавляет `AllowedTools` до выполнения узлов (аналог runtime prompt config).

## Запуск

```bash
cd examples/conditional_routing
go run main.go
```

## Миграция

| Legacy                 | v1                                      |
| ---------------------- | --------------------------------------- |
| `semantic_cache_agent` | `Next("output")` vs `Next("heavy_llm")` |
| `late_prompt_agent`    | `WithStatePatch` на `Start`/`Resume`    |
