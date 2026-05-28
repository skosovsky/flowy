# ReAct-style agent (Reason + Act)

Минимальный **ReAct** loop через `patterns.BuildReAct`: reason → action → reason, пока `Done` не станет true.

## Запуск

```bash
cd examples/react_agent && go run main.go
```

## Graph structure (фактический код)

- **react_reason** — добавляет thought, выставляет `Done`, возвращает `Completed()`.
- **react_action** — симулирует tool call, возвращает `Completed()`.
- Conditional edge после reason: пока `!Done` → `react_action`, иначе `EndNode`.
- **react_action** использует `Retry(maxReActSteps)` + `AddRetryRoute` → `EndNode` при исчерпании budget (`ErrRetryBudgetExceeded`).

Глобальный лимит шагов: `flowy.WithMaxSteps` на `Compile()` (в patterns по умолчанию 64).

## Protection against infinite loops

Цикл reason ↔ action ограничен `Retry` budget на action-узле и global `maxSteps`. Для tool-failure отдельно можно добавить `AddRetryRoute` на кастомный fallback-узел.

Routing: `Completed()` + declarative edges. Lifecycle/lease: `runner_lifecycle_test.go`, `examples/README.md`.
