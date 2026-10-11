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
- Wrapper **react_action** преобразует `Completed()` в `Retry(maxActionRetries)` и направляет исполнение через `AddRetryRoute("react_action", "react_reason")`. При исчерпании retry budget выполнение завершается с `ErrRetryBudgetExceeded`; fallback не вызывается.

maxActionRetries ограничивает повторные action attempts: максимум maxActionRetries+1 вызовов action, включая первую попытку. Это не total graph-step limit.

Лимит вызовов handler на один compute segment: `flowy.WithMaxSteps` на `Compile()` (по умолчанию 1000; Resume начинает новый segment).

## Protection against infinite loops

Цикл reason ↔ action ограничен `Retry` budget на action-узле и segment `maxSteps`. Route используется пока retry разрешён; изменение `AddRetryRoute` меняет этот маршрут, а не обработчик исчерпания лимита. Node errors возвращаются вызывающему коду; host определяет retry policy для реальных tool calls.

Routing: `Completed()` + declarative edges. Lifecycle/lease: `runner_lifecycle_test.go`, `examples/README.md`.
