# Streaming agent

Демонстрация **typed effects** и `Runner.Stream` для наблюдения за жизненным циклом графа.

## Запуск

```bash
cd examples/streaming_agent && go run main.go
```

## Pattern

- Граф: `Graph[AgentState, AgentEffect]` с `Effect(Completed(), payload)`.
- Запуск: `runner.Stream(ctx, threadID, initialState)` → канал `RunEvent[T,E]`.
- События в этом примере: `node_started`, `node_completed` (с typed `Effect`), `suspended`.
- Handoff/cancel semantics: см. `runner_lifecycle_test.go` (`EventHandoff`, `EventContextCanceled`).

## Use case

Chat/completion UI: stream событий узлов и typed effects; пауза через `Suspend` и продолжение через `Resume`.
