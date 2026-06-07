# Streaming agent

Демонстрация **typed effects** и `Runner.Stream` для наблюдения за жизненным циклом графа.

## Запуск

```bash
cd examples/streaming_agent && go run main.go
```

## Pattern

- Граф: `Graph[AgentState, AgentEffect]` с `Effect(Completed(), payload)`.
- Запуск: `runner.Stream(ctx, threadID, initialState)` → `ConsumeEventsAndWait` с callback (`printEvent`); return `false` из callback → `RequestStop` + silent drain.
- События в этом примере: `node_started`, `node_completed` (с typed `Effect`), `suspended`.
- Handoff/cancel semantics: см. `runner_lifecycle_test.go` (`EventHandoff`, `EventContextCanceled`).
- Anti-deadlock consumer: см. `examples/stream_request_stop` (`BeginStreamCollect` + `RequestStop`).

## Use case

Chat/completion UI: stream событий узлов и typed effects; пауза через `Suspend` и продолжение через `Resume`.
