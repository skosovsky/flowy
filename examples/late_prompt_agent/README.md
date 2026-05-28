# Late prompt agent

Демонстрация **late binding** политики промпта: граф стартует с минимальным state, узел `policy` фильтрует tools перед `llm`, маршрутизация через conditional edges.

## Запуск

```bash
cd examples/late_prompt_agent && go run main.go
```

## API в этом примере

- Typed state `AgentRunState` + `Completed()` routing.
- Узел `policy` применяет compile-time фильтр tools (без `Suspend`/`Resume` в `main.go`).
- Для checkpoint lifecycle (`Suspend`, `WithStateOverlay`) см. `hitl_agent` и `conditional_routing`.

## Тесты

```bash
cd examples/late_prompt_agent && go test .
```
