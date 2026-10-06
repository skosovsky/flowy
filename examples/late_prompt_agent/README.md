# Late prompt agent

Демонстрация **late binding** политики промпта: узел `policy` фильтрует tools во время исполнения перед `llm`. Маршрут `policy` → `llm` объявлен через `AddEdge`; model/tool вызовы представлены host stubs.

## Запуск

```bash
cd examples/late_prompt_agent && go run main.go
```

## API в этом примере

- Typed state `AgentRunState` + `Completed()` routing.
- Узел `policy` фильтрует state tools при каждом исполнении; `Compile()` фиксирует граф, но не проверяет бизнес-права на tools. `Suspend`/`Resume` в `main.go` не используются.
- Для checkpoint lifecycle (`Suspend`, `WithStateOverlay`) см. `hitl_agent` и `conditional_routing`.

## Тесты

```bash
cd examples/late_prompt_agent && go test .
```
