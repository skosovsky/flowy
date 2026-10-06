# Multi-Agent Supervisor

Оркестрация через `patterns.BuildDispatchGraph`: dispatch выбирает terminal worker по `Intent` и `RouteMap`.

## Граница ответственности

- Supervisor маршрутизует, workers выполняют специализированную работу.
- State остаётся type-safe в одном `teamState` — без `map[string]any`.

## Запуск

```bash
cd examples/multi_agent
go run main.go
```

## Миграция

| Старый подход | Current API                             |
| ------------- | --------------------------------------- |
| `multi_agent` | `patterns.BuildDispatchGraph` + `RouteMap` |
