# Middleware Agent

Показывает глобальные node middleware через `GraphBuilder.Use(...)`.

## Слои

- `loggingMiddleware` — замер длительности каждого узла (шаблон для tracing adapters).
- `flowy.RecoverMiddleware` — panic в узле превращается в ошибку, процесс не падает.

## Запуск

```bash
cd examples/middleware_agent
go run main.go
```

## Миграция

| Старый подход             | Current API                   |
| ------------------------- | ----------------------------- |
| `ExecutionChain`          | `NodeMiddleware` + `Use(...)` |
| Локальные обёртки вручную | Onion-цепочка на compile      |

## Stream + panic

Второй прогон использует `Stream` + `CollectEventsAndWait` (безопасный drain + `Wait`): при panic в `unstable` ожидайте терминальное событие `failed` в потоке.

## Adapter middleware

Middleware adapters should stay outside state and snapshot contracts: install them at graph build time with `Use(...)`, keep persisted state type-owned by the application, and avoid storing telemetry/session objects in `Snapshot`.
