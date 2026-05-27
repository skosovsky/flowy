# Middleware Agent

Показывает глобальные node middleware через `GraphBuilder.Use(...)`.

## Слои

- `loggingMiddleware` — замер длительности каждого узла (шаблон для Langfuse/OpenTelemetry).
- `flowy.RecoverMiddleware` — panic в узле превращается в ошибку, процесс не падает.

## Запуск

```bash
cd examples/middleware_agent
go run main.go
```

## Миграция с legacy

| Legacy                    | v1                            |
| ------------------------- | ----------------------------- |
| `ExecutionChain`          | `NodeMiddleware` + `Use(...)` |
| Локальные обёртки вручную | Onion-цепочка на compile      |

## Stream + panic

Второй прогон использует `Stream`: при panic в `unstable` ожидайте терминальное событие `failed` в потоке.

## Boxed OTel middleware

```go
import flowyotel "github.com/skosovsky/flowy/ext/otel"

flowyotel.InstallTelemetryBridge()
graph, _ := flowy.NewGraph(reducer).
	Use(flowyotel.TracingMiddleware[State](tracer)).
	Compile()
```
