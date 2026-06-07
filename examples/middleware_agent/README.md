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

| Legacy                    | v2                            |
| ------------------------- | ----------------------------- |
| `ExecutionChain`          | `NodeMiddleware` + `Use(...)` |
| Локальные обёртки вручную | Onion-цепочка на compile      |

## Stream + panic

Второй прогон использует `Stream` + `CollectEventsAndWait` (безопасный drain + `Wait`): при panic в `unstable` ожидайте терминальное событие `failed` в потоке.

## Boxed OTel middleware

```go
import flowyotel "github.com/skosovsky/flowy/ext/otel"

flowyotel.InstallTelemetryBridge()
if err := flowyotel.InstallLifecycleObserver(); err != nil {
	log.Fatal(err)
}
graph, _ := flowy.NewGraph[State, flowy.NoEffect](reducer).
	Use(flowyotel.TracingMiddleware[State, flowy.NoEffect](tracer)).
	Compile()
```

`InstallLifecycleObserver` регистрирует counters `flowy.handoff_enqueued_total`, `flowy.resume_rejected_total`, `flowy.checkpoint_soft_error_total` (атрибуты `thread_id`, `node`, `status`/`reason`).
