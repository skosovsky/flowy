# Flowy

Flowy — production-ready framework для управляемых agentic state machine на Go.

## Core Concept: The Power of `[T any]`

`flowy` не знает структуру состояния. Всё приложение передаёт через generic `T`:

```go
type Node[T any] func(ctx context.Context, state T) (T, Directive, error)
```

Это даёт type-safe интеграцию без рефлексии и без привязки к конкретным доменным моделям.

## Directives vs String Routing

Узлы возвращают директивы платформы:

- `flowy.Completed()`
- `flowy.Next(nodeID)`
- `flowy.End()`
- `flowy.Suspend(reason)`
- `flowy.Retry(maxAttempts, fallbackNode)`
- `flowy.Effect(baseDirective, payload)`

```go
b.AddNode("collect", func(ctx context.Context, s State) (State, flowy.Directive, error) {
	if s.WaitUser {
		return s, flowy.Suspend("wait_user_input"), nil
	}
	return s, flowy.Completed(), nil
})
b.AddEdge("collect", "process")
// or dynamic route:
b.AddConditionalEdge("collect", func(ctx context.Context, s State) (string, error) {
	if s.NeedsReview {
		return "review", nil
	}
	return flowy.EndNode, nil
})
```

## Agentic Patterns Guide

Пакет `flowy/patterns` содержит готовые builder-функции:

- `patterns.BuildReAct(...)`
- `patterns.BuildSupervisor(...)`
- `patterns.BuildEvaluatorOptimizer(...)`

Пример ReAct:

```go
b := patterns.BuildReAct(
	reasonNode,
	actionNode,
	func(s AgentState) bool { return s.HasPendingActions },
	8,
)
g, _ := b.Compile()
```

Пример Supervisor:

```go
b := patterns.BuildSupervisor(
	supervisorNode,
	workerNodes,
	func(s AgentState) string { return s.Intent },
	patterns.RouteMap{
		"sales":   "sales_worker",
		"support": "support_worker",
	},
)
```

## Persistence & Resume

`Runner` инкапсулирует жизненный цикл:

```go
runner := graph.NewRunner(checkpointer, interceptors...)
res, err := runner.Start(ctx, "thread-1", initialState)
res, err = runner.Resume(ctx, "thread-1", flowy.WithStatePatch(func(s *State) {
	s.LastInput = "updated"
}))

stream, err := runner.Stream(ctx, "thread-1", initialState)
for event := range stream.Events() {
	_ = event
}
_ = stream.Done()
```

При `Suspend` и `ctx.Done()` runner гарантированно сохраняет `Snapshot` через `Checkpointer`.

## Storage Layer

```go
type Checkpointer[T any] interface {
	Save(ctx context.Context, snapshot Snapshot[T]) error
	Load(ctx context.Context, threadID string) (Snapshot[T], error)
	GetHistory(ctx context.Context, threadID string, limit int) ([]Snapshot[T], error)
	Prune(ctx context.Context, threadID string, retainCount int) error
}
```

`Snapshot[T]` хранит:
- `State`
- `NodeID`
- `Revision` (append-only logical clock в рамках `thread_id`)
- `RunMeta` (`StepCount`, `RetryCounts`, `SegmentStartTime`)
- `Effects` для внешней обработки side effects.
- `RunMeta.TelemetryContext` для trace continuity через `Suspend/Resume`.

`RunEvent` содержит observability-поля:
- `Duration` (автоматический runtime-замер длительности узла)
- `Metrics` (из payload `Effect`: поддерживаются `map[string]any` и сериализуемые struct)

Политика terminal events:
- `EventNodeCompleted` содержит `Duration` узла
- terminal events (`completed`/`suspended`/`failed`) отдаются с `Duration=0`

## OpenTelemetry Integration

Опциональный пакет `flowy/ext/otel` дает готовый middleware и bridge для переносимости trace context:

```go
import flowyotel "github.com/skosovsky/flowy/ext/otel"

flowyotel.InstallTelemetryBridge()
graph, _ := flowy.NewGraph(reducer).
	Use(flowyotel.TracingMiddleware[State](tracer)).
	Compile()
```

Для сериализации состояния используется `StateSerializer[T]`. В пакете `checkpoint` есть `JSONSerializer` и `WithSanitizer`.

## Quality Gates

Базовая валидация:

```bash
go test ./...
```

Покрытие stream runtime (требование task14: >90%):

```bash
go test ./... -coverprofile=coverage.out
go tool cover -func=coverage.out | rg "runner.go:.*Stream|runner.go:.*startStream"
```

Core coverage gate по DoD task14 (целевой ориентир: 100% для core-пакета) должен проверяться в CI отдельным шагом через `-coverpkg` и пороговую проверку.
