# Subgraph Agent

Демонстрирует сценарий, когда parent graph выполняет `SubgraphNode`, а child graph уходит в `Suspend`.

## Что показывает пример

1. Parent стартует и вызывает `subgraph`.
2. Child node `gate` делает `Suspend("waiting_subgraph_approval")`.
3. Parent получает `RunStatusSuspended` и сохраняет snapshot.
4. `Resume(ctx, first.ResumeToken, WithStateOverlay(...))` обновляет parent state (`Child.Approved=true`).
5. Parent снова входит в `subgraph`, child проходит `gate` и завершает run.

## Важный нюанс

`SubgraphNode` (как в этом примере) поднимает child state в parent и при resume снова стартует subgraph с entrypoint — child должен быть restart-safe.

Для resume **внутри** child без повторного entrypoint используйте `SubgraphNodeWithSlot` + поле `SubgraphSlot` в parent state (см. `compose_test.go`, `TestSubgraphHandoffResumeContinuity`).

## Запуск

```bash
cd examples/subgraph_agent
go run main.go
```
