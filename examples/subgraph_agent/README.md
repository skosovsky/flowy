# Subgraph Agent

Демонстрирует сценарий, когда parent graph выполняет `SubgraphNode`, а child graph уходит в `Suspend`.

## Что показывает пример

1. Parent стартует и вызывает `subgraph`.
2. Child node `gate` делает `Suspend("waiting_subgraph_approval")`.
3. Parent получает `RunStatusSuspended` и сохраняет snapshot.
4. `Resume(..., WithStatePatch(...))` обновляет parent state (`Child.Approved=true`).
5. Parent снова входит в `subgraph`, child проходит `gate` и завершает run.

## Важный нюанс

Resume происходит на уровне parent-узла `subgraph`. Child должен быть restart-safe:
логика должна корректно отрабатывать при повторном входе с entrypoint, используя state.

## Запуск

```bash
cd examples/subgraph_agent
go run main.go
```
