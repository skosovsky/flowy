# Subgraph Slot Agent

Демонстрация `SubgraphNodeWithSlot` для suspend/resume вложенного графа.

## Сценарий

1. Parent graph вызывает child subgraph.
2. Child suspend — cursor сохраняется в `SubgraphSlot` parent state.
3. Parent `Resume` — child продолжает с сохранённого `ExecutionPointer`.

Вложенный runner **не** наследует parent `RunOptions` (`WithRunMetadata`, `WithBindings`, `WithRunLease`).

## Запуск

```bash
cd examples/subgraph_slot_agent
go run .
```
