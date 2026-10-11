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

На каждой границе parent получает только новые effects: slot хранит `ExportedEffects`. Completion очищает slot; повторный вход начинает новое исполнение. Непустой slot требует `Contract == flowy.InlineSlotContract`; старые slots отклоняются до запуска. Переход требует завершения старых executions либо явной offline-конверсии host с доказанным курсором экспорта.

Контекстные bindings доступны внутри; activity/child/lease capabilities parent не передаются. Durable inline запуск отклоняется до dispatch, для него нужен distributed child. Mutable state и effects требуют detachment в host mapping/storage.
