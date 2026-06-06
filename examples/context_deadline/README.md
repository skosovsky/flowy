# Context Deadline

Показывает отмену run через `context.WithTimeout` и сохранение snapshot до выхода.

## Поведение runtime

При `ctx.Done()` runner выполняет экстренный `Save` (через `context.WithoutCancel` + короткий deadline), чтобы не потерять прогресс долгого узла.

По умолчанию политика checkpoint — `HardFail`: при ошибке Save run завершается с ошибкой. С `WithCheckpointErrorPolicy(SoftWarn)` snapshot может **не** сохраниться (`reason=context_canceled_checkpoint_skipped`); observable сигнал только на `Stream`/`StreamResume`.

## Запуск

```bash
cd examples/context_deadline
go run main.go
```

Ожидайте `status=context_canceled`, `reason=context_canceled`, ошибку deadline и ненулевой `ticks` в checkpoint.

## Миграция

| Legacy demo        | v2 cookbook  |
| ------------------ | ------------ |
| `context_deadline` | этот каталог |
