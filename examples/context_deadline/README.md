# Context deadline example

Демонстрация экстренного checkpoint при отмене контекста.

При `ctx.Done()` runner выполняет экстренный `Save` (через `context.WithoutCancel` + короткий deadline), чтобы не потерять прогресс долгого узла.

По умолчанию политика checkpoint — `HardFail`: при ошибке Save run завершается с ошибкой. С `WithCheckpointErrorPolicy(CheckpointPolicySkipOnSaveError)` snapshot может **не** сохраниться (`reason=context_canceled_checkpoint_skipped`); observable сигнал только на `Stream`/`ResumeStream`. На stream `EventCheckpointFailed.ExecutionPointer` совпадает с pointer в snapshot (после `WithSuspendPointerResolver` — resolved pointer).
