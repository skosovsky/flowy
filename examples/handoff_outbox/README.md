# Handoff Outbox

Демонстрирует `WithHandoffOutbox` и фоновый `Resume` по checkpoint.

## Сценарий 1 — happy path

- Foreground: `Handoff()` → Save `pending` → `EnqueueIntent` → patch `enqueued`.
- Outbox-токен несёт revision pending-снимка; `RunResult.ResumeToken` — revision после patch.
- Worker: `Load` → свежий `ResumeToken` → `Resume`.

## Сценарий 2 — stale pending recovery

`main.go` также показывает crash-window recovery:

1. Seed stale `pending` (`HandoffPendingAt` в прошлом).
2. `RecoverStaleHandoff` с `WithRecoverStaleAfter` → enqueue + patch `enqueued`.
3. Background `Resume` по свежему revision из `Load`.

`RecoverStaleHandoff` отклоняет **свежий** `pending` (`ErrHandoffPending`), уже доставленный `enqueued` (`ErrHandoffAlreadyEnqueued`), пустой/none статус (`ErrHandoffNotRecoverable`) и вызов без outbox (`ErrHandoffOutboxRequired`). Обрабатывайте через `errors.Is`:

```go
if err := runner.RecoverStaleHandoff(ctx, threadID); err != nil {
    switch {
    case errors.Is(err, flowy.ErrHandoffPending):
        // fresh pending — retry later
    case errors.Is(err, flowy.ErrHandoffAlreadyEnqueued):
        // already delivered
    case errors.Is(err, flowy.ErrHandoffNotRecoverable):
        // none/unknown status
    case errors.Is(err, flowy.ErrHandoffOutboxRequired):
        // configure WithRunnerHandoffOutbox or WithRecoverOutbox
    case errors.Is(err, flowy.ErrHandoffPatchFailed):
        // enqueue OK but status patch failed — inspect cause
        if errors.Is(err, flowy.ErrConcurrencyConflict) {
            // cron retry: another worker won the OCC race
        }
    default:
        return err
    }
}
```

Recovery cron should retry on `ErrHandoffPatchFailed` when the cause is `ErrConcurrencyConflict` (another worker patched the snapshot first).

## Observability

Пример вызывает `ext/otel.InstallLifecycleObserver()` для lifecycle counters.

## Production notes

- Postgres: используйте `TransactionalCheckpointer.SaveWithOutbox` + outbox INSERT через `postgres.PgxTxFromContext`.
- Redis/memory: 3-phase FSM + `RecoverStaleHandoff` cron (single-leader или `WithRunLease`).
- False `enqueued`: `RecoverStaleHandoff(..., WithRecoverForceReenqueue(true))`.

```bash
go run .
```
