# Handoff Outbox

Демонстрирует `WithHandoffOutbox` и фоновый `Resume` по checkpoint.

## Сценарий 1 — happy path

- Foreground: `Handoff()` → Save `pending` → patch `enqueued` → `EnqueueIntent`.
- Outbox получает `HandoffIntent`: pending revision, committed revision, canonical resume revision, status, reason и execution pointer.
- Worker начинает с `EvaluateResume(intent.ResumeToken)` и использует decision/result token, если snapshot уже продвинулся.

## Сценарий 2 — stale pending recovery

`main.go` также показывает crash-window recovery:

1. Seed stale `pending` (`HandoffPendingAt` в прошлом).
2. `RecoverStaleHandoff` с `WithRecoverStaleAfter` → patch `enqueued` + enqueue.
3. Background `Resume` по `ResumeToken` из `HandoffRecoveryResult`.

`RecoverStaleHandoff` возвращает `HandoffRecoveryResult` + error. Result всегда несет typed `Decision`:

```go
result, err := runner.RecoverStaleHandoff(ctx, threadID)
if err != nil {
    switch result.Decision.Status {
    case flowy.ResumeDecisionHandoffPending:
        _ = result.Decision // fresh pending — retry later
    case flowy.ResumeDecisionHandoffAlreadyScheduled:
        // already delivered
    case flowy.ResumeDecisionHandoffNotRecoverable:
        // none/unknown status
    default:
        if errors.Is(err, flowy.ErrHandoffOutboxRequired) {
            // configure WithRunnerHandoffOutbox or WithRecoverOutbox
            return err
        }
        if errors.Is(err, flowy.ErrHandoffPatchFailed) && errors.Is(err, flowy.ErrConcurrencyConflict) {
            // cron retry: another worker won the OCC race
            return err
        }
        return err
    }
}
token := result.ResumeToken
```

Low-level errors remain `errors.Is`-compatible:

```go
if err != nil {
    switch {
    case errors.Is(err, flowy.ErrHandoffOutboxRequired):
        // configure WithRunnerHandoffOutbox or WithRecoverOutbox
    case errors.Is(err, flowy.ErrHandoffPatchFailed):
        // status patch failed — inspect cause
        if errors.Is(err, flowy.ErrConcurrencyConflict) {
            // cron retry: another worker won the OCC race
        }
    }
}
```

Recovery cron should retry on `ErrHandoffPatchFailed` when the cause is `ErrConcurrencyConflict` (another worker patched the snapshot first).

## Observability

Пример подключает lifecycle observer adapter для counters.

## Production notes

- Transactional adapters: используйте `TransactionalCheckpointer.SaveWithOutbox` + `TransactionalHandoffOutbox.EnqueueIntentTx(ctx, tx, intent)`; callback получает explicit transaction handle и saved revision, а core строит authoritative `HandoffIntent`.
- Non-transactional adapters: 3-phase FSM + `RecoverStaleHandoff` cron (single-leader или external lock).
- False `enqueued`: `RecoverStaleHandoff(..., WithRecoverForceReenqueue(true))`.

```bash
go run .
```
