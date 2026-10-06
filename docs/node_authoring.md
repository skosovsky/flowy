# Node authoring guide

Graph nodes receive a `context.Context` that the runner cancels on parent deadline, lease loss, `RequestStop`, or `RequestLocalHandoff`. Nodes must respect this context or the run may hang until an external timeout.

## Rules

1. **Pass `ctx` to all I/O** — HTTP, gRPC, database, message queues, and `time.Sleep` alternatives (`time.NewTimer` + `select` on `ctx.Done()`).
2. **Loops must be cancellable** — use `select { case <-ctx.Done(): return ... }` in long or infinite loops.
3. **Do not ignore `ctx` in blocking calls** — a node that blocks without watching `ctx.Done()` cannot be stopped by `RequestStop`.

## Canonical pattern

See [`examples/context_deadline/main.go`](../examples/context_deadline/main.go):

```go
func slowNode(ctx context.Context, s workState) (workState, flowy.Directive, error) {
    ticker := time.NewTicker(tickInterval)
    defer ticker.Stop()

    for {
        select {
        case <-ctx.Done():
            return s, flowy.Completed(), nil
        case <-ticker.C:
            s.Ticks++
        }
    }
}
```

## Stream consumer side

Node context is separate from stream consumer patterns. When consuming `StreamHandle.Events()`:

- **`CollectEventsAndWait`** — collect all events, then `Wait()` (simple run-to-completion).
- **`ConsumeEventsAndWait`** — callback per event; return `false` to `RequestStop` and silently drain remaining events (no further callback invocations). See [`examples/streaming_agent`](../examples/streaming_agent) for a live callback consumer.
- **`BeginStreamCollect` + `AwaitStreamCollect`** — background drain; caller triggers `RequestStop`, handoff, or ctx cancel, then awaits terminal `Outcome`. Use `AwaitStreamCollectWithSnapshot` only when you also need diagnostic snapshot data; `Outcome.ResumeToken` remains authoritative.

Persist-vs-event: when the consumer stops mid-run, the terminal `RunEvent` may be dropped; **`WaitResult()`** is authoritative for terminal reason/state, while checkpoint snapshot is the durable resume/recovery artifact.

See [`examples/stream_request_stop`](../examples/stream_request_stop) and [`examples/streaming_agent`](../examples/streaming_agent).

## Handoff recovery

Handoff checkpoints persist `RunMetadata.HandoffStatus`: `pending` → `enqueued` before enqueue, then `orphaned` only when enqueue fails and orphan patch succeeds. Terminal `EventHandoff` uses `ReasonHandoffOrphaned` only when persisted status is `orphaned`; if orphan patch fails, status stays `enqueued` and reason remains the directive (e.g. `bg`). Outboxes receive `HandoffIntent`, not a bare token; use its pending/committed/canonical revision fields instead of deriving generation semantics in application code. Transactional checkpointers skip `pending` for initial handoff only when the outbox implements `TransactionalHandoffOutbox`; `RecoverStaleHandoff` always uses the 3-phase FSM (patch enqueued, enqueue, optional orphan patch). Use `Runner.EvaluateResume` / `Runner.EvaluateHandoffRecovery` for typed preflight decisions instead of inspecting raw snapshot state. Pending checkpoints with empty `HandoffPendingAt` are treated as stale immediately. Do not call `Resume`/`ResumeStream` while status is `pending` or `orphaned` — use `Runner.RecoverStaleHandoff` for orphaned or stale pending (default TTL 5m via `WithHandoffStaleAfter`) and read `HandoffRecoveryResult.Decision` / `ResumeToken`. `WithRecoverForceReenqueue(true)` forces re-enqueue of false `enqueued`. Recovery workers should be single-leader or protected by an external lock; `RecoverStaleHandoff` itself does not acquire run leases. Do not manually `Save` with `HandoffStatus` in application code — use the runner FSM.


## Mutable values and limits

Treat state and effects as immutable after returning or sharing them. For mutable reference-valued state/effects in streaming, pass WithEventCloners with pure host deep-copy functions on every Stream/ResumeStream invocation. Consumer changes then cannot modify the running graph, and later nodes cannot change earlier events. Snapshot ownership belongs to the checkpointer; use MemoryCheckpointer's explicit cloners when testing mutable domain types. Codec Unmarshal must return detached values. Do not mutate run metadata from concurrent host goroutines.

MaxSteps counts admitted handler calls per segment, including errors and retries, and stops before the next disallowed call. Resume begins a new segment. UseBudget reports actual host units and validates its input; it is not a spending reservation. Check its returned error. Undeclared valid names can be recorded, but only declared limits are enforced after a successful node. Inline subgraphs have separate accounting and cannot use the parent durable activity/child/lease boundary; use structured children for durable nested work.

A stream buffer can drop progress and terminal events while a consumer is slow or absent. This never blocks execution; use WaitResult for the outcome. A blocking callback in a consumer helper remains the consumer's responsibility.
