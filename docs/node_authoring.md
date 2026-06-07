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
- **`BeginStreamCollect` + `AwaitStreamCollect`** — background drain; caller triggers `RequestStop`, handoff, or ctx cancel, then awaits. Use `AwaitStreamCollectWithSnapshot` for Handoff/HITL (`ResumeToken` from checkpoint).

Persist-vs-event: when the consumer stops mid-run, the terminal `RunEvent` may be dropped; **`Wait()`** and **checkpoint snapshot** are authoritative for terminal reason/state.

See [`examples/stream_request_stop`](../examples/stream_request_stop) and [`examples/streaming_agent`](../examples/streaming_agent).
