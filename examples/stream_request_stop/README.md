# Stream RequestStop (anti-deadlock)

Demonstrates the safe consumer pattern when stopping a stream from another goroutine:

1. `BeginStreamCollect(handle)` — background drain + `Wait`
2. `RequestStop` — cancel run and close event sink
3. `AwaitStreamCollect(ctx, handle, out)` — join without deadlock
4. `AwaitStreamCollectWithSnapshot(ctx, handle, out, cp, threadID)` — load checkpoint + `ResumeToken` for Handoff/HITL

Run:

```bash
cd examples/stream_request_stop && go run main.go
```

For simple “run to completion” streams use `CollectEventsAndWait` or `ConsumeEventsAndWait` instead.
