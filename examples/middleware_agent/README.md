# Middleware agent

This example demonstrates three classic middleware scenarios in one graph:

- `LoggingMiddleware` measures node execution time with `time.Since`
- `MemoryMiddleware` stores the successful state for each node in an in-memory map
- `FallbackMiddleware` wraps one unstable node and calls a fallback handler with the original state

The important fallback rule is explicit in the code: when `next(...)` returns an error, the fallback uses the incoming `state`, not the failed node output, so partially dirty data is never committed to the graph.
