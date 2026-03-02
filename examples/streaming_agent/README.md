# Streaming agent

This example combines **flowy execution** with **out-of-band streaming** (e.g. LLM token stream). The graph runs synchronously with `Invoke`, while a node pushes tokens into a channel that is read by another goroutine.

## Pattern

- State flows through the graph as usual (e.g. string).
- A **channel** (or callback) is passed via `context.WithValue`. The node `call_llm` reads it from the context and, while it "calls the LLM", pushes tokens into that channel. Another goroutine drains the channel and prints (or sends to the client).
- So you get: graph progress (and final state) from `Invoke`, and a separate stream of tokens from the channel. For **event-level** observation of the graph (node start/end, interrupt, errors), use `flowy.Stream(ctx, state)` instead of `Invoke`; you can combine both (Stream for graph events, context channel for token stream).

## Use case

Typical in chat or completion UIs: the graph drives the pipeline (e.g. retrieve → LLM → format), and the LLM node streams tokens over a channel so the UI can display them as they arrive while the graph is still running.
