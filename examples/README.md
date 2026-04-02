# Examples

Runnable programs under this directory use `flowy` from the repo root (`go run ./examples/<name>` from the module root).

## Timeouts and resilience

`flowy` does not set per-node deadlines. For patterns (macro/micro `context`, optional [routery](https://github.com/skosovsky/routery)), see the root [README.md](../README.md) section **Timeouts and resilience**.

- [context_deadline](./context_deadline/main.go) — short **stdlib** sample: caller wraps `Invoke` with `context.WithTimeout`.

## Other examples

- [hitl_agent](./hitl_agent/main.go) — checkpoint-backed Human-in-the-Loop resume
- [middleware_agent](./middleware_agent/main.go) — logging, memory, fallback middleware
- [react_agent](./react_agent/main.go) — small ReAct loop
- [semantic_cache_agent](./semantic_cache_agent/main.go) — semantic-cache short-circuiting
- [streaming_agent](./streaming_agent/main.go) — streaming steps
- [subgraph_agent](./subgraph_agent/main.go) — subgraph composition
- [multi_agent](./multi_agent/main.go) — multi-agent wiring
