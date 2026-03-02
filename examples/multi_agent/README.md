# Multi-agent composition

This example shows **composition** of two graphs: an "analyst" graph is embedded inside a "seller" graph as a single node. Context (state) flows from the outer graph into the inner one and back.

## Roles

- **Analyst graph** — one node `research`: takes a query from state, produces a research report, writes it into `state.ResearchData`. Entry and finish are both `research`.
- **Seller graph** — one node `ask_analyst` that is the analyst graph wrapped as a node via `AsNode()`. So when the seller runs, it invokes the analyst subgraph with the same state type; the analyst’s result is merged back via the shared reducer.

## Pattern

Both graphs use the same `state` type and the same `reducer`, so the inner graph can be used as a node with `analystGraph.AsNode()`. The outer graph’s `Invoke` runs the inner graph’s logic as one step. For different state types you would call the inner graph’s `Invoke` from a custom node and adapt state manually.
