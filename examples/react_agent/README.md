# ReAct-style agent (Reason + Act)

This example implements a minimal **ReAct** loop: the agent alternates between a "reason" node and a "tools" node until a condition is met, then exits to "finish".

## Graph structure

- **reason** — thinks (e.g. decides next step); returns updated state with a step counter.
- **tools** — executes a tool call (simulated here); returns result in state.
- **finish** — terminal node.

The flow is: `reason` → (conditional) → either `tools` or `finish`. From `tools` an edge goes back to `reason`, forming a loop. The **conditional edge** from `reason` checks `state.steps >= 2`; after two iterations it routes to `finish`, otherwise to `tools`. So the loop runs a fixed number of times then exits.

## Protection against infinite loops

Because the graph has a cycle (`reason` → `tools` → `reason`), execution could run forever if the router never returned `finish`. The example compiles with `flowy.WithMaxSteps(25)` so that after 25 steps the runner returns `ErrMaxStepsExceeded` and stops. Use `WithMaxSteps` in production when you have conditional loops.
