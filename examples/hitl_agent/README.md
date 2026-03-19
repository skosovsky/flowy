# Human-in-the-Loop (HITL)

This example shows middleware-based pause/resume.

## Flow

1. `process` runs normally and updates state.
2. A middleware intercepts the `approve` node before it executes.
3. The middleware saves `(state, meta.SuspendTarget)` into external storage and returns `ErrSuspend`.
4. Later, the application loads the saved state and resume node, then calls `Resume(ctx, state, "approve")`.
5. The graph continues from `approve` and finishes normally.

## Important point

`flowy` does not have a built-in checkpointer. The persistence layer is owned by the application. In this example the application uses `testutil.Store`, but in production this would usually be a database or another durable store.

This is the safe suspend pattern: pause on a sequential executable node. `ErrSuspend` is not supported inside fan-out branch execution.
