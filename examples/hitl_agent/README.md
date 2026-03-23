# Human-in-the-Loop (HITL)

This example shows checkpoint-backed pause/resume.

## Flow

1. `process` runs normally and updates state.
2. The `approve` node returns `ErrSuspend` until a human approves the state.
3. `flowy` saves a checkpoint for the thread with the current state and resume target.
4. Later, the application loads the latest checkpoint, updates the state, and calls `thread.Resume(ctx, replacementState)`.
5. The graph continues from `Checkpoint.Next` and finishes normally.

## Important point

`flowy` has a built-in checkpointing contract. This example uses `testutil.MemoryCheckpointer`, but in production you would usually plug in a durable store such as Postgres or Redis.

This is the safe suspend pattern: pause on a sequential executable node. `ErrSuspend` is not supported inside fan-out branch execution.
