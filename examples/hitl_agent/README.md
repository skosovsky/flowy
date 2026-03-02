# Human-in-the-Loop (HITL)

This example demonstrates **interrupt → checkpoint → resume**: the graph runs until it hits an interrupt point, then stops and waits for external input before continuing.

## Flow

1. **process** — runs and updates state.
2. **approve** — should run only after a human approves. We call `InterruptBefore("approve")` so execution stops *before* running `approve`.
3. On the first `Invoke`, the runner saves a checkpoint (state + next node name), then returns `ErrInterrupt`. The app can show a UI or wait for an API call.
4. When the human responds, the app calls `Resume(ctx, threadID, delta)`. The `delta` is merged with the saved state via the reducer; execution continues from the saved node (here, `approve`).

## Checkpointer and thread ID

HITL requires a **checkpointer** (Save/Load by threadID) and a **thread ID** so multiple sessions can have separate checkpoints. This example uses `testutil.NewInMemoryCheckpointer[string]()` for simplicity; in production you would use a database or other persistent store implementing `flowy.Checkpointer[T]`. Pass them at compile time: `Compile(flowy.WithCheckpointer(cp), flowy.WithThreadID("session_1"))`.
