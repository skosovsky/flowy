# Task 05 independent correctness review

Verdict: PASS; no open errors found in task 05 (F08/D14).

Reviewed the final diff against `d401897`, task 05's pre-implementation contract, authoritative F08/D14, the new collection probes and existing stream-helper tests. Reviewer did not implement or change source/tests; only this report was written.

Evidence:

- Both append and collection snapshot creation hold the same mutex. Returning clones the collected slice while locked, so background append cannot race with the header read or modify caller-owned backing storage. Nil/empty collection behavior remains valid.
- This detaches slice storage only. RunEvent values retain their BYOT references, as explicitly documented; host element ownership and WithEventCloners remain required for mutable referenced values.
- The mutex is never held around source reads, Wait or user callbacks. Cancellation still requests stop and returns without waiting for an uncooperative producer or blocked callback. The background drain owns its private collection and eventually calls Wait after source closure.
- ConsumeEventsAndWait's existing successful/error/join selection semantics are unchanged. Successful collection drains delivered events in order and preserves Wait's cause. Existing false-callback behavior still silently drains after requesting stop.
- Current GoDoc/runtime contract explicitly permit callbacks to continue after an early canceled return, require synchronized captures, and describe Begin/Await collection ownership. WaitResult remains execution authority; no durable queue, forced callback termination or terminal-event guarantee is introduced.
- New tests exercise closed buffered cancellation, caller mutation/extension of detached storage, a held-open producer proving early return, ordered full collection with a Wait error, and cancellation while a callback is held. Existing tests also cover silent drain, timeout and Begin/Await behavior.
- Inspected the preserved before-fix log: DATA RACE points to concurrent captured slice append/read in stream_helpers.go, with expected FAIL. Final source resolves that ownership boundary rather than hiding cancellation or indefinitely joining the drain.

Verification:

- Independent command: `GOCACHE=/tmp/flowy-task28-go-cache go test -race -count=5 -timeout=5m -run 'Test(CollectEvents|ConsumeEvents|AwaitStreamCollect|BeginStreamCollect)' .` — terminal exit 0, PASS 2.970s (session 73392).
- Primary targeted race log inspected: `/tmp/flowy-task28-task05-race.log`, PASS 4.366s (count 10). Primary lint log `/tmp/flowy-task28-task05-lint.log`: 0 issues. Session completion is confirmed by their owning agent before acceptance/commit.
- `git diff --check` — exit 0.

Limits: source inspection and root helper fixtures. Shallow event snapshots do not deep-clone arbitrary BYOT objects; callbacks/producers ignoring cancellation may continue in the background until their own completion. No complete six-module or live-backend gate claimed here; those remain task 13.
