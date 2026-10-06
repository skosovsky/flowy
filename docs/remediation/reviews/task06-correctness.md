# Task 06 independent correctness acceptance

Reviewer: `/root/task06_correctness`. Baseline: `714f5ed`; reviewed the current
tracked diff and untracked Task06 implementation, tests and contracts. The
reviewer did not implement these changes or change production code.

Verdict: **PASS — ошибок не обнаружено; открытых findings нет.** This verdict
covers F09, F10, D36, D44, D54, D55 and D56, not the remaining Task28 work.

## Source and contract checks

- F09: shared header validation checks both admission and decode before host
  codecs/cloners; Save derives the stored revision from OCC. PostgreSQL rejects
  invalid headers before BeginTx/outbox; backend JSONB rejection rolls back head
  and history. No arbitrary host decoder is called as a Save validator.
- F10: malformed UTF-8 reasons and manual provenance are rejected before store,
  child backend or notification work. Literal U+FFFD and Unicode remain exact
  after serialized reload. Wait cancellation, child cancellation and budget
  return replay tests use separately created runners and persisted authority.
  Audit-only dispatch error text retains its documented lossy rendering boundary.
- D36: the decision namespace table matches the existing current-group and
  token gates. Cancellation replay may notify again; budget replay is read-only;
  wait/confirmation duplicates reject settled or changed child revisions; outcome
  decision IDs are checked across stored execution groups. No new global ledger
  or implicit latest-token behavior is introduced.
- D44: deadline validation checks the JSON time domain after nonzero/UTC gates.
  Delivery operation naming is consistent in core and OTel. Stage/Code remain
  the outcome discriminator; CancelWait still ends the whole execution in failure.
- D54: memory and PostgreSQL distinguish zero, missing/future and retained missing
  payloads. PostgreSQL signed-range rejection precedes SQL conversion; malformed
  envelopes retain integrity errors rather than becoming absence.
- D55: mandatory nil interfaces and typed nil collaborators are rejected, with
  documented adapter error types. PostgreSQL clean-break error-return constructors
  have adapted Go call sites and test-only checked helpers. JSON wire and stricter
  PostgreSQL JSONB domains are explicit, preserving BYOT for typed values.
- D56: positive fractional lease TTL reaches Redis as ceil milliseconds and PG
  as ceil microseconds; memory keeps the duration. Signed backend exhaustion and
  unsigned memory advancement reject without counter wrap/reset or new lease.
  PostgreSQL lease token bounds retain ErrLeaseLost.

## Independently executed checks

All commands exited 0; backend environment variables were supplied, so live
checks were executed rather than accepted as skipped.

| Check | Result | Log |
|---|---|---|
| Root Unicode replay and durable nil constructors, race count2 | PASS, root 1.617s | `/tmp/flowy-task06-correctness-root.log` |
| Live PG snapshot admission, historical/fence boundaries and JSONB domain, tagged race | PASS 1.776s | `/tmp/flowy-task06-correctness-pg.log` |
| Redis lease full package including live counter/fencing, tagged race count2 | PASS 1.395s | `/tmp/flowy-task06-correctness-redis.log` |
| Checkpoint codecs and memory helper full packages, race count2 | PASS 1.158s / 1.170s | `/tmp/flowy-task06-correctness-memory-codec.log` |
| Redis snapshot full package, race count2 | PASS 1.858s | `/tmp/flowy-task06-correctness-redis-snapshot.log` |
| Current diff whitespace check | PASS | `git diff --check` |

These checks used PostgreSQL at `127.0.0.1:64582` and Redis at
`127.0.0.1:50840`. Parent full root/adapter race/lint and full PG integration and
blueprint logs are supplementary evidence, not claimed as reviewer-owned runs.
The review does not prove host codecs are bijective, downstream notification
delivery is idempotent, or every possible host/backend failure is covered. Those
remain the documented host and operational boundaries.

## Final-state reacceptance after tagged-lint repairs

Re-reviewed the final versions of PG `storage_boundaries_integration_test.go`,
Redis `limits_integration_test.go` and durable-agent
`scenario_integration_test.go`. The seedErr/releaseErr shadow repairs preserve
the original error branches. The two documented gocognit exclusions apply only
to scenario assertion helpers; formatting preserves all assertions, including
the renamed `wait_delivery` trace expectation. **PASS remains unchanged; no
open findings.**

Fresh reviewer-owned live tagged race checks after these edits all exited 0:

- PostgreSQL historical/fence/JSONB tests: PASS 2.074s,
  `/tmp/flowy-task06-correctness-pg-final.log`.
- Redis live counter/fencing test: PASS 1.923s,
  `/tmp/flowy-task06-correctness-redis-final.log`.
- PostgreSQL durable-agent recovery blueprint: PASS 3.480s,
  `/tmp/flowy-task06-correctness-blueprint-final.log`.

The same explicit live backend environment variables were supplied; these are
executed checks. Final diff whitespace check also passed. Parent tagged lint
is an additional required gate and is not claimed as a reviewer-owned result.
