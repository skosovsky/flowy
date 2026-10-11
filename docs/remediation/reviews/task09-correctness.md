# Task 09 independent correctness review

Verdict: errors not found; **0 open findings** for D22–D29.

Reviewed the working diff against `f033a9c`, including the four new test files,
the before-implementation Task 09 contract, and the current activity scheduling
contract. No implementation files were changed by this reviewer.

## Findings checked

- Lost manual-resolution ACK recovery reloads the committed decision and obtains
  a current resume token. Duplicate DecisionID remains a conflict; the recipe
  explicitly distinguishes a recorded decision from exact command equality and
  does not authorize blind redispatch or a replacement decision identity.
- Dispatch and reconciliation receive a callback marker that rejects nested
  CallActivity before mutation. Captured node contexts and Classify have an
  explicit host prohibition; the implementation does not claim complete dynamic
  enforcement. Reconcile is documented as a prompt read-only, idempotent,
  concurrency-safe probe. Panics remain governed by the existing node boundary.
- Invalid classifier values discard the hint, fail closed as ambiguous, and emit
  only the bounded `invalid_classification` code. The OTel allowlist accepts this
  code without propagating host text. Existing nil-RNG and persisted-deadline
  behavior is retained.
- Schedule constructors use the existing validator. Fixed zero delay is valid;
  negative delay, invalid exponential bounds and multiplier are rejected. Total
  attempts include initial dispatch; zero delay requires separate CallActivity
  invocations. Historical unknown attempts and resolution provenance remain
  intact under the existing journal validation and retry tests.
- Host version-bump rules cover callbacks, codecs, hints, projections and merge.
  No function-pointer compatibility inference or automatic migration is added.
- All three interruptionSnapshot error consumers were inspected: failed step
  publication preserves the commit cause; context cancellation preserves both
  context error and cancellation cause; requested handoff preserves its cause.
  Each also joins the restore error and ErrDurableStateUnavailable. The local
  result reason and RunResult GoDoc explicitly disclaim committed authority.
- Growth counters measure serialized committed envelopes, cumulative successful
  rewrites, and retained attempt/wait cardinality. Documented numbers match the
  fresh parent outputs. Backend/WAL/physical costs and stable performance claims
  are explicitly excluded; unresolved waits/unknown history cannot be removed to
  manufacture rollover eligibility.

## Independent validation

- `go test -race ./...`: terminal exit 0; root 15.045s, all root-module packages
  passed. Output: `/tmp/flowy-task28-task09-correctness-race.log`.
- Focused new classification, callback, constructor, rollback, manual lost-ACK
  and manual-to-automatic retry tests, `-race -count=3`: terminal exit 0.
  Output: `/tmp/flowy-task28-task09-correctness-targeted.log`.
- `git diff --check`: exit 0.

Parent final root race and accepted-final lint outputs were also read: root
16.129s and `0 issues.` respectively (lint output:
`/tmp/flowy-task28-task09-lint-accepted-final.log`). The final embedded-field
spacing in the lost-ACK fixture was reviewed and has no semantic effect.
These complement the independent checks; pending
commands were not treated as passing.

## Limits

This is correctness acceptance of Task 09's changed contracts and code. It does
not establish six-module/native-backend acceptance, production growth capacity,
or the final Task 28 release gate. Callback purity, captured-context discipline
and downstream retry safety remain host contracts, as documented.
