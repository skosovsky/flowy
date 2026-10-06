# Task 09 — independent completeness acceptance

Base: `f033a9c` (`fix: runtime configuration`). Reviewed the final worktree
against D22–D29 of Task28 and the before-implementation Task09 contract.
Reviewer did not implement the remediation.

Verdict: **100% complete — 8/8 decision items**. No open completeness gaps
within this task.

| Item | Final disposition and evidence | Complete |
|---|---|---|
| D22 | Duplicate DecisionID continues to fail. Current activity document gives authoritative load/inspect recovery, decision provenance comparison, digest limitation, fresh revision token, eligible original-command retry only after confirmed absence, and unknown-read handling. Lost-ACK fixture commits the decision then loses acknowledgement, inspects it, rejects duplicate and resumes with one downstream dispatch. | Yes |
| D23 | GoDoc and current contract require prompt read-only/idempotent/concurrent reconciliation; dispatch/classifier panics retain node panic behavior. Callback-derived dispatch/reconcile contexts reject nested CallActivity before journal mutation. Captured node contexts and contextless Classify remain explicitly prohibited host behavior, not falsely claimed runtime detection. Sequential owning-node calls remain supported. Integration test proves rejected nested dispatch creates no extra journal record. | Yes |
| D24 | Current contract distinguishes record state, historical classification/unknown attempts, live/reconciled/manual origin and replay provenance, all-zero retry exhaustion and manual lifecycle stages/DecisionID. Implementation retains attempt history and state-error taxonomy; existing retry/reconciliation/resolution suites substantiate these dispositions. | Yes |
| D25 | Validated fixed and exponential constructors, configuration examples and total dispatch table cover initial attempt counting, zero-delay behavior and separate calls. Constructor matrix rejects negative delay, invalid cap and multiplier; attempt-growth fixture exercises 16/64 explicit calls with pending versus exhausted outcomes. | Yes |
| D26 | Nil RNG standard source and persisted sampled deadline remain unchanged. Invalid classifier values discard hints, fail closed as ambiguous and emit bounded invalid_classification, admitted by OTel bounded-code mapping. Unit/integration fixtures assert unknown result, one bounded diagnostic and no private code. | Yes |
| D27 | Host bump table covers implementation/classifier/codecs, retry/hints, matcher/continuation, children/merge/budget and fork/import/rollover projections, with descriptor versions as applicable. Function-pointer hashing and silent version changes on replay are explicitly rejected. | Yes |
| D28 | Failed rollback reload/decode joins ErrDurableStateUnavailable and both causes. Step, cancellation and handoff restore failures route through diagnostic helper; RunResult GoDoc and reason disclaim committed authority. Codec fault fixture asserts local diagnostic value, both error identities, explicit reason and unchanged authoritative initial stored state. | Yes |
| D29 | Fresh measured serialized maximum/cumulative rewrite volumes cover activity journals, child fanout, attempts and unmatched wait decisions. Published numbers match fresh benchmark/growth logs. Documentation states measurement limits, resolved committed rollover boundary, immutable payload refs and retention of unknown/receipts/identity/fences; no new storage engine or TTL deletion is introduced. | Yes |

Independent executed validation:

- `GOCACHE=/tmp/flowy-task28-go-cache go test -race -count=2 -run 'Test(ManualActivityLostAckInspectAndResume|ActivityClassificationFailsClosedWithDiagnostic|ActivityCallbackContextRejectsNestedCall|ActivityScheduleConstructors|DurableRollbackDecodeFailureIsDiagnostic|ActivityCallbackReentryAndInvalidClassificationObservation|ExecutionWaitDeliveryRewriteGrowth|ExecutionAttemptRewriteGrowth|DurableActivity|ActivityRetry)' .`: terminal exit0, root 9.797s. Log `/tmp/flowy-task28-task09-completeness-race.log`.
- `git diff --check`: terminal exit0.
- Inspected parent growth log: wait and attempt measurement fixtures PASS; benchmark log: six activity/child cardinality workloads PASS. These are parent measurements, not an independent latency benchmark rerun.
- Parent confirms final root race terminal exit0 (root 16.129s). Final accepted lint log `/tmp/flowy-task28-task09-lint-accepted-final.log` contains `0 issues.` and parent confirms terminal exit0. Final lost-ACK fixture embedded-field blank line was inspected; this formatting-only change does not alter independently executed semantics.

Limits: 100% is Task09 completeness, not Task28 completion. Native backend and
all-six-module final verification remain Task13. Race evidence covers exercised
paths; captured-context reentry and Classify compliance remain host contractual
obligations. Memory serialized byte measurements exclude backend indexes/WAL and
are deliberately not production capacity or stable latency estimates. No release,
push or PR was performed.
