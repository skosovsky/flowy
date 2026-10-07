# Task 23 — independent completeness review

Review scope: `.cursor/docs/task23.md`, current working tree, implementation and
tests in `examples/approval_recovery`, contract/README/CI, and actual logs in
`/tmp/flowy-task23-*.txt`. This is an interim review; release and closeout remain
the implementation owner's subsequent responsibilities.

## Interim verdict: 40%

Four of ten acceptance criteria are fully evidenced at this checkpoint. Partial
criteria, running gates and failed historical gates are not counted. Independent
`go test -race -count=1 ./...` in the new module passed (2.954s), using the required
isolated caches. The inspected process log still contains authentication failures
against a different database; it is not a successful process-restart proof.
Follow-up unit3 passes timeout and fenced consumed-gap fixtures; make-test and
lint4 fail, and process2 has not yet produced a final result.

| AC | Status | Evidence / remaining requirement |
| --- | --- | --- |
| AC1 | Partial | Generic core InspectExecutionResume now provides validated issued recovery tokens; core remains backend-neutral. Contract/README still claim no core API change and must be synchronized. |
| AC2 | Complete | Identity/admission and envelope/mapping tests pass; same-input intentions have different IDs, replay retains IDs/results, unsupported labels and conflicting mapping fail closed. |
| AC3 | Complete | Decision matrix, authenticated admission, foreign input/operation, duplicate/stale delivery and real backend binding/expiry/revocation tests pass. Edit preserves intention and requires new bound approval. |
| AC4 | Partial | Memory lost-response, journal failure and state checkpoint fault tests pass with one effect and replay. Required actual new-process evidence remains pending successful process gate. |
| AC5 | Complete | Missing/running/unknown/incomplete/corrupt/conflicting records, cancellation, deadline-exceeded, parallel read-only probes and explicit business rejection pass in unit3. |
| AC6 | Partial | TestConsumedGapWithVerifiedAbsence now covers successful consumed-gap recovery using fenced absence and addressed retry; old attempt denied. after_arm process case adds same-generation and no-ACK assertions. CLI uses core token. Final process evidence pending. |
| AC7 | Complete | Single zero-retry runtime policy; backend has no nested dispatch retry. Capture failure blocks pre-effect write; post-effect failure suspends at separate capture node and recovery keeps intention and one effect. |
| AC8 | Partial | Real PostgreSQL execution/host evidence and file-journal operation persistence wired; mandatory missing-URL failure exists. Actual successful abruptly killed/fresh-process gate has not yet been evidenced by a completed log. |
| AC9 | Partial | CI checkout and published semantic jobs, portable consumer script and validation docs present. Required complete make lint/test/test-race and both semantic process gate logs are pending; historical lint/process failures cannot count. |
| AC10 | Partial | Host composition migration recipe is complete. New InspectExecutionResume requires its own minimal before/after example and removal of stale no-core-API-change statements. |

## Crash matrix audit

| Required boundary | Current evidence | Status / action |
| --- | --- | --- |
| Before intention checkpoint | before_intent fault, zero operations/writes in memory; child-process exit hook | Semantic PASS; process evidence pending |
| After checkpoint; before/after challenge; before arm | before_prepare, after_challenge, before_arm hooks, preparation has no grant | Semantic PASS; process evidence pending |
| After arm commit; before registration ACK | after_arm hook is after registration call but before caller ACK | Same-generation and no-ACK assertions added; final process evidence pending |
| Before/after decision commit; before Resume | before_decision, after_grant, after_decision; durable signed inbox preserves timestamps | Semantic PASS; process evidence pending; core-issued token API added |
| Before/after grant consume; before dispatch | before_claim, after_claim, before_effect; missing receipt leaves unknown, operator resolution fails | Fail-closed covered; successful fenced verified-absence recovery passes unit3 |
| After dispatch; before terminal evidence | after_effect, before_finish; independent receipt permits authenticated operator resolution | Semantic PASS; process evidence pending |
| After terminal evidence; before response/activity journal | after_finish, lost_delivery, journal_commit, readonly reconciliation | Semantic PASS; process evidence pending |
| After activity commit; before state/capture checkpoint | state_commit and after_capture hooks, separate capture continuation | Semantic PASS; process evidence pending |

## Follow-up needed

1. Synchronize docs with the new API and add its minimal before/after example.
2. Supply completed successful required verification logs, including mandatory
   process restart and published semantic fixtures; do not count skips.
3. Request final review against the resulting tree and logs. This interim report
   is not approval for release or issue closure.
