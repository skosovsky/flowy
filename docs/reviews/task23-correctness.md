# Task 23 correctness review

Status: no unresolved correctness findings after re-review 2. Historical findings and their resolutions are retained below. Independent review of the approval example, actual backend integration, host persistence, semantic consumer gate and CI. Green suites do not discharge the adversarial requirements. This report covers correctness; the separate completeness review owns AC scoring and required gate logs.

## P1 — conflicting/incomplete result evidence becomes confirmed business success

Location: `examples/approval_recovery/host.go:308–316` (`host.reconcile`), plus result validation in `effect` and operator receipt validation in `resolveReceipt`.

Reproduction against a real completed backend record:

1. Run a normal approved effect while injecting `lost_delivery`; retain the mapping and actual completed operation record.
2. Preserve the backend record binding and its typed result with `Accepted=true`.
3. Replace cached `envelope_result` with the same OperationID/value and `Accepted=false`, remove cached `data`, and clear `mime`.
4. Supply that record through the read-only Inspect test adapter, retaining actual mapping and state.
5. Call `host.reconcile` with the actual ActivityIdentity.

Observed: no error; returned receipt has `Accepted=true`. The downstream result representations conflict and required delivery evidence is missing. `JSONResultCodec.DecodeResult` does not enforce the host's result completeness/consistency policy; checking only the typed receipt silently blesses the corrupt record. Receipt JSON decoding also uses non-pointer fields, so absent `accepted` cannot be distinguished from explicit business rejection and absent `value` can match an empty input.

Evidence: disposable test `TestReviewConflictingEnvelope` passed, logging `CONFIRMED: contradictory/incomplete terminal envelope returned business success`. Probe retained at `/var/folders/46/5ywmz5gj26n7mnky51gd60g00000gn/T/task23-review-1hosd6f5/consumer/review_probe_test.go`. Original implementation was not altered by this probe.

Required fix: strict host-owned terminal evidence validation before returning recovered outcome. Validate supported codec/delivery format, every mandatory receipt field, typed/raw/envelope agreement, required MIME/data, addressed identity/input and explicit terminal business outcome. Add corruption fixtures for each missing field and each contradictory representation; all must remain unknown through runtime recovery, with no dispatch/consume/capture.

## P2 — persistent dispatch counter omits rejected duplicate attempts

Location: `examples/approval_recovery/postgres.go:96–106`; claimed assertion in `examples/approval_recovery/process_integration_test.go:165–175`.

Reproduction: map an intent; invoke `postgresEvidence.Write` twice with the same ID/input. First call succeeds. Second call reaches the external write method and returns `host: duplicate external dispatch`. Query durable `calls,writes` afterward.

Observed: `calls=1,writes=1` despite two dispatch attempts. `calls` only increments after duplicate/revocation checks and commits with the receipt. The process gate would therefore accept a forbidden redispatch if the duplicate was rejected before mutation. Memory persistence counts the duplicate attempt; PostgreSQL does not.

Evidence: disposable PostgreSQL probe `TestReviewPersistentDispatchCounter` passed, logging `CONFIRMED: two dispatch attempts reported calls=1, writes=1`, with the supplied isolated database. Probe retained in the same disposable consumer directory as `review_pg_probe_test.go`.

Required fix: independently persist dispatch attempts before potentially failing dispatch, including rejected duplicates, and count authoritative effects separately. Assert that a duplicate attempt increases attempts while preserving effect count. Extend process recovery assertions so they prove one attempt as well as one effect; clarify distinction between runtime dispatch callbacks, backend handler admission and external writes.

## Other reviewed behavior

The core dependency boundary is preserved: concrete backend imports are confined to the opt-in example. The actual published operation store performs grant binding and expiry checks. Mapping is saved before dispatch; reconciliation uses read-only lookup/Inspect. Revocation and PostgreSQL effect mutation serialize on the same row lock. Completed business rejection is carried explicitly as a receipt with Accepted=false. Runtime automatic retry is disabled. Required capture precedes claim; post-effect capture has a separate persisted suspension continuation. Signed decision replay saves the exact payload before grant creation. Consumer gates set GOWORK=off and remove replacements in published mode; process fixtures abruptly terminate and restart actual child processes.

No additional reproducible runtime defect was found in the examined paths. This is not a proof of absence of errors. Acceptance remains blocked on resolving the two findings and re-reviewing the exact final state, alongside the separate completeness and mandatory gate results.

## Re-review 1

P2 persistent dispatch counter is resolved in the inspected implementation: rejected duplicates and fenced/revoked attempts now commit a separate calls increment. P1 conflicting representations and absent MIME/data/envelope are resolved; the six TestConflictingTerminalEvidence cases and TestConsumedGapWithVerifiedAbsence passed independently in a disposable checkout copy.

Two findings remain:

### P1 — mandatory receipt field presence still not validated

`validateReceiptChunk` unmarshals raw JSON into ordinary string/bool fields. With source Value="reject" and Accepted=false, a raw receipt omitting `accepted` passes. With source Value="" and Accepted=true, a raw receipt omitting `value` passes and yields successful business outcome. JSON null fields are likewise converted to zero values. `postgresEvidence.Receipt` and `resolveReceipt` also do not prove mandatory field presence. Representation equality is necessary but insufficient for complete evidence.

Disposable TestReviewMissingReceiptField confirmed both cases and passed. Required fix: strict mandatory non-null fields before decoding raw and persisted host receipt evidence; also validate both cached result representations without losing presence metadata. Unknown/incomplete result evidence must not resolve terminally.

### P2 — split-store retry protocol cannot complete after transient failure

`authorizeRetry` calls the non-idempotent FenceAbsence and then resolves backend/runtime stores. If FenceAbsence succeeds but operation Resolve returns a transient error, current_attempt becomes 2 while the runtime remains unknown with one attempt. A second authorizeRetry repeats FenceAbsence(1), which permanently rejects. If backend Resolve succeeds but runtime ResolveActivity fails, the next invocation also rejects because OperationRetryAuthorized is not admitted. Lost acknowledgments have the same problem.

Disposable TestReviewRetrySplitStore injected a one-off backend Resolve failure; removing that failure did not permit completing the protocol. It confirmed zero effects, so the split is fail-closed, but the documented operator completion path is not implemented or specified concretely. Required fix: idempotent addressed retry staging that validates persisted fence proof and can finish each remaining store, plus fault tests for failures/lost ACKs between every stage. Alternatively explicitly document the exact authenticated repair actions for unsupported windows; a generic "operator completion" instruction is insufficient.

InspectExecutionResume was reviewed as a read-only integrity/collection validation helper; it does not authorize retries, claim a lease or inspect host state. No new reproducible defect was found in that helper. Mandatory aggregate gates and external publication remain owned by the parent workflow; their incompletion is not treated as PASS here.


## Re-review 2 — all reported findings resolved

Reviewed the current strict receipt decoding, stored-result prevalidation, runtime retry staging, memory/PostgreSQL fencing and backend retry authorization. Mandatory operation_id/value/accepted fields are pointers during validation; absent/null values are rejected before conversion to receipt. Raw, typed and envelope evidence agree, and unsupported codec/MIME/delivery/audience still fail closed. The PostgreSQL Receipt path uses the strict decoder before operator resolution.

Retry fencing is now idempotent for the same addressed attempt while no receipt exists. Backend RetryAuthorized with the matching persisted proof permits completion of the runtime stage. A matching prepared runtime retry resolution is replayed using the current validated token. Old downstream attempt remains fenced, a receipt prevents authorizing retry, and effect authorization/grant expiry are rechecked on the actual new dispatch. Runtime owns the bounded second attempt; default unknown classification does not automatically retry ambiguous execution.

Independent command in the actual current example directory:

`GOCACHE=/tmp/flowy-task23-go-cache GOPATH=/tmp/flowy-task23-go GOWORK=off go test -race -count=1 -run 'TestReceiptRequiresExplicitFields|TestConflictingTerminalEvidence|TestConsumedGapWithVerifiedAbsence|TestRetryProtocolCrashStages' -v .`

Result: PASS. The six corrupt-envelope cases, explicit receipt-field checks, fenced consumed-gap recovery and all three split-store lost-response boundaries passed. Inspected `/tmp/flowy-task23-process2.txt`: process integration module PASS in 171.010 seconds. No new reproducible correctness finding was identified, and no implementation/regression edits were necessary for this re-review.

Correctness review has zero unresolved findings for the inspected current code. This does not prove absence of all errors and does not discharge separate completeness, mandatory aggregate gates, actual published consumer verification, release or issue closeout requirements. Those remain independently verifiable workflow gates.
