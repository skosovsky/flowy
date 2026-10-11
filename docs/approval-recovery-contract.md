# Approval and recovery composition contract

This contract precedes implementation of the opt-in `examples/approval_recovery`
module. Core remains provider-neutral: it owns execution identities, activities,
waits, OCC, fencing and continuations. The host owns approval policy, authentication,
operation bindings, persistence of external evidence and capture. A generic read-only token inspection helper supports lost-token recovery. Pending approval is preparation, not an effect.

## Envelope and checkpoints

The host format `approval-recovery` carries a compatibility label, provider CallID,
logical OperationID, canonical input digest, challenge binding and terminal result.
ActivityIdentity is absent before dispatch and mandatory in the addressed mapping
before an external action. CallID, OperationID and ActivityIdentity need not equal.
Two identical calls are distinct intentions; redelivery retains the same intent.
Unknown format, missing fields, conflicting mapping/binding, incomplete evidence or
unsupported codec fails closed. Terminal business rejection is an explicit result,
not successful business execution. Capture/delivery status is separate from outcome.

Start checkpoints host-allocated IDs before preparation. Preparation executes the
real backend without a grant and must return a bound challenge without a claim or
handler invocation. The challenge is checkpointed before a separate Await node.
The backend rejects grants for changed inputs. Edit before dispatch keeps OperationID,
changes the canonical input, clears the old grant and obtains a new challenge/wait.
Edit after dispatch is unsupported for that intention: reconcile it, then create a
new intent. Deterministic preparation is safe to repeat before its checkpoint.

## Decision and dispatch

Authenticated host admission checks the full addressed challenge and signs delivery
identity, decision, grant expiry and edited input. Correlation alone is not authority.
Grant issuance occurs before delivery and is idempotent; Match and Apply are pure.
Only a committed winner continues. Duplicate/stale/conflicting deliveries cannot
create another effect. Allow requires current authorization at dispatch. Deny,
expired and revoked approvals produce no effect. Wait first-committed arbitration
is independent of grant expiry: a late accepted event cannot bypass expiry checks.

Grant reservation and operation claim must be atomic in the selected backend.
Current authorization is rechecked immediately before claim and at the external
write boundary; the latter serializes with revocation. The host saves the actual
ActivityInvocation.Identity mapping before claim/effect, without copying runtime's
identity algorithm. Required pre-capture completes before claim. One runtime retry
owner is selected; the named bounded activity policy permits only an explicit operator resolution
after old-attempt fencing and verified downstream absence. Default error
classification remains unknown, so no automatic retry occurs.
Backend and transport do not retry effect dispatch. Transport redelivery and capture
recovery are separate bounded operations.

## Evidence and reconciliation

Reconcile reads the mapping and backend Inspect only. It validates the complete
binding and decodes a confirmed completed terminal result; missing, in-progress,
unknown, corrupt or incomplete records remain ErrActivityUnknown. No grant consume,
dispatch, capture or operator resolution occurs inside Reconcile. Concurrent probes
are safe. Runtime may persist a confirmed outcome in its own journal.
Cancellation, timeout and lease expiry do not prove rollback. A consumed claim with
no terminal evidence requires authenticated operator reconciliation against an
independent downstream receipt, or verified absence/idempotency before retry.
A receipt can resolve the original attempt to completed; absent proof leaves unknown.
Post-effect capture failure retains the completed result and operation ID, and
recovery retries only capture. Replaying a completed activity does not claim again.

## Crash matrix and executable evidence

| Hook / boundary | Evidence and recovery | Required tests |
| --- | --- | --- |
| before_intent | no checkpoint, preparation, claim or write | crash matrix |
| before_prepare / after_challenge | IDs persisted; repeat pure preparation | crash matrix |
| before_arm / after_arm | challenge persisted; same armed generation; no false ACK | crash matrix |
| before_decision / after_grant / after_decision | grant may exist; redeliver same signed event; one winner | crash matrix |
| before_claim / after_claim / before_effect | mapping persisted; claimed gap stays unknown without proof | crash matrix, operator recovery |
| after_effect / before_finish | receipt external to worker; unknown until operator verifies receipt | crash matrix, operator recovery |
| after_finish / lost_delivery | backend terminal committed; read-only reconciliation | lost delivery, process restart |
| journal_commit | backend terminal survives failed activity publication | journal failure, process restart |
| state_commit | activity completed before state checkpoint; replay | crash matrix |
| after_capture | capture idempotent; activity replay never repeats write | capture recovery |

Test hooks are deterministic errors or abrupt child-process exits, not time-based
simulated success. Process acceptance uses PostgreSQL execution persistence and the
published local file operation journal, with external receipts/mappings in PostgreSQL.
It does not claim physical disk failure or distributed transactions. Memory cases
prove semantics only. An obligatory integration gate fails without PostgreSQL.

The example's README maps each rule to named tests. CI executes those semantic tests
against checkout code, then against published dependencies with GOWORK=off and no
local replacements. The optional consumer module is tested independently and does
not make its backend a dependency of core or any core adapter.

Capture is a separate graph step. Its failure persists CapturePending and Suspend
with ResumeAt(capture); it does not turn the completed effect into terminal failure.
The local authenticated inbox syncs the exact signed decision before grant issuance;
redelivery reuses timestamps and ID, including a crash after grant persistence.


For the consumed-before-dispatch gap, `authorizeRetry` first fences the old
external-service attempt and atomically verifies that no receipt exists. It then
records authenticated RetryAuthorized in the backend and an addressed ResolveActivity
retry in the runtime. The original OperationID, CallID, grant and activity mapping
remain stable; the backend attempt suffix changes. Any split-store failure stays
fail-closed for operator completion. Timeout/lease expiry alone cannot call this
protocol. `TestConsumedGapWithVerifiedAbsence` proves this successful recovery path
and that a late old attempt is denied. This capability requires downstream fencing;
hosts with arbitrary unfenced tools must keep the gap unknown.

Operator retry stages are idempotent: an already advanced downstream fence with
no receipt proves the same absence; an existing matching RetryAuthorized proof
bypasses duplicate backend resolution; a prepared activity with the matching
recorded resolution returns its exact token. Repeating the trusted retry command
completes split-store interruptions without a new operation or grant.
Terminal receipt decoding requires explicit non-null operation_id, value and
accepted fields; raw, typed and delivery-envelope results must agree, including
false and empty values. Missing fields are not converted into success by Go zeros.
