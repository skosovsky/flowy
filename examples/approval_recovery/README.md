# Authenticated approval and activity recovery

This optional executable module owns its application state, approval binding,
operation IDs, authenticated decisions, effect receipts and capture policy.
Core has no dependency on its concrete execution backend. It uses the published
operation profile and file journal for real binding/expiry/consume checks, and
PostgreSQL for execution state, host mappings and independently stored receipts.
It is a bounded local reference recipe, not a production authorization service.

The graph is `prepare → wait → decision → effect → capture`. Editing a not-yet-
dispatched intention returns to preparation with the same OperationID, changed
arguments and a new binding; it requires a fresh approval. Grant consumption and
operation claim are atomic inside the selected backend. Accepted delivery alone
never authorizes the write. Revocation is rechecked under the external write lock.

Allocate CallID and OperationID at trusted host admission, then Start checkpoints
them before preparation. Do not derive an operation identity solely from argument
hashes. The actual ActivityInvocation.Identity is saved before effect dispatch;
it is a separate address, not a replacement for either host identity.

## Migration from unsafe glue

Before:

```go
// Wrapping approval preparation as an external effect turns PendingApprovalError
// into unknown activity outcome. Blind retry or fake reconciliation is unsafe.
flowy.CallActivity(ctx, flowy.ActivityRequest{Dispatch: prepareAndExecute})
```

After:

1. Checkpoint the host intention before preparation.
2. Execute preparation without a grant; save its bound challenge on a step boundary.
3. Await approval durably. Authenticate the full decision before DeliverWait;
   save the exact signed event before issuing its bound grant. Match/Apply stay pure.
4. On Resume, reauthorize before claiming/dispatching through CallActivity.
   Save the actual activity identity mapping first; complete required pre-capture.
5. Reconcile only through mapping lookup and read-only OperationStore.Inspect.
   Missing or incomplete evidence remains unknown. An authenticated operator may
   resolve the original attempt from an independent receipt, outside Reconcile.
6. Retry post-effect capture from its separate persisted continuation. Do not
   generate another OperationID or dispatch again to repair capture.

`InspectExecutionResume` supplies a validated core-issued recovery token after
a lost response. Existing callers can keep using tokens returned by Start, Resume
and DeliverWait. The bounded activity policy allows one operator-authorized retry
only after downstream fencing and verified absence; the default classifier keeps
errors unknown and never automatically retries. The backend never retries handlers. Transport
redelivery and idempotent capture are separate recovery paths. Cancellation and
lease expiry never establish absence of the external effect.

## Run and verify

From this directory:

```sh
go test -race -count=1 ./...
FLOWY_TEST_DATABASE_URL='postgres://user:password@localhost/test?sslmode=disable' \
  go test -race -tags=integration -count=1 -timeout=5m ./...
```

The mandatory integration test fails without its database URL. It creates and
removes only isolated random schemas, uses trusted temporary local directories,
builds a race-enabled worker, abruptly exits that worker at each named boundary,
and starts fresh OS processes against persisted data. Each successful recovered
effect has dispatch count=1 and write count=1. Claimed gaps without receipt remain
unknown with both counts zero. Lease expiry is only admission to inspect/reconcile.

For a manual run, create an isolated schema yourself, then export
`FLOWY_TEST_DATABASE_URL`, `APPROVAL_SCHEMA`, `APPROVAL_DIR` (absolute private local
directory) and `APPROVAL_SECRET` (hex encoded 32-byte authentication key). Run:

```sh
go run . init
go run . start
go run . allow
go run . resume
```

`deny`, `edit`, `expired`, `revoked` deliver their respective decisions; `revoke`
changes current host authorization. `resolve` is a trusted operator command using
an independently persisted downstream receipt. Never expose this CLI, grants or
its journal to model/tool input. Authenticated sender/issuer credentials, UI,
transport quotas, production storage and decision policy are host responsibilities.
The example's fixed subject/scope and one intention are deliberate finite fixtures.

Local journal requires a trusted directory on Linux/macOS, file/directory sync and
its persistent lock file. Do not delete that lock or clear history on recovery.
Network filesystems, distributed exactly-once semantics and physical disk-crash
proof are outside this recipe. PostgreSQL writes model a host-controlled external
service; arbitrary remote services need their own receipt/idempotency protocol.

## Evidence map

| Contract | Executable tests |
| --- | --- |
| identities, authenticated addresses, redelivery | TestIdentityAndDeliveryAdmission, TestEnvelopeAndMappingConflicts |
| full allow/deny/edit/expired/revoked cycle | TestApprovalDecisions |
| actual binding, expiry and current revocation | TestRealBackendBindingExpiryAndRevocation |
| lost response / journal / state checkpoint | TestLostDeliveryAndJournalFailure |
| missing/unknown/incomplete result; read-only probes | TestReconcileRejectsUnprovenEvidence, TestConcurrentReconcileIsReadOnly |
| terminal business rejection | TestTerminalBusinessRejection |
| required pre-capture, post-effect capture recovery | TestCaptureRecovery |
| deterministic faults and authenticated resolution | TestCrashMatrixAndOperatorRecovery, TestConsumedGapWithVerifiedAbsence |
| actual OS process loss and persisted outcomes | TestProcessCrashRecovery, TestPersistentDispatchCounter |

Checkout semantic gate: `python3 scripts/check_approval_consumer.py --mode checkout`
from the repository root. Published semantic gate: use `--mode published`, optionally
`--version` for an exact published dependency selection. That mode copies the
consumer into a disposable directory, removes every replace directive, sets
GOWORK=off and executes the same semantic and process fixtures against downloaded
artifacts. A successful build alone cannot pass this gate. Release tags for the
optional executable do not make it an imported library or core dependency.


For the consumed-before-dispatch gap, `authorizeRetry` first fences the old
external-service attempt and atomically verifies that no receipt exists. It then
records authenticated RetryAuthorized in the backend and an addressed ResolveActivity
retry in the runtime. The original OperationID, CallID, grant and activity mapping
remain stable; the backend attempt suffix changes. Any split-store failure stays
fail-closed for operator completion. Timeout/lease expiry alone cannot call this
protocol. `TestConsumedGapWithVerifiedAbsence` proves this successful recovery path
and that a late old attempt is denied. This capability requires downstream fencing;
hosts with arbitrary unfenced tools must keep the gap unknown.

Recovery token before/after:

```go
// Before: host assembles an address after loading an execution head.
token := flowy.ResumeToken{ThreadID: head.ExecutionID, SnapshotRevision: head.Revision}
// After: the core validates the seal/collections and issues the latest exact address.
token, err := flowy.InspectExecutionResume(ctx, store, executionID)
if err != nil { return err }
result, err := runner.Resume(ctx, token)
```

The new helper does not execute, reauthorize or reconcile. A concurrently changed
head still yields an OCC conflict in Resume. To compare current source contracts,
checkout mode accepts `--backend-source <explicit-checkout>` or `--backend-ref <source-ref>`;
the latter fetches into disposable directories without requiring adjacent projects.
CI selects the audited source commit implementing the declared named operation-profile API,
and separately tests the pinned published dependency set. The backend default branch
may lag its published artifacts; unsupported older constructors fail compilation
rather than silently selecting compatibility glue.

For an interrupted operator retry, repeat `retry` with the current inspected token.
FenceAbsence idempotently proves the same old attempt is fenced and has no receipt;
an already RetryAuthorized operation with the matching proof proceeds directly to
runtime resolution, and an already prepared addressed activity returns its token.
`TestRetryProtocolCrashStages` covers interruption after each persisted stage.
