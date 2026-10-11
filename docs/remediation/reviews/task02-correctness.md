# Task 02 independent correctness review — final

Base: b2935d8. Final production/test diff reviewed after lint fixes and durable admission repair. Review covers F03, D05, D08, D09 and their task 02 contracts.

## Verdict

No open correctness errors found within the reviewed scope. Accept task 02.

## Finding resolved during review

Originally WithAtomicHandoff was silently accepted by DurableRunner because its validateRunOptions bypassed graphRunner.resolveRunOptions. Durable Start/Stream could acquire and commit initial state before internal ordinary nontransactional fallback. Final durable_runner.go rejects inv.atomicHandoff with ErrTransactionalOutboxUnsupported. All four durable public invocation forms validate before acquireSession. TestDurableAtomicHandoffRejectedBeforeMutation now covers all four forms; documentation makes the execution-profile boundary explicit. Resolved, no remaining finding.

## Evidence

- Shared prepareSnapshot validates runtime/domain invariant then applies registered BeforeSave hooks; both persistSnapshotPrepared and tryTransactionalHandoffSave call it before adapter persistence.
- Hook/domain errors preserve errors.Is cause and reject before save/outbox. Ordinary handoff keeps prepared storage state for successful enqueued/orphaned metadata patches. Recovery uses loaded storage representation and patchHandoffStatus directly saves it without domain validation or reencoding.
- Runtime result remains domain representation. Value copies preserve existing BYOT contract; reference-backed state requires hook-owned detached replacement, explicitly documented.
- Metadata writes retain OCC expected revisions and lease-bearing save context. Lease guard capability resolver checks inner transaction support; adapter-native fencing and transaction callbacks are unchanged.
- WithAtomicHandoff checks both ordinary transaction collaborators before lease acquisition, node work and preparation; runner-default outbox supported. Durable profile rejects before mutation.
- SkipOnSaveError never suppresses preparation or enumerated structural/OCC/lease/capability rejections. Other adapter errors remain explicitly documented opt-in degradation; no invented resume token/outbox enqueue. Transaction errors remain hard failures.

## Independent validation

PASS: GOCACHE=/tmp/flowy-task28-go-cache go test -race -count=1 -run 'Test(HandoffPreparationParity|PreparationInvariantRejectsBeforeHooksAndPersistence|HandoffMetadataRecoveryDoesNotReencodeState|AtomicHandoff|DurableAtomicHandoff|SkipOnSaveError|CheckpointSkipOnSaveError|TransactionalHandoff|RecoverStaleHandoff|HandoffWithRunLeaseAndMemoryCP|InterceptorsAreApplied|InvariantValidatorBlocksSave|InvariantBlocksContextCancelSave)' .

Result: root package PASS, 1.747s; /tmp/flowy-task28-task02-correctness-tests.log. This exercises new preparation/atomic tests plus existing skip, transaction, recovery, lease-wrapper fallback, interceptor and invariant regression tests. Initial default-cache invocation was blocked by sandbox cache writes; retried with writable cache and succeeded.

PASS: git diff --check.
Inspected parent terminal fresh root go test -race ./... evidence (/tmp/flowy-task28-preparation-race-final.log, all root packages PASS, root 11.338s) and lint final (/tmp/flowy-task28-preparation-lint-final.log, 0 issues).

## Limits

No live Postgres/Redis integration or release execution run here; final multi-module/live adapter gates belong to task 13. Cannot prove arbitrary BYOT host implementations honour declared transaction capability or avoid in-place reference mutation; these boundaries are documented. No workspace edits or commit made by reviewer; report only under /tmp.
