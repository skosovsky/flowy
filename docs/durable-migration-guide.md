# Adopting durable execution

This is a clear break, not an owner-only lease compatibility layer. Ordinary sequential execution remains available, but it does not acquire durable activity/child/wait guarantees automatically. All state, input, result and effect types, codecs and business decisions remain host-owned.

## Stop incompatible workers before changing coordination storage

Replace owner-only lease mutations with the exact ExecutionLease returned by Acquire. Renew and Release use that incarnation; never reconstruct authority from owner text or reset fence counters during cleanup. PostgreSQL adapters require explicit READ COMMITTED transactions and the shared thread lock protocol. Native paired lease/checkpoint adapters must use the same coordination tables. Custom adapters must satisfy the atomic OCC/fencing and immutable-history conformance contract, not just return success from mocks.

Apply explicit offline conversions for old lease/snapshot wire formats. Preserve monotonically increasing fence counters independently of checkpoint/history retention. Redis snapshot revisions use canonical decimal strings; numeric legacy records are not silently interpreted. Execution envelopes require seals and semantic descriptors. Fork heads require an independent creation-lineage anchor; do not manufacture one by trusting a possibly modified latest payload. Do not run old and new workers in one coordination domain while schemas/protocols differ.

For an existing native PostgreSQL coordination schema, stop writers and explicitly convert flowy_checkpoints.thread_id/node_id and flowy_leases.thread_id/owner to TEXT before restarting with the new adapters. New SchemaSQL creates exact TEXT columns, but CREATE TABLE IF NOT EXISTS does not change existing columns. Preserve original values and keys; never shorten IDs to fit old columns. New queries do not narrow IDs, so a still-narrow schema rejects an oversized write rather than targeting a different execution. PostgreSQL index size limits still apply; rejection is not permission to alias or hash an identity silently.

Remove replaced wrappers, deprecated branches and hidden fallback implementations from consumers. Update examples, storage implementations and lease call sites together. A transport cache, event log or handoff outbox is not an activity journal.

## Choose ordinary or durable execution explicitly

Bind DurableRunner to a compiled BYOT graph, ExecutionStore, state/effect codecs and explicit ExecutionDescriptor. Assign graph identity/revision, state codec, execution contract and a named replay-safe step policy. Labels express semantic compatibility; the content digest does not substitute for them. Identity/compatibility strings are valid UTF-8 text; opaque payload bytes remain unrestricted.

Declare node computations/routing replay-safe and put durable external effects through CallActivity. Arbitrary node I/O is not intercepted. Host codecs, transforms, projections and merges must be pure and support the required concurrency. A successful preparation may decode again; do not make decoding a side effect. Core runs no automatic durable maintenance. Explicit retention/rollover use the implemented dependency-safe [lifecycle contract](durable-lifecycle.md); ordinary snapshot cleanup is a separate capability.

Treat interrupted durable node output as uncommitted: cancellation/deadline/consumer stop or context-requested handoff retains the last committed entry state/effects/cursor with already committed activity outcomes. Resume may recompute the pure node from that entry; it must not repeat an external dispatch. Use the returned latest token, not partial local handler output. Explicit Suspend/Handoff directives remain separately selected continuation boundaries.

## Convert saved executions explicitly

For an addressed compatible source, register a pure migration chain with explicit source/target descriptors. Transform target state and cursor; preserve successful activity results. Moving a cursor with unresolved activities/groups requires JournalReferences/ChildGroupReferences to their immutable original identities in the same activation. Original child wait/cancellation addresses and budgets are not renamed or replenished. Target cursor, runtime metadata and state/effect codecs are validated before publication. Missing chains and invalid targets reject without node/dispatch calls or a partial target.

For legacy data, capture a consistent frozen LegacyExecutionSource with ID, revision, format and digest, and provide its named pure ExecutionImporter. Import creates a new target ID and returns a token without node execution. It must not invent completed activities for old side effects or import dangling activity/group bindings. A failed import returns no successful token; a later valid import may reuse the target ID. Resume is a separate action. If conversion is inappropriate, host policy must route to compatible executable code or finish/discontinue the old runs; runtime does not retain old application code for you.

## Move external writes to activities

Provide stable logical keys, implementation labels, encoded host input and downstream operation identity handling. Completed outcomes replay from the journal. Crash after remote dispatch without committed outcome yields unknown, not proof of absence. Supply reconciliation using downstream evidence or an addressed manual decision with ID/reason/evidence. Runtime validates decision structure, not business authority or truth of evidence.

Retry needs a named downstream safe-retry contract and bounded attempt/deadline policy. Choose one retry owner and disable competing adapter retries. A persisted pending retry releases the worker; redelivery before its deadline must not reset attempts or dispatch. ErrActivityJournalUnavailable retains the adapter cause and means confirmation unavailable, not rollback. OCC and lease loss remain distinct errors. Only acknowledge a stored outcome after its aggregate commit.

## Replace ad hoc fan-out when structured children are needed

Provide isolated host projections/codecs, stable child IDs, explicit compatibility labels, bounded concurrency, deterministic join-all and fixed named allocations. Keep each sibling's outcome/wait independently addressable. Do not automatically upgrade inline subgraphs into distributed children. CancelChildren records a request before notification; notification acknowledgement is not termination confirmation. ConfirmChildCancellation requires exact child/request/revision plus host evidence. Unknown work cannot be relaunched blindly or return allocation merely because a lease expired.

ReturnChildBudget records explicit usage of every allocated named counter and returns unused units once. Named counters are not a monetary ledger. External reservations/releases remain host-owned idempotent activity ports; cancellation never promises remote rollback.

## Configure waits and one recovery owner

Opt into the capable execution store and persist a matching WaitCapabilityProfile for journal, lease, timer, clock, retry and recovery ownership. The optional PostgreSQL adapter provides registration and bounded due discovery; host drives scheduling and transport mapping. Discovery is an observation, not a lease, acceptance or dispatch.

Use Await for a saved armed boundary, not an implicit timer interpretation of Suspend. Authenticate transport and verify business permission outside core. DeliverWait commits decision and continuation before acknowledgement; resume separately from its returned token. Retain/retry early ErrWaitNotArmed deliveries. Duplicate replay invokes no matcher/state transition. Event/timer winner and durable loser decisions must not create two continuations. CancelWait records terminal cancellation; late deliveries do not revive the run or prove remote work stopped.

## Inspect or fork without inherited authority

Use exact historical references with revision and expected seal; an absent/pruned source never falls back to latest. Inspection is raw and read-only. Fork creates a new target with immutable lineage and reset journals/effects/waits/children/handles/counters, leaving source unchanged. Unresolved source activities or unjoined descendants must be settled explicitly first.

Default fork mode is fake; a supplied fake policy may execute pure steps and simulated activity/child dispatch. Creation without an executable policy remains inspectable but cannot Resume. Supply a named fake activity dispatcher and, for child graphs, fake child dispatcher; there is no live fallback. Live fork requires explicit current host authorization and a separately named projection removing/rebinding opaque approvals/reservations/continuations. Core cannot sanitize unknown business references inside BYOT state. Sibling forks use new operation identities; a source result never authorizes a new external write.

## Current consumer changes and validation

Historical task22–27 files retain their original evidence/status; current contracts
are [runtime](runtime-contract.md), [lifecycle](durable-lifecycle.md),
[storage](storage-adapter-contract.md) and [validation](validation.md).
No real release is implied by a passing local test or this guide.

Current clean breaks: BuildDispatchGraph replaces BuildSupervisor; stateless
inline composition uses AsStatelessNode/StatelessSubgraphNode, while slot adapters
retain the cursor in parent state. NewAdvisoryLeaseGuardCheckpointer declares its
best-effort precheck; it is not native atomic fencing. ExecutionProgress replaces
MigrationState, ErrChildInvalid replaces ErrChildJoinInvalid, and
ComputeChildBudgetReturn is pure arithmetic; ReturnChildBudget persists it.
Shared exact-history/lifecycle errors use ErrExecutionHistoryUnsupported,
ErrExecutionCheckpointUnavailable, ErrExecutionLifecycleInvalid and
ErrExecutionSourceDigest. TelemetryBridge.Capture/Restore replace Extract/Inject;
WithLifecycleObserver/WithTelemetryBridge provide run-scoped defaults.
Handle constructors that now return errors; do not install nil collaborators.
MigrationProvenance retains source revision/digest/chain under the envelope seal;
the removed inner Digest requires drain/archive or explicit offline conversion
for old migrated records, not blanket JSON relabeling.

Use [canonical ownership and recovery recipes](runtime-contract.md) for unknown
ACK, duplicate decisions, callbacks and postcommit decoding. Run all six module
gates plus explicit persistent backend/blueprint tests; skipped backend tests do
not count as passed. Real publishing is a separate authorized release operation
following [release runbook](release.md).
