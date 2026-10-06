# Durable lifecycle contract

Status: implemented current contract. Historical origin: task25 specification, followed by task26 indexed discovery. The primary growth mechanism is **rollover**, not journal compaction. See ADR `adr/0001-durable-lifecycle.md`. All APIs here are opt-in generic capabilities; no background maintenance runs in core.

## State and effects compatibility

`ExecutionDescriptor.EffectsCodec` is a mandatory nonempty UTF-8 compatibility label, independent of StateCodec. Existing unlabeled descriptors/journals are rejected: drain or explicitly convert offline under host audit; no inferred codec.

Each `ExecutionMigration` may include pure `EffectsTransform func([]byte) ([]byte,error)`. When the effects label changes, this transform is mandatory; reject the chain before running transforms if missing. Each hop supplies a detached copy of the prior effects payload. State and effects transformations prepare one detached aggregate, and both target codecs must decode successfully before any migration publication. Failures leave source/head/history unchanged. This does not rewrite activity or child results. Fork creates fresh accumulated effects using the target codec; it does not copy the source effects. Its separate authority contract is unchanged.

## Rollover authority

Host policy sets named record, complete-source-byte and complete-target-byte limits, and supplies a pure BYOT projection of state/effects. Rollover is an explicit addressed operation, using an exact source ResumeToken, a unique decision ID and an explicit target ID. Host owns external opaque-reference sanitation; core validates runtime references. Rollover requires an idle safe boundary: no current activation activity/child records, all older activities terminal, all children settled and joined, every nonempty child allocation covered by a valid usage claim, no armed wait. Initial intent is not a completed transition. Migration bindings/inline cursors that would refer to source runtime work cannot be copied into target.

Publication is one storage transaction: mark source as transferred, store its immutable decision+target reference, create one target initial checkpoint with independent source/target creation anchors, and preserve fence counters. Source becomes unable to Resume its continuation; target is the only execution authorized to advance it. A fork does not supply this guarantee. The source counter history remains; target starts a new segment/activation: segment-local StepCount resets to zero, while RetryCounts and cumulative BudgetCounts are preserved. Source historical counters remain unchanged. Rollover projection cannot rewrite the source or remote outcomes.

Identical replay with the addressed source decision/target must return the previously published target (including after a lost response), not allocate another ID or silently reproject. Changed content or another target conflicts. Active lease, stale revision, bad descriptor/payload, unresolved work, reused/deleted target ID or failing projection reject publication. The capability must check transitions/anchors itself under locks, rather than trusting a well-formed runtime request. The source-to-target link is metadata, not authority to bypass the target lease.

## Retention and deletion

Maintenance is an explicit opt-in store capability. A request has exact execution/revision and a named host retention policy with a bounded window plus protected revisions. Reject active leases and unsafe unresolved work. Only a terminal or transferred execution may lose its head payload; ongoing executions retain the authoritative head. A completed execution with outstanding allocated usage claims cannot be deleted. Retained checkpoints are self-contained; exact pruned history returns a typed unavailable error and never falls back to latest.

Keep permanent minimal metadata: monotonic fence and head revision, used execution ID tombstone, fork creation anchor, rollover source/target anchor. Never reuse an execution ID, including after deletion; this avoids ABA and makes retained lineage validation independent of source payload availability. Retention may make the original source checkpoint unavailable without invalidating a retained target's creation anchor. Host archive/reference policies can protect extra revisions; core never infers references from opaque bytes.

Fork, rollover and maintenance serialize on the relevant heads/anchors. There is no actionable candidate pointing to deleted payload; task26 derived projections follow the same transaction. Cleanup is atomic and idempotent under an exact decision/request, so interruption before commit leaves all payloads, and interruption after commit can be recovered by inspecting metadata or replaying the same request. DB failure is an error, not empty success. Minimal anchors grow with executions and are measured separately from retained payload volume.

## Limits and measurement

Record/byte thresholds are host-selected admission boundaries, not an implicit guarantee inside a callback that creates unbounded work. Rollover limits the completed-cycle aggregate; each individual fan-out/action payload remains bounded by host input limits. Report active payload, retained payload, anchors, commits, allocations and latency separately. Timing values measured on one machine are observations, not CI latency thresholds. PostgreSQL implementation must demonstrate transaction rollback, lost response and independent-pool recovery before acceptance.

## Concrete publication ports and replay

`RolloverRequest` names DecisionID, TargetID, `RolloverPolicy` (Label, MaxRecords, MaxAggregateBytes, MaxTargetBytes), ProjectionLabel and a pure projection of detached `RolloverPayload{Progress,EffectsPayload}`. MaxRecords is a checked cycle size, not an automatic scheduler: the host bounds batches and pauses at committed node boundaries. MaxAggregateBytes bounds the complete serialized source at the checked boundary, including metadata; MaxTargetBytes bounds the complete sealed fresh aggregate. All limits are positive and mandatory. Oversized metadata is an explicit limit error, not dropped accounting. Host must bound graph/budget dimensions and opaque inputs within a cycle. Fork sources are excluded from rollover in this version; their live/fake authorization cannot be discarded by an authority transfer.

`DurableRunner.Rollover(ctx, source ResumeToken, request)` obtains the source lease. The optional `ExecutionRolloverStore` exposes immutable `LoadRollover` receipt metadata and atomic `CommitRollover`. Replay addresses the ORIGINAL source revision: same decision/target/policy/projection label and target descriptor returns the exact target initial token without projection, codecs or dispatch. Changed request conflicts. It is the only exception to current-token mutation semantics, because it explicitly replays an immutable transfer receipt, never advances a stale cursor. If the target already advanced, the returned initial token is stale for Resume; host must explicitly inspect/load its authoritative current state. No implicit latest fallback occurs.

The target has immutable `RolloverLineage`, including original source address/seal, source and target descriptors, checked policy, projection label, decision and request digest. The source gets `RolloverReceipt` (target initial address/seal) with terminal status `transferred`. Incoming and outgoing anchors survive payload pruning. Normal commits cannot add, change, remove, or revive these anchors; only atomic publication can create them. Exact source history before transfer remains a historical observation, not executable continuation authority.

`ExecutionRetentionStore.RetainExecution` addresses exact current execution revision and a policy Label, KeepLast count, DeletePayload flag and sorted protected revisions. The store returns a receipt with deleted revision/byte counts. An active lease rejects maintenance (even the same owner). Without DeletePayload, always retain the latest head; with it, require terminal/transferred and a safe dependency boundary. Protected revisions must exist and cannot contradict DeletePayload. Pruned exact revisions return ErrExecutionCheckpointUnavailable; a deleted authoritative head returns that same typed unavailable error, never ErrThreadNotFound (which Start could mistake for a fresh ID). Keep original revision numbering, used IDs, fences and all creation/transfer anchors.

Cleanup is one atomic operation; repeat on the same head/policy yields the same retained set without deleting new revisions. A concurrent new revision is a conflict. Requests do not accumulate unbounded decision audit records: metadata holds current revision/fence and anchors, and retention idempotence is defined by the selected retained set. Receipt counts describe work in this call, so a repeated completed cleanup reports zero additional deletions. Atomicity means no partial phase needs hidden retry state.

The independent incoming anchor stores the full receipt, including the exact target revision-one digest. Loading retained target creation revision one checks that digest; matching lineage plus a newly recomputed envelope seal does not legalize changed initial state/effects.


## PostgreSQL deployment and offline transition

Stop all old writers and maintenance before applying `ExecutionSchemaSQL()`.
The schema adds `rollover_incoming`, `rollover_outgoing` and `payload_deleted`
to execution heads, preserving the existing primary keys, revision counters,
fences, leases and fork anchors. Apply the returned SQL using a host-managed
migration transaction. Core never runs schema DDL or a background maintenance
worker. For this adapter revision and fence storage remain PostgreSQL BIGINT;
values outside that range are not supported.

Schema application does not convert old envelopes. The new mandatory EffectsCodec
label participates in descriptor equality and the sealed integrity payload. An
old unlabeled record must not be loaded by the new runner or relabelled with a
blanket SQL JSON update. Drain executions with their original binary, archive
that namespace and start a fresh namespace under the new contract. If the host
needs historical data under the new format, perform an explicit offline audited
conversion: choose both codec labels from actual domain codecs, preserve exact
outcomes and reference relationships, regenerate all affected integrity seals
and independent creation/transfer anchors consistently, and verify the converted
namespace before making it writable. The library provides no legacy decoder or
automatic guessing path. Existing labeled records with no rollover fields keep
those fields absent; schema columns are null and payload_deleted is false.

Rollover publishes the target head, both history rows and both full receipt
anchors in one database transaction, under the live source fence/OCC lock.
Cleanup locks the same execution head, rejects any live lease, deletes selected
history rows and marks a deleted head in that same transaction. The ID/fence and
anchors survive. PostgreSQL discovery excludes payload-deleted heads and queries indexed projections
published under the same head lock/transaction as authoritative envelopes. Discovery
is a bounded query capability; the host owns scheduling, polling and delivery.
No implicit full-envelope scan repairs a missing projection. SQL payload byte
measurements use `octet_length(payload::text)` and metadata measurements must
include both anchors and head identity; neither claims to measure disk pages or
WAL amplification. Pool closure does not revoke a persisted lease; recovery must
wait for TTL or use another explicit ownership policy, never infer remote safety
from a connection failure.

Rollover source validation runs before the host projection and is repeated under
storage publication locks. A source with a fork authorization, an existing
transfer, a definitive failed terminal, an exhausted revision counter or an
initial intent with no completed transition cannot be transferred. Completed
sources and committed nonterminal cycle boundaries may proceed if all dependency
checks pass. Rejected sources invoke no projection, node, merge or dispatcher.
An owner may still finish a pure projection after lease expiry; the publication
fence rejects that late result. Lease expiry does not prove that callback stopped.


## Progress, migration and import provenance

ExecutionProgress is the neutral state/cursor/reference projection used by
execution, import, fork, migration and rollover. MigrationState has been removed
without an alias. Shared address/contract errors are ErrExecutionLifecycleInvalid
and ErrExecutionSourceDigest; fork-only policy/transform errors stay specific.

PrepareExecutionMigration accepts an addressed, sealed source and checks its
integrity before callbacks. Callers must already establish runtime collection and
source-metadata validity through admission; this raw helper is not full runtime
admission. The whole registry must have unique IDs, valid descriptors/functions
and at most one outgoing edge per descriptor, including disconnected entries.
Only the requested chain is traversed; a missing hop or a cycle on that chain is
an error. No branch search, version guessing or shortest-path policy exists.
SourceRevision, SourceDigest and Chain are the migration provenance; the whole
committed envelope seal protects them. The redundant MigrationProvenance.Digest
field is removed. Old migrated envelopes must be drained/archived with their
original binary or explicitly converted offline with consistent seals and anchors;
no implicit decoder accepts old seals under the new shape.

Import deliberately retains Source.Payload in every later aggregate/history row.
This makes provenance self-contained and lets admission validate the artifact
without an external reader or a missing-reference fallback. An artifact of N bytes
retained across R revisions repeats N*R raw artifact bytes, plus base64/JSON and
other envelope metadata. Host bounds artifacts, history and rollover cycles;
retention can remove eligible old rows. Imported metadata is never evidence that
an external effect occurred. Replacing bytes with a host reference would require
an explicit reader, integrity and missing-artifact contract; none is inferred.

Fork defaults to fake mode and may be inspectable without a resumable host policy.
Live execution requires current authorization and a named pure opaque-state
projection; approval/reservation handles must be sanitized by the host. Fork
resets operational counters/handles, while rollover preserves cumulative budget
and retry accounting. Their dependency gates remain separate and are not assumed
interchangeable. Replay of rollover returns its immutable target creation token.
To continue an advanced target, explicitly LoadExecution for that target ID,
validate its authoritative descriptor/integrity, derive its current ResumeToken
and Resume with a compatible runner. A creation receipt never chooses latest.

KeepLast=0 with DeletePayload=false still retains the current head. Repeated
cleanup reports newly deleted rows/bytes only; lost ACK does not provide exactly-once
deletion metrics. Permanent identity rows, monotonic fences/revisions, fork anchors
and incoming/outgoing rollover receipts consume metadata even after payload
removal. This is deliberate ABA protection, not payload compaction. A seal detects
integrity failures, not a coordinated rewrite by a DBA controlling all records.

## Indexed discovery repair

PostgreSQL RebuildDiscovery explicitly repairs bounded head pages under native
head locks. DiscoveryRebuildPage.Processed includes healthy, quarantined and
deleted heads; it is not a count of runnable work. On a later failure, the returned
page retains the last confirmed cursor/count/diagnostics and More=true. Resume
from AfterExecutionID; the failed head might have committed without ACK and may
be reprocessed safely. Never advance from an unconfirmed head. Initial query
failure confirms no progress. Ordinary due queries use indexes and revalidate
heads; no automatic full-scan fallback or background scheduler exists.

Redis adapters provide ordinary snapshot/lease capabilities, not ExecutionStore.
Memory helpers are conformance/smoke implementations, not persistent production
storage. Only native atomic capabilities establish their declared OCC/fencing and
publication guarantees; a profile label alone supplies no durability.
