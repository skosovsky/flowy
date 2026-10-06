# Storage adapter contract

## Redis standalone deployment and key transition

Redis checkpointer and lease manager support a standalone server. Constructors
return an error for nil clients, known ClusterClient/Ring deployments, negative
checkpoint TTL or invalid UTF-8 namespaces. Cmdable is a command/fault-injection
port, not a promise of Cluster support; a host wrapper must address the same
standalone server. Cluster/multi-shard deployment is explicitly unsupported.
No Cluster capability is advertised without a real multi-node acceptance gate.

Key schema v2 encodes namespace and execution ID independently using unpadded
URL-safe base64: `flowy:v2:<namespace>:<execution>:history|lease|fence`. Encoding is
injective, including Unicode, braces and delimiters. Checkpointer LeasePrefix
must equal the lease manager Prefix; history Prefix may differ. Cleanup and
history TTL never remove the fence key. There is no old-key reader or fallback.
Stop old writers, drain/reconcile old leases, and start a fresh namespace; an
offline conversion must preserve fence maxima and copy history/lease ownership
consistently before new writers start. Do not reset fences on a reused logical ID.

Checkpoint TTL: zero means persistent and explicitly removes an existing expiry
on successful Save. Negative TTL rejects construction. Positive durations round
UP to whole milliseconds (a sub-ms duration becomes 1ms). PEXPIRE refreshes TTL
on every successful Save; failed OCC/fence checks do not change data or expiry.
Integer duration arithmetic handles time.Duration's full range without float
rounding/overflow. Lease TTL is a separate ownership contract; expiry of either
key does not prove external work stopped or permit deleting fence history.

DeleteIfIdle performs one atomic Lua decision: busy=-1, absent=0, deleted=1.
Busy becomes ErrThreadLeaseBusy even if another client releases immediately
after Lua returns. Deleted and absent are idempotent success. No second query
infers the result. Server errors and unexpected response codes are returned.

## PostgreSQL due projections and discovery

The aggregate remains authoritative. A derived candidate table stores execution
ID, exact revision, work kind, work identity, profile fingerprint and absolute
deadline. A due index orders profile/kind/deadline/execution/identity.
Every aggregate publication updates candidates in the same transaction; no
separate successful index write or scheduler/dispatch ownership is introduced.
Terminal/transferred or payload-deleted executions have no actionable candidates.
Retention removes candidates transactionally when deleting payload. Creation
and transfer anchors remain independent and are verified on candidate reads.

Each page selects at most the requested candidate count plus one through the due
index. The page cursor includes deadline, execution and work identity; equal
deadlines are stable. A scan freezes its caller-supplied upper deadline. A full
polling cycle must restart from an empty cursor: concurrent insertion/reschedule
behind the cursor is observed next cycle, repeats are at-least-once. Discovery
acquires no lease and dispatches nothing; delivery still requires lease/OCC and
revalidates authoritative generation/revision/deadline.

For each selected candidate load its exact authoritative head and recheck profile,
revision, seal, anchors and pending work. Corrupt/stale candidates yield addressed
diagnostics plus a usable next cursor; unrelated healthy candidates continue.
Never silently repair corrupt aggregates or mark them missing. DB errors abort
the operation and remain errors, not empty pages. A projection mismatch is a
diagnostic requiring explicit rebuild; it is never dispatchable.

Schema transition requires applying the new candidate schema and running an
explicit bounded rebuild/validation before enabling indexed discovery. Rebuild
locks each head, validates it and republishes only its current projection in the
same transaction; concurrent commits/retention share that lock. Rebuild reports
corrupt heads by address and continues with a cursor. No normal full-archive scan
fallback remains. Physical plans and archived-workload benchmarks must prove
that normal polling decodes candidates rather than the terminal archive.

### Concrete discovery API and readiness

`DiscoveryCursor` binds UpperDeadline, AfterDeadline, AfterExecutionID,
AfterIdentity, ProfileDigest and Kind. A zero cursor starts a cycle; continuation
requires the same caller-supplied upper deadline, profile and work kind. All
returned pages carry Cursor, More and addressed Diagnostics. Existing
AfterExecutionID-only public pagination is removed, with no deprecated wrapper.
Wait and retry entry points remain `DiscoverDueWaits` and
`DiscoverDueActivityRetries`; both use the same indexed candidate mechanism.

Each head has discovery_projected and optional discovery_error metadata. New
publication marks readiness atomically with replacing candidates. A partial
index over unready live heads provides a bounded EXISTS readiness check; normal
polling returns ErrDiscoveryRebuildRequired if any old unrebuilt head remains.
`RebuildDiscovery(ctx, afterExecutionID, limit)` processes a bounded keyset page
with a transaction/head lock per execution. It returns Processed count (including quarantined/deleted heads), Diagnostics,
AfterExecutionID and More. On a later head failure it returns confirmed partial
progress with More=true: retry after its last confirmed cursor. The failed head
may have committed without ACK and can be safely reprocessed under its head lock. Valid heads replace projections; invalid heads retain
unchanged payload and record explicit quarantine (empty candidates and addressed
discovery_error). This is a host-invoked repair of derived state, never implicit
repair of execution state. Quarantined heads do not block healthy discovery;
a later explicit corrected aggregate publication clears discovery_error.

Candidate primary key is execution_id/kind/work_identity, and the due index is
profile_digest/kind/deadline/execution_id/work_identity with revision included.
Head and projection writes share the transaction for ordinary commits, rollover
source/target and retention deletion. Rebuild never claims a dispatch lease.
Candidate rows are collected and SQL rows closed before authoritative loading,
so a pool with a single connection does not deadlock on nested queries.

Diagnostics distinguish invalid aggregate/anchor, unavailable head, stale revision
and projection mismatch. Semantic invalidity skips only that candidate while
advancing the cursor. Query/connection/transaction errors remain operation errors.
The normal path never scans/decodes noncandidate archive heads; EXPLAIN must show
index candidate selection and partial readiness lookup after archive ANALYZE.

PostgreSQL timestamp precision is microseconds. Derived deadlines round up to the
next microsecond; the sealed authoritative deadline stays unchanged. Discovery
may observe such work up to one microsecond later and never delivers it early.

A healthy head racing publication is not called corrupt: stale revision uses
ErrDiscoveryStaleRevision; profile disagreement uses ErrDiscoveryProfileMismatch;
missing identity or changed deadline uses ErrDiscoveryProjectionMismatch. Their
text is returned as diagnostic Reason. Seal/anchor violations retain the core
corruption error. Invalid import provenance is an addressed invalid aggregate,
not a database outage.

### Measured discovery workload

On local PostgreSQL17, a one-connection pool and 256 actual armed future waits,
100 repeated due polls read exactly one authoritative head per poll with both
zero and 20,000 additional sealed terminal heads. The measured results were
2.018ms/poll, 54,957B/poll, 474allocations/poll without the additional archive,
and 2.344ms/poll, 55,131B/poll, 476allocations/poll with it. These are local
samples, not a production latency SLA or physical storage measurement.
Unforced EXPLAIN ANALYZE after ANALYZE uses flowy_due_order for first and
keyset-next selection, with the tuple boundary inside Index Cond, and
flowy_discovery_unready for readiness. Normal polling reads no archive payloads.
The executable benchmark is BenchmarkIndexedDiscoveryTerminalArchive; the
real-server single-connection/plan/outage test is
TestDiscoveryTerminalArchiveAndSingleConnection.
