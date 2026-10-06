# Durable execution design

## Node panic boundary

RecoverMiddleware catches panics only inside its wrapped node/middleware chain.
The first registered middleware is outermost: place Recover before middleware
whose panic should be caught. A middleware outside that boundary can still panic.
Recovery preserves the supplied input state, returns a failure directive and a
nonnil error; an error panic is wrapped so errors.Is retains its cause. Reducer,
effect commit and checkpoint persistence do not proceed as successful node work.

The surrounding runner's reducer, routing, checkpoint preparation/save/load and
lifecycle publication are outside this wrapper. Synchronous callbacks invoked by
the node itself (including nested graph or activity tooling) are within its call
chain and still retain their own effect/recovery contracts. Shared mutable input
and external effects already performed by a callback are not rolled back. Panic
does not establish absence of remote dispatch and does not justify retrying an
unknown external outcome. Recovery adds no blanket catch around the surrounding
runner callbacks.

## Ordinary snapshot preparation and atomic handoff

Save and SaveWithOutbox use the same state preparation: validate domain state,
then invoke StateInterceptor.BeforeSave once in registration order. The prepared
representation belongs to persistence; RunResult retains runtime/domain state.
An encoded/redacted representation need not satisfy the domain invariant.
AfterLoad restores the domain representation before resume validation/overlay.
For reference-backed BYOT values, hooks must replace shared state with detached
representations: Flowy cannot generically roll back in-place host mutations.

Non-atomic handoff commits pending, patches enqueued, then enqueues the intent;
enqueue failure attempts an orphaned patch. These metadata-only patches reuse
the exact prepared state, including recovery from Load. They do not invoke
BeforeSave or domain validation again. Non-idempotent encoding is therefore
applied once to each prepared checkpoint state, not once per metadata revision.
OCC and adapter-native fencing still apply to every write.

WithHandoffOutbox uses transactional snapshot/outbox commit when both collaborators
support it, otherwise the documented multi-phase protocol. To require atomic
publication, add `WithAtomicHandoff[T, E]()` to Start/Resume/Stream/ResumeStream
options, with a transactional outbox (invocation or runner default). Missing
TransactionalCheckpointer or TransactionalHandoffOutbox returns
ErrTransactionalOutboxUnsupported before lease acquisition, node callbacks or
persistence. Capability declarations do not verify arbitrary host implementations.
DurableRunner uses the ExecutionStore profile and rejects WithAtomicHandoff with
ErrTransactionalOutboxUnsupported before acquiring its execution session or
committing initial state; this option applies to the ordinary snapshot/outbox profile.

CheckpointPolicySkipOnSaveError is explicit degradation for adapter Save errors.
Invariant/interceptor rejection is always returned before persistence. Known
ErrInvalidSnapshot (including envelope errors), ErrConcurrencyConflict,
ErrLeaseLost, ErrThreadLeaseBusy, ErrExecutionCapability, ErrInvalidHandoffIntent
and ErrTransactionalOutboxUnsupported are never suppressed. Other arbitrary
adapter errors may be suppressed: BYOT Checkpointer errors do not provide a
universal infrastructure classification. SaveWithOutbox errors always fail.
Suppression yields no new ResumeToken and never asserts resumability; streams
emit EventCheckpointFailed and terminal reasons carry checkpoint_skipped.
Sync callers must inspect the terminal reason. No suppressed save enqueues an
outbox intent. Hook rejection preserves its cause through errors.Is; it does not
publish a snapshot or outbox entry.

## Scope and ownership

Implement BUG-01–04, then FLW-003/001/002/004/005. Core owns execution consistency; host owns domain types, codecs, permissions and transport. No stage is deferred. Clear break is allowed and required where existing API cannot express the contract.

Runtime identity strings and compatibility labels are UTF-8 text, not arbitrary byte keys. Invalid UTF-8 must reject before hashing, persistence or dispatch; JSON replacement-character normalization is not an identity conversion. Valid Unicode, including the replacement character itself, is allowed unchanged. Host payload bytes remain opaque and need not be UTF-8. Address validation applies to execution/node/activity/group/child identities and their compatibility labels.

This includes imported source/importer labels, migration IDs/cursors/bindings, capability-owner labels, delivery and operator decision IDs, and child wait IDs returned by a dispatcher. Validate public requests before host callbacks; reject invalid transformed runtime metadata before target publication. Inline cursor/reference map keys are identity text too. Error messages and encoded domain payloads do not participate in operation identity.

## Lease mutation protocol (BUG-01–04)

Redis renew/release compare ownership and mutate TTL/delete within a single server operation. An expired or replaced lease cannot be changed by its predecessor. Lease incarnations and durable write fencing are represented explicitly by ExecutionLease; owner names alone are not reusable fencing identities.

PostgreSQL acquire/delete-if-idle use the same transaction-scoped per-thread advisory lock. The lock is acquired in a separate statement before checking ownership, so the subsequent read uses a fresh READ COMMITTED snapshot. Transaction isolation is explicitly READ COMMITTED. Active leases reject all Acquire calls, including the same owner. The lock works for an absent lease row. Renew must take the same lock when deciding whether to extend an active lease.

Commit releases the lock. Any read/write failure rolls back. The PostgreSQL DB contract explicitly requires BeginTx with isolation options, as provided by the pool/connection; passing a pgx.Tx as an implicit nested transaction is not supported. There is no transaction-less fallback. Tests use independent connections and deterministic barriers, and check both lock orderings.

### Lease-port clear break

Owner-only LeaseManager mutation signatures permit ABA when the owner text is reused; atomic Redis scripts alone do not solve this. Acquire now returns ExecutionLease, Renew accepts ExecutionLease and returns its renewed handle, and Release accepts ExecutionLease. There are no owner-only overloads/deprecated fallbacks. Holder/IsHeld remain read-only observations, not mutation authority.

Memory and every persistent adapter allocate a monotonically increasing per-thread incarnation. Expiry/release/history cleanup never removes or resets the counter. Redis allocates the incarnation and installs the lease in one server operation; renew/release compare owner plus incarnation atomically. PostgreSQL uses the existing shared per-thread lock and a retained fence counter. Expiry decisions use authoritative storage time; supplied ExpiresAt is informational only.

Ordinary runner lifecycle retains the acquired handle for heartbeat, release, handoff/outbox and checkpoint writes. Native checkpoint adapters validate that handle atomically with snapshot mutation whenever a lease is active; OCC alone is insufficient. A write without ownership cannot overwrite an actively leased checkpoint. The native Acquire/DeleteIfIdle shared-lock protocol remains intact. All mocks, adapters, examples and tests must adopt the new signature together. Reused-owner ABA regressions must exercise renew, release and stale checkpoint/outbox writes, including real persistent backends. This stage is required before claiming BUG-01–04 complete or proceeding to children/waits/fork.

Redis snapshot wire revisions are canonical positive decimal strings, compared exactly in Lua rather than converted to floating-point numbers. Numeric legacy records and corrupt head metadata must reject before mutation; importing them requires an explicit offline migration while workers are stopped. Revisions cannot wrap uint64; an exhausted revision rejects before encoding or write. PostgreSQL snapshot/outbox writes use the shared thread lock and validate the live incarnation both before encoding and before commit. Enqueue callbacks must perform their effects exclusively through the supplied transaction; arbitrary external callback I/O cannot be rolled back.

## Persisted aggregate and compatibility (FLW-003/001)

Durable persistence uses a raw envelope. Descriptor metadata is readable before invoking a host state codec. Descriptor contains explicit graph identity, graph revision label, state codec label and execution contract label; digest is a separate integrity/provenance field. An absent descriptor never means current. Ordinary non-durable execution remains opt-in independent.

The descriptor also requires a labelled StepReplayPolicy with mode replay_safe. This is the host's explicit graph-wide declaration that all node computation and routing may repeat after an uncommitted step, and that external effects use the activity boundary rather than arbitrary node I/O. Runtime does not infer purity or intercept I/O. No empty/default policy is accepted. The synchronous durable profile supports only this declared replay-safe mode: node computations may run again while completed activities replay their committed outcomes and unknown activities require recovery. This is not a second activity retry owner. Policy label/mode participate in exact descriptor compatibility and migration-chain keys; changing them rejects before codec/node calls unless an explicit pure migration commits the new target descriptor. Legacy envelopes missing the policy require explicit import, not an implicit replay-safe upgrade. Ordinary execution is unchanged.

The durable store atomically commits a full execution aggregate against expected revision and a live lease incarnation/fencing token. Aggregate includes cursor, state bytes, effect bytes, activity intents/outcomes, child join progress and wait decisions. No separate successful journal write is treated as a checkpoint commit. Each logical activity transition may advance aggregate revision while retaining the same step cursor until the step is committed. A crash after outcome commit replays the stored outcome and commits the step without dispatch.

Every stored aggregate is sealed with a canonical SHA-256 content digest after storage assigns the committed revision. The top-level digest field is excluded from its own hash; descriptor labels remain the semantic compatibility authority. Load validates identity, exact stored/requested revision, pointer and digest before returning raw data. Missing seals or mismatched payload/metadata are ErrExecutionCorrupt, not absent executions or implicitly upgraded records. Legacy unsealed data requires explicit import. Runtime revalidates integrity before descriptor selection, migration callbacks, codecs, nodes and manual resolution. Store sealing belongs to the same fenced/OCC transaction as the write. Digests detect corruption, not authorization or malicious actors able to rewrite both payload and digest.

Activity journal validation is independent of the content seal: a sealed invalid journal must still be rejected before migration callbacks, domain codecs and nodes. Empty bytes represent no activities; explicit JSON null and non-object payloads are corrupt. Map keys match nonempty record identities; logical keys, input digests and implementation labels are required. Attempts are sequentially numbered with nonzero incarnations and start timestamps, valid persisted states and classifications; only the final attempt may remain running. Prepared retries require an explicit safe policy, remaining attempts and a persisted deadline. Running/unknown/completed/failed records must have attempts and consistent outcome origin; reconciled/manual completion preserves the abandoned unknown attempt rather than fabricating successful dispatch. Journal failure is ErrExecutionCorrupt and never authorizes dispatch or silent record replacement. Domain payloads remain opaque.

An unclassified storage failure while committing an activity transition returns ErrActivityJournalUnavailable and retains the original cause through errors.Is. The error means journal confirmation is unavailable, not proof that a remote commit was rolled back. OCC conflict, lease loss, cancellation, invalid snapshot and corruption retain their existing classifications rather than being relabelled as outages. Failure before intent confirmation prevents dispatch; failure after dispatch requires recovery of the authoritative journal and may leave an unknown outcome. No automatic retry of the external action follows a storage error.

Acquire returns a unique incarnation and monotonically increasing fence per execution. Renew/release/commit compare that incarnation, not only owner text. Expiry preserves the fence counter; deletion/recreation cannot recycle a live fencing identity. Store validation and write occur in one storage transaction. Host codec supplies domain types; runtime never examines application references inside opaque payload.

Migration loads the raw source revision and runs a registered pure chain without node/dispatcher invocation. Chain edges use explicit source/target descriptors and stable migration identity. Reject cycles, ambiguous/missing chains and invalid target before commit. Callback receives a detached copy; source checkpoint identity and successful activity results are immutable. Commit records source revision plus chain identity/digest and retains source history. Runtime validates the target cursor, runtime collections and detached target state/effects using the selected target codecs before publication. Target codecs must be pure and may be called again during execution. Incompatible source bytes are never decoded using target codecs before an explicit chain is selected. Invalid target encoding returns ErrMigrationInvalid and leaves latest/source history unchanged. A failed transformation or OCC/fencing conflict exposes no partial target.

Explicit legacy import creates a new execution identity rather than guessing the descriptor of an existing record. The host supplies a frozen, addressed source artifact (source ID, nonzero revision, explicit format label, raw bytes and SHA-256 digest) and a named pure importer for that format. Runtime validates the digest before calling that importer, passes detached bytes, validates the resulting target cursor and target codecs, then commits revision zero-to-one under a live target lease. Import returns a continuation token but never invokes nodes/dispatchers. Source artifact, digest and importer identity are retained in the target envelope; the caller's original artifact is untouched. Import never fabricates activities or completed outcomes for old side effects. Host owns atomic capture of a legacy source from its original storage and permissions to import it; importing does not imply freezing a mutable remote source. Any transform, target decode, cancellation, OCC or fencing failure leaves no partial target.

Import validates the complete target runtime collections and provenance before commit. Its empty activity journal and child groups cannot resolve imported JournalReferences or ChildGroupReferences: dangling bindings return ErrExecutionImportInvalid without a token or checkpoint. A subsequent valid import may reuse the same target ID. Host-managed inline ChildCursors remain allowed; they do not create distributed children or activity outcomes.

## Current activity protocol

Execution session acquires the fencing lease and starts heartbeat before load/initial encoding/migration preparation. The same session survives stream handoff and terminates only when execution finishes; cleanup is idempotent. Losing the heartbeat cancels with ErrLeaseLost. After uninterruptible host codec/transform returns, cancellation is checked before any aggregate commit or node dispatch. The session releases ownership using an independent bounded cleanup context. Stream creation commits its initial envelope before returning a handle. Fresh and replayed completion events follow the persisted terminal outcome, never an uncommitted node result.

Terminal failures persist an explicit failure record (reason and original error message) with the terminal aggregate. Resume returns a typed PersistedExecutionError unwrapping ErrExecutionFailed; arbitrary host Go error identities are not reconstructed from text. Live execution retains the original error. Both live and replayed failure events are emitted only after a committed failure. Invalid terminal status, a completed terminal carrying a failure, or a failed terminal without its failure record is corruption and rejects before host decode/node calls. Retry-pending, unknown/running activities, cancellation, lost ownership and storage commit failures remain recoverable, not fabricated terminal outcomes.

The synchronous durable runner binds a raw envelope to a live fencing lease. A node calls the explicit activity boundary; arbitrary node I/O is not intercepted. The activity identity incorporates execution ID, node cursor, activation and host logical key. Host dispatch receives that persisted identity, detached input bytes and attempt number. A cycle advances activation only with the committed step; recovering a failed step does not invent a new identity.

Intent (prepared) and dispatch intent (running) each commit before the host callback. A stored completed outcome is replayed without calling the dispatcher. Recovery of a prior lease's running attempt durably marks it unknown; host reconciliation is explicit and preserves the abandoned attempt. Neither terminal completion nor suspend/checkpoint may advance a cursor with an unresolved activity, even if the node ignores the returned error. Failed checkpoint commits leave the local envelope activation unchanged. Historical typed decode rejects mismatched descriptors before selecting the current codec.

Retry design: the runtime is the only retry owner. Host supplies a compatibility-labelled policy and error classifier, with a named downstream safe-retry contract before any retry is allowed. Adapter-internal retries must be disabled. Each invocation dispatches at most one attempt; a retryable failure durably commits a prepared retry with absolute next-attempt deadline and returns a typed pending error. The runner releases its worker/lease rather than sleeping. Resume before the stored deadline dispatches nothing and does not increment attempt count. Policy changes for an existing identity conflict rather than reset limits. Unknown outcomes are not classified as absence of a remote effect; they remain unresolved unless explicitly reconciled. The host classifier is never used to reinterpret an abandoned running attempt.

The execution clock is injected into DurableOptions and supplies persisted activity timestamps/deadlines; the store remains authoritative for lease expiry. Default clock is wall time. The optional PostgreSQL backend supplies bounded due-wait and due-activity-retry discovery for executions with matching ownership profiles. The declared host scheduler/recovery owner drives discovery and redelivery; the adapter does not run an automatic polling loop. Event/timer arbitration commits one wait transition. Explicit manual resolution and declared replay-safe step policy are implemented. There is no claim of exactly-once external delivery. Host dispatch must implement any downstream idempotency guarantee itself.

Manual activity resolution is an explicit addressed operation, separate from Resume and dispatch. ResolveActivity takes the latest execution token plus activity identity, expected input digest/implementation and a unique decision ID, reason and evidence label. It acquires a new fenced lease, validates descriptor and exact revision without running migration/codecs/nodes, and resolves only the current activation's unknown or abandoned-running activity. Resolution may record completed output, definitive failure, or authorize a retry under the already persisted named downstream safe-retry contract and remaining attempt limit. Retry preserves all attempts and commits an absolute next-attempt deadline; it does not dispatch. Abandoned running attempts become unknown, never completed/canceled by assumption. Decisions retain action, prior state, source revision, timestamp, lease incarnation and host evidence. Duplicate decision IDs, resolved activities, mismatched inputs/implementation or absent/unsafe/exhausted retry policy reject without mutation. Host owns operator authorization and evidence interpretation. A losing lease or OCC failure commits no resolution; the caller receives the new token only after aggregate commit. Completing manually does not rewrite the abandoned attempt as a successful dispatch. Runtime never infers remote absence from TTL/timeout.

## Children, waits and fork

PrepareChildren persists the detached plan as child-group intent in the same sealed/fenced/OCC execution aggregate. Repeating the same current group address replays the original plan; incompatible input/policy/allocation labels reject without reset. The returned copy cannot mutate persisted inputs. No child dispatch is authorized before this commit. A parent cannot advance or publish terminal completion while a current group is unjoined, even if node code ignores the group. Intent-only planning does not launch children or prove the full child runtime capability. Persisted group metadata is validated before domain decoding and migration; a cursor migration cannot strand an unresolved group.

RunChildren launches planned/queued children in stable-ID order up to the concurrency bound. Queue/running/outcome transitions each advance child revision within the aggregate; running records retain the owning lease incarnation. Results commit independently, preserving siblings. Dispatcher error is unknown, not definitive failure. Waiting outcomes need an explicit wait ID. Completed/failed children replay without dispatch; abandoned running children become unknown on owner recovery and cannot relaunch blindly. Dispatcher context masks parent activity/group/lease capabilities: a host child runner must establish its own execution boundary. Parent cancellation notifies workers and returns without awaiting non-cooperative callbacks; late writes remain fenced. JoinChildren, ConfirmChildCancellation, ResolveChildWait and ReturnChildBudget provide separate persisted boundaries; launch alone does not perform them.

Child-group design decision: the group belongs to one parent execution/node/activation and logical group key. Child execution identities derive from that full address plus stable child IDs; sorted IDs define launch/join order, not arrival time. Host codecs/projection provide detached input bytes per child. Group compatibility includes explicit merge, budget and cancellation labels, bounded concurrency, and fail-fast/collect-errors policy. Named allocations are fixed before launch, their sum cannot exceed available parent counters, and invalid/negative/unknown allocations reject before dispatch. They are not monetary reservations: host owns the external reservation port and its evidence. Unused counters return only after a confirmed resolved child; unknown/non-cooperative work retains allocation until explicit recovery evidence. Restart never blindly relaunches running work: it becomes unknown and requires reconciliation or addressed operator action. Cancellation persists a request, stops new launches and notifies active children; only worker confirmation permits canceled, while completed effects/results remain retained. No detached mode or implicit upgrade of inline subgraphs is introduced.

Manual decisions retain the addressed attempt number in addition to source revision/incarnation. Journal validation rejects missing/duplicate decision identities or evidence, zero timestamps/fences, non-increasing source revisions/attempt addresses, decisions for non-unknown attempts, unsupported actions and retry decisions without the exact persisted safe contract/remaining limit. Decision source revisions precede the loaded envelope revision. Manual origin requires persisted provenance. This validates runtime structure, not the truth or authorization of host evidence.

Manual outcome origin must agree with the final decision action and addressed attempt. Starting a new dispatch clears the previous outcome origin while retaining all decisions; automatic retries cannot inherit a stale manual outcome marker.

Attempt history permits another dispatch only after a failed attempt classified retryable under the named safe policy, or an unknown attempt with a persisted addressed manual retry decision. Completed, running and non-retryable prior attempts cannot precede another dispatch. Unknown history is never treated as proof that the external action did not happen.

Migration journal references map current logical activity keys to immutable activity identities in the same execution/activation. Runtime validates every reference and all unresolved records before committing the migrated target. Moving a cursor with unresolved activities requires explicit bindings; dropping, foreign/stale or dangling bindings rejects migration without a target commit. CallActivity and manual resolution use the binding while retaining original input/implementation/retry checks, attempts and outcome identity. Step advance clears bindings; a same-node retry retains them. Unresolved records from earlier activations are corrupt, not safely ignorable. A current step cannot commit while any activity in its activation is unresolved, even when migration changed its node. Completed/definitively failed historical activities remain immutable and need not be rebound unless the target step intends to reuse them.

Each activity record persists its execution ID, node and nonzero activation alongside the logical key. Runtime recomputes the identity from this address, rejects records belonging to another execution or a future activation, and retains historical node/activation addresses across graph migration. A migrated cursor does not rename a successful activity or imply that its external effect is new. Unaddressed legacy records require explicit conversion/import; hashes are never reverse-inferred into fabricated addresses. Fork must not copy the source journal into the target coordination domain.

Child and wait mutations share the same aggregate commit/fencing boundary. Bounded execution may run outside that transaction; only committed outcomes can participate in deterministic merge. Cancellation is a request until the worker confirms, and abandoned external attempts remain unknown.

## Interruption before a durable step commit

Cancellation, parent deadline, stream consumer stop or context-requested handoff before step commit must retain the last committed entry state, effects, cursor and activation. Already committed activity/child outcomes remain in that same aggregate and replay from their original inputs. Do not save a handler's partial output with the old cursor: pure state updates would be applied twice and state-dependent activity inputs could conflict. Restore the entry boundary by decoding detached committed payload bytes, not by keeping a shallow copy of BYOT values that a handler may mutate in place. Cancellation may update segment termination metadata, but must not advance step counters or invent terminal completion. Explicit Suspend/Handoff directives remain separate selected continuation boundaries.

If the durable step commit fails, the returned failed result also describes the committed entry boundary, not an acknowledged partial handler update. Preserve the commit error; do not retry a possibly unconfirmed storage commit just to produce a cancellation result. The result token addresses the last confirmed aggregate; an unconfirmed remote commit may require explicit latest/OCC recovery.

Native PostgreSQL checkpoint/lease/node/owner identities use exact TEXT storage and query arguments, never narrowing varchar casts. Distinct valid IDs with a shared long prefix must not share checkpoints, locks, lease checks, history, prune or deletion targets. Database index/storage bounds can reject oversized values but may not truncate or alias them. Existing narrow schemas require an explicit host-controlled offline schema conversion before using the new storage contract; CREATE TABLE IF NOT EXISTS is not a conversion.

Ordinary RequestLocalHandoff waits for completion only while its caller context is live. If completion and caller cancellation are both observable when returning, caller cancellation takes precedence and returns ErrHandoffNotCompleted wrapping the context error. This reports an unconfirmed wait, not proof that the execution failed to persist its handoff; inspect/resume independently.

Migration ChildGroupReferences maps current logical group keys to original immutable group identities in the same activation. Moving a cursor with an unjoined group requires an explicit binding; missing, dangling, wrong-key or foreign-activation bindings reject before migration commit/node/dispatch. Prepare/RunChildren reuse the original group and require identical plan/capacity instead of new identities or replenished allocation. Wait/cancellation confirmations remain addressed to the original group node and child revision, with a current binding check at the new cursor. Join/cancellation/budget return use that bound group. Normal advancement clears references, same-activation Retry preserves them, and fork resets them. The detached map is part of migration state, not permission to modify child outcomes or ledger. See child-migration-contract.md; budget/cancellation/unknown/fault/chained acceptance must not be inferred from a completed/waiting-sibling fixture alone.

Child group cancellation commits the named request ID/reason before remote notification. Planned/queued children were never dispatched and can be confirmed canceled locally; admission rejects the group after request commit. Running/waiting/unknown children receive a compatibility-labelled host notification with the stable request/child identity. Notification is at-least-once across recovery; the host must deduplicate it. An acknowledgement is not confirmation of stopping. Existing completed/failed results remain unchanged, and a concurrent committed active-child outcome remains authoritative. Requested active children retain unresolved state until a worker outcome or explicit confirmation. Timeout/context cancellation/lease loss never fabricate cancellation confirmation or rollback.

ConfirmChildCancellation is a raw safe boundary requiring the latest parent token and exact node/activation/group/child execution/revision/request identities, with unique decision ID, reason and host evidence. Only requested running/waiting/unknown children can be confirmed. A completed/failed outcome wins over a stale confirmation; confirmation wins over any later outcome from the old dispatch revision. Confirmation records prior state, child/source revisions and decision incarnation/time atomically with canceled state. Runtime validates structure and address, not the truth of host termination evidence. It invokes no codecs, nodes or notifications and does not automatically resume the parent.

A committed cancellation request signals the current child coordinator. RunChildren returns its current settled/unresolved group without awaiting non-cooperative dispatchers and cancels their local contexts; that local signal is not remote termination proof. The parent cannot publish a step/terminal while requested active children remain unresolved. After ownership changes, prior running children become unknown without a second dispatch. Late outcomes from the old coordinator still require its live fence and exact child revision; they cannot replace a confirmation or a new owner's aggregate. Test handlers can remain in memory after worker release; host execution ports must enforce their own remote cancellation/resource policy.

JoinChildren accepts an exact detached group assertion including child revisions and compatibility labels. All children must be settled before merge; waiting/unknown/running children cannot be merged. The pure host merge receives isolated outcomes sorted by child IDs outside the aggregate mutex. Its result and the complete ordered MergedIDs are committed together under the original aggregate revision and current lease fence, after checking cancellation again. A concurrent aggregate mutation rejects the merge without overwriting outcomes. A committed join replays the cached bytes without invoking merge. A failed pure merge or commit retains child outcomes and may require recomputation; this is not an exactly-once external effect guarantee. Partial/forged merge markers are rejected during envelope validation.

The armed aggregate is committed before registration acknowledgement. Event/timer arbitration persists acceptance and exactly one resolution with continuation. The selected early-delivery policy is explicit not-armed with caller redelivery, not an implicit inbox. The optional persistent adapter owns discovery/registration storage; the declared host recovery owner drives polling/scheduling and acceptance without a competing scheduler for the same execution.

Structured child wait resolution is a separate safe boundary: ResolveChildWait addresses the parent token, node/activation/group key, child execution ID and revision, wait ID and unique decision ID. It requires the latest parent revision, matching descriptor and a currently waiting child. Host authentication and meaning of the supplied completed/failed result remain outside core. Runtime persists the result and addressed decision provenance in the same fenced aggregate commit, without state codec, node or dispatcher calls. A stale/duplicate/wrong address is rejected; sibling outcomes/waits remain unchanged. Resolution does not automatically resume a parent or acknowledge transport delivery before its commit. Event/timer arbitration for general durable waits remains a separate capability.

Fork loads an exact raw checkpoint, checks its digest/descriptor, runs the host projection and atomically creates a fresh target with source lineage. Activity identities incorporate target execution ID; source intents, unresolved descendants and live runtime handles cannot be copied. Inspection is a separate operation with no node calls. Live policy and a compatibility-labelled projection are mandatory where opaque state may carry external references.


## Invocation admission and BYOT composition

MaxSteps limits handler invocations in one compute segment. The runner checks admission before NodeStarted and before the handler. Every admitted handler invocation, including a returned error or Retry, consumes one segment step. Completion on the Nth admitted call succeeds; a required N+1th call fails without dispatch. Nonpositive configured values select the default 1000. Resume resets segment counters; it does not reset persistent named budget usage. Persisted durable metadata retains the committed entry count until a step or definitive failure terminal commits. A live recoverable handler error can report the admitted attempt; interruption or a failed step commit returns authoritative entry metadata. Neither case claims an uncommitted attempt in storage.

UseBudget is accounting, not reservation. Empty/invalid UTF-8 names, negative units, negative existing counters and integer overflow return ErrBudgetInvalid without mutation. Missing metadata returns ErrBudgetContext. A valid previously unseen name creates a counter; only compile-time declared limits are checked after a successful node. Zero is a valid no-op. Calls are serialized within one node; host goroutines must not concurrently mutate run metadata. No runtime accounting guarantee establishes provider usage, prices or a pre-dispatch spending cap.

Effect wrappers preserve inner-to-outer order when a pattern changes the base directive. Inline graphs export only newly produced effects at each successful parent boundary. SubgraphSlot requires Contract == InlineSlotContract and retains an ExportedEffects cursor across suspend/handoff; completion clears the slot. Reentry after completion starts a new inner execution. Failed inner computation does not replace a prior confirmed slot; context cancellation restores a saved inner continuation through the parent cancellation boundary. Inline runners have their own counters and graph defaults, inherit ephemeral context bindings, and do not inherit parent RunOptions. Inline execution under a durable activity capability is unsupported and rejected before inner dispatch: distributed durable work must use the child boundary with its own ownership. No parent activity, child or lease capability is passed to an inner ordinary runner.

Stream events are bounded best-effort observations: full buffers drop events immediately, independently of Wait. WaitResult is the authoritative outcome. Delivery is not a committed event log; even a terminal event can be absent. RequestStop cancels execution, while Wait only waits and never changes delivery policy.

BYOT values are immutable across shared boundaries by default. Hosts using mutable maps, slices or pointers supply WithEventCloners for state and reference-valued effects. Pure cloners run synchronously on the producer before publication, must return detached values, and must not panic or mutate their input; only the host knows how to clone its domain. They isolate events, not arbitrary host goroutines mutating a node state. Checkpointers own snapshot detachment; MemoryCheckpointer provides explicit state/effect cloners to model serialized adapters. A serializer Unmarshal must return detached values; WithSanitizer round-trips through its base serializer before mutating a value during Marshal. Runtime metadata collections are always copied by memory storage. Inline mapIn/mapOut/loadSlot/storeSlot share the same host ownership obligation; runtime does not infer deep copy of arbitrary domain types.

Inline slot format transition is explicit: persisted nonempty slots without the current Contract marker are rejected with ErrInvalidSnapshot before inner execution. Drain old executions before upgrading, or perform a host-owned offline conversion that reconstructs state, cursor and the exact number of already exported effects from authoritative evidence. Missing evidence requires rejecting or discarding that execution; the runtime never assumes a missing effect cursor means zero. Empty slots are valid fresh executions. AsNode uses the same inline boundary and effect/capability policy as SubgraphNode.

## Child outcome reconciliation

Unknown child outcomes have an addressed raw boundary described in [child outcome resolution](child_outcome_resolution.md). It accepts opaque completed/failed results under exact parent/child revisions and live fencing. Original group identity and current migration binding are both checked. Resolution does not dispatch, decode, merge, resume, return budget or infer absence of remote work. Decision provenance and terminal child outcome commit atomically; stale parent tokens never silently advance to latest.

## Persisted retry schedules

[Activity retry scheduling](activity_retry_scheduling.md) defines explicit fixed/exponential configuration, bounded jitter and compatibility-labelled host not-before hints. The deadline and its provenance commit once with the prepared retry, survive restart and never cause dispatch through discovery. Old active Delay-only policies are rejected; recovery does not guess schedule semantics.

State and accumulated effects have independent mandatory compatibility labels; changing either requires the corresponding explicit pure transformation. See [durable lifecycle](durable-lifecycle.md). Activity/child outcomes are immutable throughout these representation changes.


### Cancellation ownership and cleanup

Each stream stop cancels its own context, including before session registration.
Completed and rejected handles cannot stop a later run with the same thread ID.
Every execute-owned context is canceled on return, after heartbeat shutdown and
session completion/unregistration; the caller's context remains independently owned.
An active-run Canceled or DeadlineExceeded node error, raw or wrapped, checkpoints
continuation. A node-local timeout with a live run remains a failure. Lease loss
and explicit handoff preserve their existing precedence; durable interruption
restores the committed entry rather than an aliased pre-node value.

Lease release and post-run terminal prune/delete use detached contexts bounded to
five seconds per operation, preserving caller context values. Rejected resume
admission releases under the same bound. Failures wrap ErrRunCleanup and the
original cause and are joined with execution errors; a returned completed or
failed RunResult still describes execution. Busy DeleteIfIdle is a cleanup error.
RequestLocalHandoff acknowledges execution/checkpoint completion; Start and stream
WaitResult also report subsequent cleanup. Durable Start/Resume and streams,
DeliverWait, CancelWait, activity/child resolution and fork/import/rollover
preserve a committed result/token alongside a cleanup error. Such an error does not mean the preceding durable write failed. Timeouts
require context-cooperative adapters; no background retry or detached goroutine is
used to pretend uncooperative I/O completed.

All failed live RunResults close Segment with a UTC EndTime and EndReason fail.
The Retry-route restriction is retained: AddRetryRoute requires an
AllowNoOutgoingRoute node, which cannot also route Completed. Split mixed behavior
into separate graph nodes; directive Retry provides no implicit I/O retry safety.


### Collection ownership and callback lifetime

CollectEventsAndWait returns a synchronized, detached snapshot of the events
collected so far. After an early canceled return, the drain may still append to
its private collection; it cannot change the returned slice storage or header.
This is a shallow copy of RunEvent values. Arbitrary BYOT state/effect references
still require host ownership or WithEventCloners when events are produced.
Successful collection includes every delivered event and reports Wait's error;
cancellation requests stop and preserves the collection context's error without
waiting indefinitely for an uncooperative producer.

ConsumeEventsAndWait callbacks run on its drain goroutine. If collection context
cancellation causes an early return, callbacks already in progress or queued on
the event source may continue afterwards until the source closes. Returning from
the function is not a callback join: keep captures alive and synchronize concurrent
access, or arrange your own completion signal. Callbacks must not panic or block
indefinitely. BeginStreamCollect transfers a complete private collection through
its result channel; AwaitStreamCollect can return early without transferring an
unfinished slice. WaitResult remains execution authority because the bounded event
stream may drop a terminal event. None of these helpers provides durable delivery.
