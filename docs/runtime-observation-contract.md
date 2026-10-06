# Runtime observation contract

Status: task27 implementation contract. Observations describe runtime-owned
boundaries. They are never an execution journal, claim, delivery acknowledgement
or authorization to repeat external work. Core remains BYOT and imports no OTel
SDK. The host owns model usage, prompts, business evidence, downstream idempotency,
transport, scheduling and optional provider-specific telemetry.

## Events and publication boundaries

Replace the old three-method LifecycleObserver with one optional
ObserveLifecycle(context.Context, LifecycleObservation) method. No deprecated
observer adapter or parallel legacy callback path remains. Ordinary handoff,
resume rejection and checkpoint soft-error observations use this same contract.
The existing process-wide installation remains explicit and synchronized.

Operation and stage are closed provider-neutral classifications. Runtime
operations include execution/terminal, activity dispatch/replay/reconcile/retry,
child launch/resolve/join/cancel, wait arm/winner/cancel, lease loss,
migration/fork, rollover and explicit retention. Stages distinguish started,
failed, committed and replayed. A dispatch return is not an outcome commit.
Only an acknowledged successful atomic publication emits committed. A failed or
lost-ack commit emits failed; a later authoritative recovery may observe its
already committed outcome as replayed. Do not invent an exactly-once telemetry
guarantee: a process can die after commit before observing it.

For activities, started is published after the running attempt is acknowledged,
immediately before host dispatch. Replayed never invokes dispatch. An outcome
publication is committed even when the persisted outcome describes a definitive
host failure or a pending retry; bounded runtime codes distinguish that outcome
from successful completion. A failed journal publication has no acknowledged
Revision. Retry committed is emitted only for an acknowledged pending deadline.
Activity identities, attempt numbers and revisions describe that boundary. Host
reconciliation has its own started/failed/committed operation; successful host
evidence without a successful journal acknowledgement remains failed.

Child group preparation, running intent and cancellation request publication are
separate from host dispatch/outcome and notification. ChildLaunch committed with
child_running describes acknowledged intent; started precedes host dispatch.
ChildResolve committed describes the recorded child outcome, including unknown
or failed outcomes. A queued intent followed by a failed running write emits
launch failed, never a successful launch. Join committed requires acknowledged
merge publication; replay returns the stored merge without calling the host.
Cancellation committed describes the request, never remote termination; a later
notification failure remains separately observable. WorkID addresses the group,
ChildID the local spec, ChildExecutionID the isolated execution. Parent envelope
revisions address the atomic publication, not a child-store checkpoint revision.

WaitArm committed describes the aggregate arm, independently of external
registration; registration failure cannot revoke it. WaitWinner describes an
acknowledged delivery decision with a bounded accepted/lost/unmatched/canceled
code, so a non-winning decision never impersonates an accepted winner. Identical
delivery/cancellation decisions are replayed without a new publication. Public
addressed mutations restore the validated source carrier before observations or
host callbacks. They never trust a telemetry carrier as execution authority.

Observations contain operation/stage, runtime execution/segment/node/activity/
child/work/decision identities and addressed source/target revisions where
available. They contain no T/E, raw input/output, evidence, arbitrary error text,
or model/provider payload. Optional structured diagnostics use bounded runtime
codes; business content remains host-owned. Segment and parent-child identities
correlate traces; none of those arbitrary IDs become default metric labels.

Migration observes the validated source before pure transformations and marks
the migrated head committed only after its write is acknowledged. Fork observes
historical source and new target identities separately: target Revision starts
at one and is not comparable with SourceRevision from another execution.
Rollover carries the transferred source Revision and the new TargetRevision;
identical receipt replay is explicitly replayed. Retention uses the explicit
core RetainExecution helper over the optional storage capability: successful
maintenance may keep the head revision unchanged. Direct calls to a raw storage
capability bypass runtime observation. Retention context is host supplied,
because payload removal may leave no execution carrier to restore. No implicit
full payload read or lease acquisition is added solely for telemetry.

Execution started denotes an invocation, terminal replay denotes a validated
cached terminal without node dispatch. Lease loss is observed once at owned
session completion, including initial preparation and manual operations. The
heartbeat stops and release is attempted before the callback. A bounded session
slot copies its latest validated source addresses/restored context under a short
mutex, released before observing. Commit/release ErrLeaseLost and heartbeat loss
mark the same session diagnostic; no extra observer worker is created. Before a
source is available, the acquisition context and execution ID are sufficient;
telemetry does not load payloads solely to enrich a diagnostic. This reports loss
of runtime authority, not absence of an external effect or remote termination.

## Callback policy and resources

The observer receives a value-only event synchronously after copying the observer
slot under its lock, then releasing that lock. Returning from the callback is not
an external acknowledgement and never establishes a commit. There are no
internal observer queues, workers, retained operation-span maps or asynchronous
fire-and-forget goroutines. The no-observer path invokes no user callback.
Activity observations copy addresses under the checkpointer mutex and invoke the
observer after releasing it. Observer callbacks never run under that mutex.

Observation panics are contained; callback failure never changes the run result,
budget accounting or persistence protocol. Callbacks must be prompt and respect
context cancellation. A non-cooperative blocking callback blocks its caller;
there is no promise to preempt arbitrary Go code. This keeps resource use bounded
without claiming that observer latency cannot delay a worker or consume lease
lifetime. Host exporters that need buffering own explicit bounded capacity,
drop/error policies and shutdown. Tests must cover panic isolation, disabled
telemetry semantic equivalence and a blocking callback with no hidden goroutine
or unbounded queue growth.

## Trace carrier and replay

Execution RunMetadata persists the host bridge's detached carrier at checkpoint
boundaries, including the initial durable checkpoint before external work.
Durable Resume restores it before node execution and cached terminal
or armed-wait replay, including a fresh worker/context. OTel's default bridge
propagates only W3C traceparent/tracestate, never arbitrary baggage. Host-supplied
bridges may explicitly opt into other metadata and own its privacy policy.

The selected default OTel contract is remote-parent continuation: a new node or
lifecycle observation span uses the restored persisted span context as parent,
retains trace identity, and has a new span ID. The persisted carrier is trace
correlation only; seal/descriptor/profile/lease/OCC remain authority. Lifecycle
spans explicitly identify replay as replay, rather than simulating a new dispatch.
Lifecycle observations create bounded-lived event spans; no map keeps started
spans alive until potentially missing completion events. A host may add links
under its own integration contract without changing execution ownership.

## OTel privacy and cardinality

Default metric dimensions are bounded operation and stage (and any explicitly
closed runtime code classification). Execution/thread/child/decision/segment IDs,
node/graph names, arbitrary reasons and error messages are excluded. Thousands
of IDs with identical dimensions must produce the same metric series. Trace
attributes may include runtime identities; payload/evidence and raw error text
are absent by default. User node/graph/custom dimensions require an explicit
host-defined bounded cardinality policy, not an automatic attribute export.

## Blueprint, performance and gates

A separate PostgreSQL example module demonstrates BYOT state/effects, deterministic
fake model/tool interfaces, a durable external action, host-input wait, isolated
children, addressed unknown-child recovery and budget accounting with trace
correlation. Independent workers/pools recover deterministic faults; fake remote
operation counters prove no repeated external side effect. Its bounded example
poll/redelivery component sits above indexed discovery and never moves a daemon
into core. Real provider credentials or remote LLM requests are unnecessary.

Meaningful AAA integration assertions check external counts, child outcomes,
join, exact final token and trace relation. A memory-only smoke run cannot prove
backend recovery. Root and changed module race/lint/backend gates remain required.

CI must fail when any named benchmark is missing or produces no measurement.
Hot node path (zero/five middleware), streaming, journal/fan-out growth and indexed
discovery workloads have explicit discovery of benchmark names. Allocation limits
have recorded workload, Go/toolchain assumptions and a reproducible chosen
baseline/threshold; timing is reported, not enforced as an absolute SLA on a
shared runner. Negative fixtures prove absent-benchmark and allocation-overrun
failures. Storage benchmarks retain the task25/task26 workloads and scope.

Documentation distinguishes ordinary/durable, PostgreSQL/standalone Redis,
inline composition/isolated children, event stream/token stream and accounting/
reservation. Supervisor helpers describe actual control flow. Compile-time tool
filtering or installed hooks are claimed only when implementation proves them.
