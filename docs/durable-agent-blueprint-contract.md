# PostgreSQL recovery blueprint

Status: executable in `examples/durable_agent`; actual PostgreSQL acceptance in
its integration test. This contract was fixed before implementation.

## Boundary and host types

A separate `examples/durable_agent` Go module uses the PostgreSQL ExecutionStore
with an explicit wait capability profile. The executable owns concrete parent
state, child input/result and effect types, JSON codecs and descriptor labels.
Model/tool interfaces and deterministic fake implementations belong to this
example. Core acquires no Agent, Message, Tool, Prompt, Token or Money schema.

The fake external service survives worker replacement, has an idempotency map
and counts dispatch calls separately from applied writes. Runtime activity
identity is passed as the downstream key; honoring that key is the fake
service's contract. Recorded usage travels in the BYOT outcome so recovery can
account it at the acknowledged step boundary. `UseBudget` is accounting, not a
payment reservation. The host authenticates approval deliveries and operator
evidence before invoking runtime APIs.

The example creates and cleans up only its own isolated PostgreSQL schema.
Every recovery phase constructs a fresh worker, store adapter and connection
pool against that schema. An existing in-memory execution store or a surviving
worker-side journal cannot satisfy backend recovery acceptance.

## Scenario and failure boundaries

1. A fake model supplies a deterministic plan. An activity performs the external
   tool write. Its outcome reaches PostgreSQL, then the adapter loses the reply
   to the runtime. The failed invocation must not observe committed success.
2. A fresh worker loads the authoritative sealed head and resumes. The activity
   returns its stored outcome as replay, without another dispatch. Parent state,
   effects and usage are applied once at the step checkpoint.
3. `Await` arms host approval. A bounded host component calls indexed discovery;
   it owns transport, polling and redelivery. Approval and a due timer race under
   the documented first-committed policy. Duplicate approval replays, and the
   losing timer records no continuation. A new worker uses the latest core-issued
   token to continue. No scheduler daemon is added to core.
4. The parent launches two isolated children. Each child completes its own
   durable execution before the dispatcher returns. A deterministic barrier
   fixes that order. A parent outcome publication is rejected after child work
   finished, leaving running/unknown parent ownership without permission to
   blindly relaunch either child.
5. A fresh worker attempts recovery and exposes unresolved child work. The host
   inspects the independently persisted child terminals and submits addressed
   `ResolveChildOutcome` decisions for each unresolved child. It uses current
   parent tokens after every publication; stale decisions cannot replace outcomes.
6. Parent recovery returns child usage allocations, joins in stable child-ID
   order and commits its final state/effects. A further fresh worker replays the
   final token. Model/tool/child dispatch counters remain unchanged.

The children use their own execution identities, codecs and durable stores;
inline subgraph execution is not a substitute. Unknown work retains its budget
allocation until the host can justify a recorded resolution and usage return.
Timeout/lease expiry alone never justify a successful child result.

## Trace and resource contract

The host installs the provider-neutral lifecycle adapter and an in-memory OTel
test exporter. Core remains SDK-free. Initial checkpoint, activity replay,
approval delivery, child resolutions, join and terminal replay share the persisted
trace relationship across fresh contexts/pools. Runtime identities are trace
attributes; payload, operator evidence and arbitrary error text are absent.

The example bounds poll/redelivery rounds, graph steps, child concurrency and
operation count. It uses no real model credentials or remote LLM requests. Test
exporter storage is bounded by that finite scenario; production exporter queues
and shutdown remain host responsibilities.

## Executable acceptance

AAA integration tests must assert all of the following from actual PostgreSQL
state and observable fake-service behavior:

- Both deterministic publication faults were reached at their named boundaries.
- The tool write and both child actions dispatched once; actual applied writes
  match their expected identities. Dispatch count and write count are separate.
- Duplicate approval called neither matcher nor continuation again; its timer
  loser did not change accepted state/cursor.
- Unknown-child recovery used exact addresses and current parent revisions,
  invoked no dispatcher and allowed one acknowledged ordered join.
- Final state, ordered child result, effect list and named budget counts match
  explicit expected host values. The final token addresses the stored completed
  head, and terminal replay returns that exact token without additional work.
- Required spans show the documented parent/trace relation across replacement
  workers. Failed publication has no committed-success observation; later
  authoritative replay is distinct from dispatch.
- Every replacement worker used a distinct pool/adapter instance; cleanup leaves
  no owned schema or active worker behind.

The integration command fails when its required PostgreSQL configuration is
missing. A skipped test, memory smoke, exit code or printed success line alone
is not backend evidence. CI runs this scenario with its PostgreSQL service; the
ordinary examples remain a separate smoke gate.
