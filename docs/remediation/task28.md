# Task 28 execution journal

Source: `.cursor/tasks/task28-flowy-review-remediation.md`, review base
`d605794b4dc71e0c48bf3e718bc9cc2f02d1930c`. Historical review remains unchanged.

## Sequential plan and coverage

Each task requires its contract, implementation/disposition, relevant AAA regression
tests and current documentation before two independent reviews. Completeness must
be 100%; correctness must have no open findings. Reviewers inspect the final diff
and report evidence and limits. Commit only after both gates; record its SHA in the
next journal update (a commit cannot contain its own SHA).

| Task | Depends on | Source requirements | Acceptance criteria |
|---|---|---|---|
| 01 Release isolation/recovery | — | F01, F02, D59, documentation 8 | Exact file/ref scope; original checkout/index untouched; failures before/after tagging and push are recoverable; none/partial/unknown publication distinguished; exact retry; portable edits and explicit major-version gate; local-origin fixtures |
| 02 Persistence preparation | 01 | F03, D05, D08, D09 | Shared Save/SaveWithOutbox preparation; transform/reject parity, hook count and metadata-only encoding; atomic capability contract; unchanged persistence on rejection |
| 03 Panic recovery | 02 | F04, D10 | Panic cause/input preserved, no reducer/effect success; documented callback boundaries |
| 04 Session cancellation/cleanup | 03 | F05, F06, F07, D07, D12, D62 | Incarnation-scoped stop; owned contexts released; deadline checkpoint parity; bounded cleanup; deterministic lease fixtures and terminal metadata |
| 05 Stream collection | 04 | F08, D14 | Detached immutable early-return collection, race-clean cancellation and complete successful drain; callback lifetime documented |
| 06 Storage/replay admission | 05 | F09, F10, D36, D44, D54, D55, D56 | Structural admission before mutation; UTF-8 equality fields rejected before effects; typed errors and exact valid replay; adapter metadata/TTL boundaries and JSON domain |
| 07 Runner/composition/graph | 06 | D01, D02, D03, D04, D06, D11, D13 | Explicit persistence profiles; production test scaffolding removed; inline continuation contract; reserved name/diagnostics; advisory fencing boundary |
| 08 Patterns/bindings/observation | 07 | D15, D16, D17, D18, D19, D20, D21 | Explicit naming/limits, callback validation and ownership; bindings concurrency; observation instance/global disposition and privacy |
| 09 Activities/results/growth | 08 | D22, D23, D24, D25, D26, D27, D28, D29 | Lost-ACK recipe, callback ownership, taxonomy/retry/versioning contracts; rollback dual-error authority; measured growth and rollover guidance |
| 10 Children | 09 | D30, D31, D32, D33, D34, D35, D37, D38 | Naming disposition; correct cloning; capacity/unknown/FSM contracts; post-commit decode fault test and migration/recovery recipes |
| 11 Waits/lifecycle/discovery | 10 | D39, D40, D41, D42, D43, D45, D46, D47, D48, D49, D50, D51, D52, D53, D57, D58; documentation 1, 3 | Immutable delivery/winner contract; correct clones; lifecycle identities/digests/retention authority; discovery partial progress; source-payload independence |
| 12 Current docs/examples/validation tooling | 11 | D60, D61; documentation 2, 4, 5, 6, 7 | Executable BYOT start/install, six-module index, canonical ownership/error/recovery contract, current cookbook and truthful fuzz/benchmark gates |
| 13 Final verification | 12 | Source DoD 1–7; all F/D/documentation items | Fresh six-module lint/race, targeted fault tests, required isolated live adapters/blueprint, applicable benchmark manifests, clean consumer installability, release fixtures; final independent reviews |

## Task 01 contract (before implementation)

Release preparation runs in an isolated local clone of the exact source HEAD.
Only tracked module `go.mod` files may change. The original branch, index,
worktree and tags are never mutated; untracked files are never copied. Root and
module release tags are the complete publication allowlist. Publication uses
atomic push; unsupported atomic capability fails without a non-atomic fallback.

The remote root release tags determine the next version, never unrelated local
tags. Preparation persists an immutable manifest (destination, version, commit,
exact refs) before any push. On push failure, query the exact remote refs and
report none, partial, unknown, conflicting or complete publication. Retain the
prepared clone and manifest for exact retry; never delete remote/local source
refs based on a network error. Retry verifies the manifest against its prepared
clone and publishes the same commit/version. Preparing a new version while a
failed attempt is pending is an operator error: use the printed resume command.

The editor is portable Python 3; Go formats module files. Releases reaching v2+
are rejected until semantic import paths and all consumers are explicitly migrated.
Successful publication still requires the separately documented clean-consumer
installability gate; fixture success is not a real release.

## Progress

- Task 01 accepted. Changes: isolated release
  preparation, portable module editor, immutable retry manifest, exact atomic
  refs, remote publication classification, major-version guard and runbook.
- D59 disposition: portable Python/Go editing replaces BSD sed; v2+ rejected until
  explicit semantic import migration. Clean-consumer fixture checks prepared root
  and adapter artifacts without replaces; actual six-module installability remains
  a task 13 gate.
- Before-fix disposable-origin evidence: original script publishes both an
  untracked fixture file and unrelated `scratch-local`; rejection leaves detached
  HEAD and unpublished release tags. Log: `/tmp/flowy-task28-release-before.log`.
- Task 01 completeness reviewer `/root/task01_completeness`: 100%, 10/10 criteria;
  final independent 16 fixtures PASS (29.931s). Correctness reviewer
  `/root/task01_correctness`: initial two P2 findings (incomplete manifest and
  commented replace block), both fixed with regressions; final PASS, no open
  findings, 16 fixtures PASS (28.911s), extra-ref rejection probe PASS.
  Reports: `reviews/task01-completeness.md`, `reviews/task01-correctness.md`.
- Primary final checks: 16 release fixtures PASS (29.388s), shell syntax,
  Python compile and `git diff --check` PASS. All release origins were disposable
  local repositories; no real release/push performed. Commit SHA follows in task 02.
- Tasks 01–05 accepted below; tasks 06–13 pending.

## Task 02 contract (before implementation)

Task 01 commit: `b2935d8` (`fix: release recovery`).

Ordinary Save and transactional SaveWithOutbox share one preparation function:
validate the runtime/domain state, then run BeforeSave once in registration order
to produce persisted state. Runtime results retain domain state; persisted state
is not revalidated as domain state after redaction/encoding. A metadata-only
handoff status patch reuses the prepared/persisted snapshot, including recovery
from Load; it does not run BeforeSave or domain validation again.

BYOT copies remain value copies: an interceptor must replace reference-backed
state with detached storage representation rather than mutate shared domain
resources. Flowy cannot generically deep-copy arbitrary host resources.

Preparation rejection is a hard error with the original cause, before Save,
SaveWithOutbox or enqueue. SkipOnSaveError applies only to adapter Save failures;
known structural/OCC/lease/capability rejections are hard errors. Other arbitrary
adapter errors remain opt-in degradation (the Checkpointer interface cannot prove
they are infrastructure errors); this is explicitly documented, never resumable
without a successful commit. Transactional handoff errors are always hard errors.

Default outbox behavior retains the documented non-atomic multi-phase FSM when
transactional capability is absent. `WithAtomicHandoff` requires both a
TransactionalCheckpointer and TransactionalHandoffOutbox, rejecting unsupported
invocations before lease acquisition, node dispatch, preparation or persistence.
This checks capability, not whether arbitrary host implementations honour it.
DurableRunner explicitly rejects this ordinary snapshot/outbox option before
session acquisition or initial commit, rather than silently accepting it.

Acceptance: transform/reject/invariant parity; non-idempotent encoding exactly
once across successful/orphaned/recovered metadata patches; unchanged history
and outbox on rejection; fail-closed skip policy for contract errors; atomic
preflight across entry points and positive transaction coverage.

Task 02 dispositions:

- F03: implemented shared preparation before both Save and SaveWithOutbox;
  fallback/recovery metadata patches retain prepared storage state.
- D05: implemented explicit WithAtomicHandoff capability preflight; documented
  multi-phase default and durable-profile rejection.
- D08: tightened policy to reject preparation and known contract errors; retained
  explicitly opted-in suppression of other adapter Save errors because their
  arbitrary BYOT error values cannot prove an infrastructure classification.
- D09: clarified domain/persisted state and value-copy ownership; hooks run after
  domain validation and metadata changes cannot re-encode persisted state.
- Before-fix regression overlay at task 01 HEAD: expected FAIL for ordinary hook
  count 2 and transactional hook count 0, including rejecting interceptor bypass.
  `/tmp/flowy-task28-preparation-before.log`.
- Initial targeted race PASS (1.884s). Initial lint had 10 formatting/test-structure
  findings; all corrected. Final lint PASS, 0 issues; full root race PASS, exit 0
  (root 11.338s), `git diff --check` PASS.
- Task 02 accepted: `/root/task02_completeness` 100%, 10/10; correctness reviewer
  `/root/task02_correctness` found an atomic admission bypass in DurableRunner,
  fixed with explicit rejection and four-entrypoint regression. Final correctness
  PASS, no open findings; independent targeted race PASS (1.747s).
  Reports: `reviews/task02-completeness.md`, `reviews/task02-correctness.md`.
  Commit SHA follows in task 03. No live adapters were claimed as verified.

## Task 03 contract (before implementation)

Task 02 commit: `cffaa7b` (`fix: checkpoint preparation`).

RecoverMiddleware returns the supplied input state and a nonnil error on panic.
Panicked errors remain discoverable with errors.Is; other panic values retain
their formatted cause. The returned directive is failure, never success. Direct
wrapper and graph execution must preserve cause and nonzero state; graph reducer,
effect commit and checkpoint save cannot run as though the node succeeded.

Middleware registration order is outermost first. Recover catches only callbacks
inside its wrapped node/middleware chain, including synchronous callbacks called
by a node. The surrounding runner's reducer/routing/persistence/publication
are separate boundaries. Shared mutable input or remote
effects already performed by a callback cannot be rolled back by recovery; a panic
does not prove an external effect was not dispatched. No blanket recovery/retry
is introduced outside this node boundary.

Task 03 dispositions and evidence:

- F04: named return slots preserve nonzero input and panic error; error-valued
  panics retain errors.Is identity; recovery returns a failure directive. Replaced
  the weak zero-state graph test with direct cause/input, graph no-success,
  mutable-input and middleware-order regression coverage.
- D10: documented the surrounding runner/node call-chain boundary, outermost-first
  middleware order and host ownership of mutations/external outcomes. Retained
  separate callback contracts rather than adding blanket recover/retry.
- Before fix: direct wrapper returned state 0, zero directive, nil error; graph
  lost cause and returned invalid-directive error. Expected FAIL recorded in
  `/tmp/flowy-task28-panic-before.log`.
- Final targeted race PASS (2.023s); lint PASS, 0 issues; diff check PASS.
  Independent completeness `/root/task03_completeness`: 100%, 10/10; independent
  targeted race PASS (1.901s). Independent correctness
  `/root/task03_correctness`: PASS, no open findings; targeted race PASS (2.012s),
  diff check PASS. Reports: `reviews/task03-completeness.md`,
  `reviews/task03-correctness.md`. Task 03 accepted; commit SHA follows in task 04.

## Task 03-to-04 handoff (historical)

Task 03 commit: `fca533d` (`fix: panic recovery`).

Next sequential scope: F05, F06, F07, D07, D12, D62. Tasks 01–03 are accepted
and committed. No task 04 implementation or acceptance is claimed. Go/lint caches
for the continuation: `GOCACHE=/tmp/flowy-task28-go-cache`,
`GOLANGCI_LINT_CACHE=/tmp/flowy-task28-lint-cache`; existing module cache was readable.
The only post-commit change is this journal handoff, to include in the next
accepted task commit. Final six-module/live-backend/installability gates remain
pending; no overall goal completion is claimed.

## Task 04 contract (before implementation)

Stream cancellation belongs to its own context incarnation, never a thread-ID
lookup. Completed/rejected handles cannot cancel newer sessions; stopping before
registration still cancels that handle's execution. Every execute-owned context
is canceled on return, after heartbeat stop and session completion/unregistration.

Canceled and DeadlineExceeded node errors enter cancellation checkpoint handling
only when the active run is canceled. Wrapped errors have the same semantics;
local node deadlines with a live run remain node failures. Lease loss wins over
node cancellation; explicit handoff keeps its committed-entry restoration path.

Release, prune and delete cleanup uses bounded detached contexts (5 seconds per
operation). Cleanup failures are returned as ErrRunCleanup with their original
cause, joined with the execution error. RunResult remains the execution outcome:
cleanup error does not retroactively undo completion or claim failed persistence.
RequestLocalHandoff acknowledges execution/checkpoint completion; final Start or
Stream WaitResult also reports subsequent cleanup failure. Busy DeleteIfIdle is
an explicit skipped cleanup outcome through the same error, never a warning-only
success. Rejected resume admission also releases its acquired lease within a bound.

Failed RunResult metadata always ends its segment with a UTC EndTime and EndReason
fail. Retry-only routing restriction is retained: AddRetryRoute requires an
AllowNoOutgoingRoute node, which cannot also use Completed routing. Mixed behavior
must be split into separate graph nodes; no implicit I/O retry is added.

D62 fixtures will freeze lease storage time and control loss using explicit
barriers/triggers; wall-clock TTL expiry cannot decide whether a session is live
before the handoff request. Both stream and synchronous variants are covered.

Task 04 cleanup propagation includes every durable session owner: Start/Resume,
stream preparation/execution, wait delivery/cancellation, activity resolution,
child wait/outcome/cancellation resolution, and fork/import/rollover. Their returned
committed result/token is preserved alongside ErrRunCleanup. Child dispatch's
separate finish-store boundary belongs to D38/task 10; adapter transaction rollback
is a separate storage boundary, not post-run release/prune/delete.

## Task 04 dispositions and verification

- F05: removed thread-ID consumer-stop lookup; each stream cancels its owned
  context. Regressions prove stale completed and rejected handles cannot stop a
  later run, and a barrier proves stop before execute registration.
- F06: defer cancelRun immediately after context creation. Every return releases
  the execute context; heartbeat stop and session completion/unregister run first.
  Captured-context tests cover End, error, Suspend and Resume with a live parent.
- F07: both active-run cancellation errors, raw/wrapped, enter checkpoint handling.
  Start/Stream/Resume/ResumeStream parity and a live-parent local-deadline failure
  are tested. Durable interruption/deadline tests retain committed entry state,
  effects, cursor and activation; the detached restore read is bounded too.
- D07: bounded release/prune/delete, original causes joined under ErrRunCleanup,
  every durable session owner propagates finish error without discarding a known
  result/token. Consumer-stop normalization preserves cleanup errors. Busy delete
  returns its explicit cause and leaves the snapshot. Legacy warning-only cleanup
  and the unused threadID parameter were removed.
- D12: retained Retry-only compile restriction with explicit contract rationale:
  mixed Retry/Completed behavior uses separate nodes; no implicit I/O retry.
  Failed live results close their segment with UTC EndTime/EndReason fail; already
  ended failure metadata is preserved. Cached durable replay retains committed
  metadata rather than inventing a new termination time.
- D62: frozen storage clock and controlled Renew/ownership trigger replace both
  timing-sensitive lease fixtures. A barrier holds the session until loss/handoff
  is ordered; another owner's fence is retained. Removed the old permissive race
  helper. A further full-race failure exposed an unrelated rapid-loop cancellation
  fixture: cancel now precedes Stream, matching its sync twin and preserving the
  terminal event/reason assertions.

Verification history: before fixes, targeted probes reproduced F05/F06/F07.
The first new targeted run was interrupted after finding a fixture that incorrectly
expected a stopped stream's successful checkpoint to return Canceled, and a
release spy that failed to release before testing delete-policy failure. Both
fixtures were corrected; local-stop success still returns nil Wait by contract.
First full root race FAIL is retained at `/tmp/flowy-task28-task04-race.log`: old
busy-delete expectation and the rapid-loop event fixture above. Neither is hidden
by accepting a lucky repeat. Final fresh full root race exited 0 (10.010s, all
packages PASS), log `/tmp/flowy-task28-task04-race-final.log`. Final lint exited 0,
0 issues, log `/tmp/flowy-task28-task04-lint-final3.log`; diff check PASS.

Independent acceptance: `/root/task04_completeness` 100% (10/10), targeted race
PASS 1.801s and repeated count20 PASS 4.021s; `/root/task04_correctness` PASS,
no open findings, independent race count3 PASS 1.833s and count2 PASS 2.009s.
Reports: `reviews/task04-completeness.md`, `reviews/task04-correctness.md`.
Task 04 accepted for commit `fix: stream ownership`; SHA follows in task 05.
Tasks 05–13 remain pending. Six-module, live-backend and final
installability/release acceptance remain task 13 gates.


## Task 05 contract (before implementation)

Task 04 commit: `d401897` (`fix: stream ownership`).

F08/D14: CollectEventsAndWait owns its collection behind synchronization and
returns a detached slice snapshot even if collection cancellation returns before
background callbacks/drain/Wait finish. A successful drain returns every delivered
event and Wait's error. Cancellation keeps the existing ctx.Err/join semantics,
requests stop, and never waits forever for a producer or callback that ignores it.
Detached means independent slice storage/header, not deep cloning arbitrary BYOT
state/effects; WithEventCloners remains the host's explicit element ownership hook.
ConsumeEventsAndWait callbacks may continue after a canceled call returns; callers
must keep captured data alive and synchronized until their own completion signal,
or use BeginStreamCollect/AwaitStreamCollect to receive a completed collection.
WaitResult remains execution authority; terminal events may be dropped. No hidden
queue, forced callback termination, or durable delivery guarantee is introduced.
Acceptance: buffered closed-channel early-cancel race probe, stable detached
returned storage, eventual drain/Wait, a deliberately held producer proving early
return, and complete successful collection/Wait errors; run race and lint then two
independent reviews before committing.


## Task 05 dispositions and acceptance

F08: callback appends and snapshot creation use the same mutex; returned events
are cloned while locked, so early return cannot race with the captured header or
share mutable slice storage with late appends. Successful full collection retains
all delivered events/order and Wait's cause. Cancellation semantics are retained,
including non-blocking return for an open producer or held callback.
D14: retained bounded best-effort event delivery and WaitResult authority. GoDoc
and runtime contract explicitly describe callback lifetime, synchronized captures,
shallow BYOT values and the complete Begin/Await ownership transfer. No queue or
forced join was added.

Before-fix race probe exited 1 with the captured append/read DATA RACE at
`/tmp/flowy-task28-task05-before.log`. Final parent helper race count10 exited 0
(4.366s), `/tmp/flowy-task28-task05-race.log`; lint exited 0, 0 issues,
`/tmp/flowy-task28-task05-lint.log`; diff check PASS. Four new regressions cover
closed buffered early cancellation and caller storage mutation, held-open producer
with eventual drain, ordered complete collection/Wait cause, and held callback
lifetime across canceled return.

Independent acceptance: `/root/task04_completeness` reviewed only Task05 scope,
100% (8/8), independent race count3 PASS 2.154s; `/root/task04_correctness`
Task05 PASS, no open findings, independent helper race count5 PASS 2.970s.
Reports: `reviews/task05-completeness.md`, `reviews/task05-correctness.md`.
Task05 accepted for commit `fix: stream collection`; SHA follows in Task06.
Tasks06–13 and final six-module/backend/installability gates remain pending.


## Task 06 contract (before implementation)

Task 05 commit: `714f5ed` (`fix: stream collection`).

F09: snapshot storage admission and decode share a pure metadata validator: nonempty
UTF-8 ThreadID/execution pointer and positive stored revision. Save chooses its new
revision before validation; caller snapshot.Revision is not an OCC authority.
Reject before state codec/cloner, writes, TTL changes or transactional enqueue.
Memory test helper follows the same identity/revision contract. EncodeRecord
requires JSON-valid state bytes; Save never calls arbitrary host Unmarshal merely
to validate headers. PG's JSONB extra wire limits remain explicit and backend
errors cannot publish a partial head/outbox.

F10: equality/provenance strings must be valid UTF-8 before backend/store/notify
calls. Do not canonicalize malformed bytes or relax exact replay equality. Cover
invalid byte/truncated multibyte, real U+FFFD and valid Unicode for wait cancel,
child cancel and budget-return, including reload and current group assertions.
Inventory other Reason/Error surfaces: exact recorded fields reject malformed
text; audit-only runtime error rendering can be lossy and is not an identity.

D36: document each child decision's own namespace and token gate: cancellation
request replay may redeliver the same notice; budget replay reads the stored return
without returning units twice; outcome replay uses its recorded addressed decision. Wait-resolution and
cancellation-confirmation duplicates reject changed child revision; lost ACK is
resolved by reading the addressed persisted record rather than redispatch. No global UUID ledger or implicit latest token.
D44: CancelWait deliberately terminates the execution with failure. Rename the
wait-delivery observation boundary to LifecycleWaitDelivery so unmatched/loser
observations do not imply a winner. Deadline validation also requires JSON time
representability, retaining the existing nonzero UTC-offset-zero requirement.
D54: historical zero revision is ErrInvalidSnapshot; absent/future identity or
address is ErrThreadNotFound; pruned/deleted known payload is
ErrExecutionCheckpointUnavailable. Preserve invalid/corrupt envelope and lease
errors distinctly; adapter signed-range input rejects before SQL conversion.
D55: constructors reject nil collaborators as configuration/capability errors,
including typed nil. PG constructors may clean-break to error returns; adapt all
call sites without compatibility wrappers. Standalone snapshot codecs produce
JSON, with extra PostgreSQL JSONB string/number limits documented; BYOT describes
the typed domain, not arbitrary storage bytes.
D56: positive fractional lease TTL rounds up to native precision (Redis millisecond,
PG microsecond; memory keeps exact duration). Checkpoint TTL retains its separate
ceil-millisecond policy. Never wrap/reset a retained fence or published revision:
signed64 backend exhaustion rejects without mutation, and native unsigned memory
limits remain explicit. Overflow is a capacity boundary, not an asserted P1.

Acceptance: AAA metadata/codec/no-side-effect tables on memory and Redis, isolated
live PG head/history/outbox admission checks; exact Unicode replay and rejection
before store/notification; historical address/error parity; constructor nil/typed
nil table; fractional TTL and counter-edge tests. Relevant root/adapter race and
lint plus both independent final-state reviews are required before task06 commit.


## Task 06 continuation evidence (partial, unaccepted)

Implemented shared public ValidateSnapshotHeader and used it in EncodeRecord,
DecodeRecord and MemoryCheckpointer. It checks nonempty UTF-8 ID/pointer and
positive stored revision before host codec/cloner. Encode rejects non-JSON state
bytes without invoking Unmarshal. Memory rejects revision wrap. PG SaveWithOutbox
checks the header and signed revision bound before BeginTx/SQL/enqueue.

Metadata before probe exited 1: all five malformed headers reached Marshal and
returned nil. Preserved log `/tmp/flowy-task28-task06-metadata-before.log`.
After implementation, checkpoint/testutil race exited 0 (1.618s/1.588s),
`/tmp/flowy-task28-task06-metadata.log`; memory healthy-history/cloner admission
race exited 0 (1.534s), `/tmp/flowy-task28-task06-memory-admission.log`; Redis
healthy head/history/TTL/key-set admission race exited 0 (1.491s); PG unit
header-before-BeginTx/enqueue race exited 0 (1.657s),
`/tmp/flowy-task28-task06-pg-admission-unit.log`. PG unit is not a live-backend gate.
An accidentally root-scoped PG test command was interrupted (exit130) and was
replaced by the correct separate-module command; it is not counted as PASS.

Reason before probes exited 1: malformed UTF-8 reached child backend twice and
wait/activity/confirmation storage three times. Preserved log
`/tmp/flowy-task28-task06-text-before.log`. Guards now cover wait cancellation,
child cancellation/budget return, manual activity/cancel confirmation and child
wait Result.Error; record/group text checks preserve equality/provenance. Real
U+FFFD and valid Unicode pass the child boundary probe. Wait Deadline now rejects
non-JSON time domain. After these guards, targeted root race exited 0 (1.942s),
`/tmp/flowy-task28-task06-text.log`; root lint exited 0, 0 issues,
`/tmp/flowy-task28-task06-lint-progress.log`; diff check PASS.

Still required before accepting/committing Task06: isolated live PG head/history/
outbox admission, malformed JSON/codec domains and constructor nil/typed-nil clean
break plus all call-site updates; actual Unicode exact replay after reload for
wait/child cancellation and budget return; D36 namespace table and relevant
conformance tests; D44 observation rename and time-domain tests; D54 memory/PG
historical address/error parity; D56 fractional TTL and counter exhaustion tests;
final relevant-module race/lint and two independent Task06 reviews. Current
changes are uncommitted and no Task06 acceptance is claimed. Tasks07–13 remain
pending, as do the final six-module/backend/installability/release gates.

## Task 06 final-state review preparation (not yet accepted)

The nil/typed-nil constructor contract is implemented across core, checkpoint,
Redis and PostgreSQL. PostgreSQL constructors and WithSanitizer now return
errors; all Go call sites are adapted without production compatibility wrappers.
Historical LoadCheckpoint zero/missing/future/pruned/deleted errors are aligned;
PostgreSQL rejects signed-range addresses before SQL. Lease TTL rounds upward
to native precision; memory revision/fence wrap and signed backend exhaustion
reject without mutation. LifecycleWaitDelivery replaces the old winner-named
observation, including the live blueprint metric expectation. Deadline admission
rejects negative and five-digit years.

Added complete Unicode/malformed-byte admission and serialized reload tests for
wait cancellation, child cancellation and budget returns, including literal
U+FFFD. Added JSON codec admission, constructor nil tables, native TTL SQL-argument
probes and retained-counter exhaustion tests. Current contracts are in
`docs/storage-admission.md` and `docs/decision-namespaces.md`, linked from the
runtime contract. Two existing telemetry corruption fixtures now inject invalid
pointers at Load: valid Save must not be weakened to seed corruption.

Parent completed checks (exit0, no skipped live gates):
- Root full fresh race: `/tmp/flowy-task28-task06-root-race-final.log`.
- Four adapter unit race: module1-race-final and module2/3/4-race logs;
  durable_agent unit build: module5-race log (no tests without integration tag).
- Six-module lint: root-lint-final, pg-lint-final and module2/3/4/5-lint logs,
  each 0 issues.
- Actual disposable PostgreSQL17 metadata admission, JSONB rejection rollback,
  historical errors and both signed-fence exhaustion paths: PG boundaries live
  log; actual Redis7 Lua exhaustion and same-owner fencing: redis-live log.
- Exact Unicode replay/JSON time-domain targeted root race: replay-final-progress
  log. Controlled Redis TTL, PG SQL TTL arguments and exact memory TTL are tested
  separately from live wall-clock scheduling.

Preserved non-PASS attempts: first root suite rejected the old invalid-pointer
Save seeds; first PG unit suite had a mistaken sentinel name; lint initially
reported test-matrix complexity, embedded-field spacing and an integration-only
helper in untagged test code. These were fixed; their logs remain. An accidentally
root-scoped PG command is not backend evidence.

Task06 independent reviewers `/root/task06_completeness` and
`/root/task06_correctness` are reviewing the final implementation; no acceptance
or commit is claimed. Parent full PostgreSQL tagged suite and live durable_agent
blueprint are still running. Tasks07–13 remain pending.

Parent final live gates finished (exit0): full tagged PostgreSQL race 43.697s,
`/tmp/flowy-task28-task06-pg-integration-final.log`; actual PostgreSQL durable_agent
blueprint race 3.021s, `/tmp/flowy-task28-task06-blueprint-final.log`.
Independent reviewers have reported targeted root/native-backend PASS while
completing source review; their final verdicts are still pending.

## Task 06 accepted

Completeness reviewer `/root/task06_completeness`: final 100% (7/7), no open
findings; final test-only edits and completed tagged lint gates verified.
Correctness reviewer `/root/task06_correctness`: final PASS, errors not found,
no open findings; after the final edits independently repeated actual PG
boundaries/JSONB race (2.074s), Redis live race (1.923s) and PG blueprint race
(3.480s). Reports: `reviews/task06-completeness.md` and
`reviews/task06-correctness.md`. Earlier reports and failures are historical
evidence; both reviewers accepted the final diff.

Additional tagged lint final gates: PG and Redis `tagged0/1-lint-final.log`,
blueprint `tagged2-lint-accepted.log`, each completion exit0 and 0 issues.
The initial tagged lint findings were only test shadow names, blueprint condition
formatting and scenario-helper complexity; fixed without weakening assertions.
No current runtime, adapter, blueprint or review gate remains open for Task06.
Task06 commit message: `fix: storage admission`; SHA recorded in the following
journal update. Tasks07–13 and the overall final acceptance remain pending.

## Task 07 contract (before implementation)

Task06 commit: `1f08d04` (`fix: storage admission`). Its temporary PG/Redis
containers were removed after all live checks and independent acceptance.

D01: ordinary snapshots and durable aggregate execution remain explicit, separate
persistence profiles. Split ordinary runner implementation into execution, session,
publication/recovery and inline capture files without an implicit upgrade or plugin
framework. File movement must preserve callback, fencing and replay contracts.
D02: remove subgraphTestMode, failingCaptureCheckpointer and bumpRevisionOnLoadCP
from shipped Go source. A private generic composition helper receives an ephemeral
checkpointer factory; only *_test.go owns fault implementations/context selectors.
No global test hook and no externally exposed test mode.
D03: choose the explicitly permitted naming disposition: AsStatelessNode and
StatelessSubgraphNode distinguish invocation-only composition from
SubgraphNodeWithSlot. Remove old ambiguous names, update consumers and docs.
Suspend/Handoff from the stateless variant pause only the parent; its next invocation
starts the inner entry again. This restart is explicit, not an inner continuation
guarantee. Retain useful parent-boundary directives and test this behavior. The
slot variant persists the inner pointer/state/effect cursor; neither creates a
durable child execution. Durable parent admission continues rejecting inline use.
D04: synthetic parent::node identifiers and capture history are invocation-local.
SubgraphSlot.Revision is informational capture progress, never an independent OCC
authority. Parent checkpoint fencing/revision protects the persisted slot. Retain
the field with precise GoDoc; ephemeral seed always starts at revision zero.
D06: expose the wrapper as NewAdvisoryLeaseGuardCheckpointer; remove the old
ambiguous constructor name. Its Holder-before-DeleteIfIdle check remains best-effort,
with no atomic fencing capability inferred. Document cross-store race and the
explicit native adapter alternative.
D11: EndNode is a routing sentinel; registering a node with that name makes Compile
fail. Fluent AddNode continues returning its builder; all validation occurs at
Compile and no caller node is silently shadowed by a terminal edge.
D13: compile errors are sorted deterministically by their complete node/edge
diagnostic text before errors.Join, preserving all diagnostics and errors.Is causes.

Acceptance: production source/package inventory has no test scaffolding; fault
tests use only the private factory seam; stateless restart, slot continuity and
parent effect/outbox boundaries pass; reserved-name and randomized-registration
diagnostics tests; current consumers compile with no legacy aliases. Fresh root
race/lint and relevant example/module checks, then two independent final reviews
are required before the separate Task07 commit.

Task07 implementation in progress: ordinary runner now separates execution,
sessions, recovery, publication and stream handling; invocation capture is in
compose_capture.go. Durable session ownership moved to execution_session.go,
retaining existing durable_start/execution_checkpointer/session-observation
boundaries. Private fault implementations/selectors are exclusively *_test.go.
New explicit stateless/advisory API names replace old names without aliases.
Reserved EndNode admission and stable aggregate diagnostic tests added.

Initial targeted run overlapped the durable session file move and its vet process
observed the intermediate missing type; exit1 is not PASS. Root lint on the
completed move exited0, 0issues. Fresh final-state race/lint and independent
reviews are still required; Task07 is not accepted or committed.

## Task 07 accepted

D01–D04, D06, D11 and D13 are complete under the recorded contract. Current
runner files separate ownership/execution/publication/recovery; all original
ordinary function declarations and durable session declarations were retained,
with only the intended API names changed. The test-only factory wrappers and
fault context keys are absent from production package inventory.

Parent final checks, all completion exit0:
- Full root fresh race: 12.105s, `/tmp/flowy-task28-task07-root-race.log`.
- Root lint: 0 issues, `/tmp/flowy-task28-task07-root-lint.log`.
- Separate durable_agent module build/race command: exit0, no untagged tests;
  `/tmp/flowy-task28-task07-example-race.log` is build evidence only.
- Final diff whitespace check PASS. No backend storage semantics changed; final
  six-module/native-backend acceptance remains Task13.

Completeness reviewer `/root/task07_completeness`: 100% (7/7), gaps0; independent
targeted root race count2 PASS1.890s, production declaration/inventory checks.
Correctness reviewer `/root/task07_correctness`: errors not found, findings0;
independent targeted race count3 PASS2.131s and full root race PASS10.744s.
Both reviewed the final implementation and corroborated completed parent logs.
Reports are `reviews/task07-completeness.md` and `reviews/task07-correctness.md`.
Task07 commit message: `refactor: runner boundaries`; SHA will be recorded in the
following journal update. Task08–13 and overall completion remain pending.

## Task 08 contract (before implementation)

Task07 commit: `268f458` (`refactor: runner boundaries`).

D15: BuildSupervisor becomes BuildDispatchGraph without an alias: the routing node
selects a terminal worker and never supervises a worker loop. Its fixed routing
node is dispatch. BuildReAct keeps its name but names its positive parameter
maxActionRetries: the existing Retry semantics permit at most that many fallback
rounds and at most maxActionRetries+1 action callbacks, not a total graph-step
limit. Keep this explicit existing policy rather than silently changing persisted
retry accounting. BuildEvaluatorOptimizer names maxCorrectionRetries and retains
the same Retry budget; correction rounds are domain evaluation, not transport retry.
D16: all pattern constructors return (builder,error) and reject missing node/
predicate/accessor callbacks and nonpositive retry budgets before execution.
Dispatch snapshots routes/worker definitions at construction, rejects nil workers
and conflicting reserved worker IDs, and retains Compile's topology validation.
ReAct predicate is evaluated exactly once in the router for each Completed reason
node; non-Completed directives preserve their wrappers without evaluating it.
Pure callbacks must be prompt and safe for concurrent graph runs.
D17: retain replacement reducers and fixed node IDs with a full-state-update
contract and explicit IDs in docs. No graph DSL or implicit host merge reducer.
D18: retain exactly one zero-value binding slot per Go type T. Different sentinels
of the same T collide. Bind only during setup, then freeze by discipline before
WithContext/WithBindings/share. Concurrent reads are allowed; host resources own
their concurrency. No reflected resource cloning, DI framework or silent same-type
identity introduced.
D19: add context-scoped LifecycleObserver and TelemetryBridge selection, including
explicit nil disable, while synchronized process-wide setters remain defaults.
Context values provide instance/run isolation for ordinary and durable entry
points; host must reattach its instance scope on a fresh worker/resume context.
OTel observer/bridge constructors permit use without global installation and use
explicit providers for observation. No mandatory telemetry dependency in core.
D20: rename TelemetryBridge methods to Capture/Restore and update all consumers
without old-method adapters. Installation returns errors to the host, never also
logs them. Observer panics remain contained; bridge callbacks are pure synchronous
codec-like hooks and must not panic, block indefinitely or mutate retained inputs.
Bridge panics propagate rather than inventing safe replay after unknown work.
D21: preserve bounded operation/stage/code metric dimensions, default W3C-only
carrier, no raw payload/evidence/error traces and no commit/delivery guarantee
from observation. Add isolation/privacy tests for scoped integrations.

Acceptance: nil config tables, route ownership and single predicate evaluation
under concurrent runs, action/correction retry counts and effect order; binding
same-type collision/frozen read tests; scoped observer/bridge isolation and global
fallback/explicit disable, Capture/Restore carrier detachment, OTel constructor
error and privacy checks. Fresh relevant race/lint plus two independent final
reviews are required before Task08 acceptance or commit.

## Task 08 final-state review preparation (not yet accepted)

Implemented pattern construction error returns/nil callback and positive retry
budget admission, BuildDispatchGraph without a supervisor alias, owned route
configuration and single pending-predicate evaluation in ReAct routing. Existing
Retry semantics are explicit and tested: two fallback rounds allow three action/
evaluation callbacks and six graph nodes. Full-state replacement and fixed IDs
are current GoDoc/runtime contracts; the old dispatch cursor requires explicit
host migration/drain. Updated both executable pattern examples and README usage.
RunBindings retains a slot per type and documented freeze-by-discipline; same-type
collision and eight frozen concurrent readers have regression coverage.

WithLifecycleObserver and WithTelemetryBridge provide immutable context scope;
typed nil explicitly disables rather than using globals. Core bridge methods are
Capture/Restore without old-method adapters. OTel constructors bind explicit
providers without global installation; install failures return the cause without
also logging. Added scoped isolation/detachment, constructor nil, provider error/
no-log and provider isolation/privacy tests. W3C-only bridge and bounded telemetry
dimensions remain unchanged. Current observation doc now names WaitDelivery and
explains instance scope, host callback concurrency, observer panic containment
and nonpanicking bridge policy.

Parent evidence so far:
- Pattern/OTel initial fresh race exit0, 1.420s/1.498s, patterns-otel-progress log.
- Scoped core/bindings targeted race exit0, 1.806s, scopes-progress log.
- Full final root race exit0, 11.176s; ext/otel1.575s/patterns1.511s/testutil1.174s,
  `/tmp/flowy-task28-task08-root-race-final.log`.
- Separate durable_agent module compile/race command exit0, no untagged tests;
  `/tmp/flowy-task28-task08-example-race.log` is build evidence only.
- Final lint exit0, 0 issues; `/tmp/flowy-task28-task08-lint-accepted.log`.
- Diff whitespace check PASS; production BuildSupervisor/old bridge methods absent.

Preserved non-PASS logs: the new config table initially omitted explicit generic
E arguments; lint reported an intentionally nonexecuted callback's always-nil
error and embedded-field spacing in test provider stubs. Corrected fixture types/
poison callback and spacing; no runtime assertion was weakened. Earlier fullroot
PASS13.215s predates the added retry-count/provider-error fixtures and is not used
as the final evidence for those fixtures. No Task08 acceptance or commit claimed.

## Task 08 accepted

Both independent final reviews accepted the unchanged implementation: completeness
100% (D15–D21, 7/7), correctness 0 open findings. Reports are
`reviews/task08-completeness.md` and `reviews/task08-correctness.md`.
Completeness independently passed patterns/OTel race and scoped core/bindings
race count3. Correctness independently passed full root race (12.169s),
patterns/OTel count3, scopes count3 and explicit bindings/durable restoration
count3. Parent final root race and lint are terminal PASS as recorded above.
No six-module/native/final Task28 acceptance is inferred from this task.
Local commit follows; its SHA will be recorded with the next task contract.

## Task 09 contract (before implementation)

Task08 accepted and committed as `f033a9c` (`fix: runtime configuration`).
Scope: D22–D29, eight independently checked criteria.
D22: retain duplicate manual DecisionID rejection; document load/inspect exact
resolution plus fresh revision token after lost ACK. No blind redispatch or new
operator ID to bypass uncertainty, no new exact command replay semantics.
D23: reconciliation is read-only/idempotent/concurrency-safe and prompt; dispatch/
classifier panic remains panic, never an ordinary retry classification. Forbid
nested CallActivity from activity callbacks using a context marker, before journal
mutation. Separate keys within the owning node remain supported sequentially.
D24: document record state/origin versus historical attempt classification,
all-zero retry policy exhaustion, manual resolution provenance and observation.
D25: add validated fixed/exponential schedule constructors with examples and
explicit total-attempt accounting; one CallActivity performs at most one dispatch.
D26: retain nil RNG standard source and invalid class -> ambiguous; emit bounded
invalid-classification diagnostic with no raw classifier value or dispatch error.
Persisted deadline remains authority and unknown does not automatically retry.
D27: current contract provides host bump rules for callbacks/codecs/projections/
merge; no function pointer hashing or inferred compatibility.
D28: failed committed-state reload/decode returns ErrDurableStateUnavailable plus
both original/reload errors; returned local state/effects are explicitly diagnostic
in result reason and GoDoc, without suggesting a restored committed boundary.
D29: measure fresh journal/attempt/child/wait aggregate rewrite growth, document
measurement limits and explicit resolved-boundary rollover/payload references.
No TTL deletion of unknown/receipts or unmeasured storage-engine replacement.
Acceptance requires regression/fault tests for changed boundaries, fresh relevant
race/lint, measured growth output and two final independent reviews (100%/0 open).

## Task 09 review preparation (not yet accepted)

Implemented callback-derived context rejection, bounded invalid-classification
observation and validated fixed/exponential schedule constructors. Added explicit
ErrDurableStateUnavailable diagnostic classification on failed entry restoration
at commit, cancellation and handoff boundaries; original causes and restore error
remain joined. No diagnostic local state claims committed authority. Duplicate
manual decisions remain rejected; a real commit/lost-ACK fixture covers inspect
and fresh-token resume without redispatch. Current retry doc records callback
ownership, taxonomy/observation, total attempts, host compatibility bump rules,
recovery and aggregate amplification limits.

Fresh growth measurements passed under race: waits16/64 retained all deliveries
and activities16/64 retained all attempts. One-iteration activity/child growth
benchmarks passed for16/64/256 records; raw logs are
`/tmp/flowy-task28-task09-growth.log` and `...-bench.log`. Serialized rewrite bytes
exclude native indexes/WAL; no latency threshold or production capacity inferred.
Targeted boundary checks passed after correcting a test that incorrectly required
zero revision on a diagnostic result; its token legitimately addresses stored
head1. Initial FAIL retained in `...-targeted.log`; final targeted exit0 in
`...-targeted-final.log`. First lint found function length/shadowing and test style;
refactored observation classification into a small helper, final lint exit0,
0 issues in `...-lint-final.log`. Full final race and independent reviews pending;
no Task09 acceptance or commit claimed.

## Task 09 accepted

Independent completeness: 100%, D22–D29 8/8, 0 gaps; independently executed
selected regression/growth race count2 exit0 (9.797s). Independent correctness:
0 open findings; full root race exit0 (15.045s), targeted count3 exit0 (1.640s).
Reports: `reviews/task09-completeness.md`, `reviews/task09-correctness.md`.
Parent full final root race exit0 (16.129s), lost-ACK targeted exit0 (1.541s),
final lint including all new fixtures exit0, 0 issues (`...-lint-accepted-final.log`).
The penultimate lint's embedded-field blank-line issue in the lost-ACK fixture
was corrected; no semantic changes followed fullroot PASS. Whitespace check PASS.
Prior failed expectation/lint logs remain recorded, not relabelled PASS.
Local commit follows; SHA recorded at next task contract. Overall Task28 and
native/all-six-module gates remain incomplete, assigned to remaining tasks.

## Task 10 contract (before implementation)

Task09 accepted and committed as `7a0a8d3` (`fix: activity contracts`). Its signing
wait has completed; clean worktree verified before Task10 implementation.
Scope: D30, D31, D32, D33, D34, D35, D37, D38 (eight criteria; D36 covered Task06).
D30: rename pure PlanChildBudgetReturn -> ComputeChildBudgetReturn, no legacy alias;
computation never persists/releases named capacity or monetary reservations.
D31: retain bounded admission per group; cancellation cannot preempt arbitrary
callbacks and group bounds do not imply host-wide resource bounds. Host owns
worker pool/quota/kill and downstream cancellation evidence.
D32: replace ignored-error JSON child-group clone with explicit detached copies
of all slices/maps/bytes and pointed provenance. Preserve nil/empty semantics,
invalid caller data and zero timestamps: copying is not wire admission.
D33: document activation capacity, used claims and returned units separately from
money. Dispatcher masks parent activity/group/lease capabilities but retains
parent run metadata; UseBudget requires serialized host access (no fabricated new
race claim). Host child execution owns its own metadata/counters where concurrent.
D34: retain conservative unknown on every dispatcher error, including typed decode
before effects, panics and canceled admission; never automatic redispatch from a
claimed definitely-not-sent error without a separately specified contract.
D35: document post-commit typed merged decode failure and inspect/PrepareChildren
exact-group cached JoinChildren read/redecode recipe; unhandled decode failure
may be terminal and Resume must not be promised to reenter it. Fault fixture proves no
second merge/dispatch after successful join publication and failed decode.
D37: retain centralized flat wire DTO/FSM validation and explicit transition matrix;
completed empty bytes are a valid host encoding, while failed partial result bytes
remain evidence, not a successful typed result. No added presence-bit wire change.
D38: retain exact original group references/bindings on migration and fail closed
when absent. Late child outcome writes use WithoutCancel plus bounded I/O context
and existing live fencing; no detached retry or ownership upgrade.
Acceptance: deep-copy mutation/nil/invalid-domain tests, postcommit decode fault/
cached-read replay, late-write bounded-context/fencing coverage, current contracts,
fresh race/lint and two final independent reviews (100%/0 open).

## Task 10 review preparation (not yet accepted)

Implemented ComputeChildBudgetReturn clean rename with all Go callers updated;
replaced ignored-error JSON child group roundtrip by complete explicit deep copy;
late child finish now uses detached five-second I/O context and unchanged fencing.
Added mutation coverage for every owned map/slice/byte/provenance pointer, nil/empty
shape and outside-JSON-domain memory values. Strengthened existing noncooperative
late-outcome fixture to inspect bounded detached context at the failed stale write.
Typed postcommit decode fixtures prove inspect/redecode of committed bytes,
immutable terminal replay and handled-in-activation cached join with a fresh exact
group assertion, one dispatch and one merge. Current child/runtime contracts cover
parent metadata/counter ownership, conservative unknown, host-wide resource bounds,
central FSM transition matrix, empty/partial payloads and migration binding rules.

Preserved non-PASS evidence: `...-targeted.log`, `...-targeted-final.log`, and initial
root race logs exposed incorrect test expectations about reentering terminal failure
and inferring live codec error identity on persisted replay. Correct behavior is
terminal replay with PersistedExecutionError/ErrExecutionFailed; no runtime terminal
policy was changed to satisfy the fixture. Fixed the test and recovery recipe.
Initial lint PASS; later benchmark-only lint caught cognitive complexity, split
comparison setup from measurement helper (no new suppression). Final gates pending.
Copy benchmark exit0 in `/tmp/flowy-task28-task10-copy-bench.log`, detached3,257ns/
23alloc versus JSON29,103ns/50alloc for one valid nested fixture. This proves local
copy benefit only; no native-storage or stable performance threshold inferred.
Fresh final targeted race/lint/fullroot and two independent reviews required;
Task10 is not accepted or committed yet.

## Task 10 D30 contract amendment (before sentinel change)

Completeness reviewer identified the omitted first half of D30: ErrChildJoinInvalid
is used beyond join. Adopt clean-break ErrChildInvalid for invalid child/group/
composition contract, message `flowy: invalid child contract`, no legacy alias.
Update all Go consumers and current contracts. Stored historical terminal messages
remain historical text; replay still exposes ErrExecutionFailed, not a reconstructed
sentinel. Native wire formats and successful behavior do not change. Both final
reviews and fresh race/lint must cover this rename; preliminary verdicts are not
Task10 acceptance. Benchmark wording is JSON-representable fixture, not an admitted
execution DTO: copy tests/bench deliberately separate copying from validation.

## Task 10 accepted

Final renamed state accepted by two independent reviewers: completeness100%
(D30–D35/D37/D38, 8/8, 0gaps), correctness0 open findings. Reports:
`reviews/task10-completeness.md`, `reviews/task10-correctness.md`.
Independent final races exit0: completeness root2.998s; correctness root2.866s.
Parent final root19196 exit0, root16.461s/allrootpackagesPASS; final lint89256
exit0,0issues. Logs `...-root-race-renamed.log`, `...-lint-renamed.log`.
Prior preliminary root17.504s/targeted1.690s/lint0 did not substitute finalrename
acceptance. Final whitespace check PASS and old Go identifiers have no matches.
Source stable after rename/format; D30 reviewer gap was corrected then both reviews
repeated. Overall/native/all-six-module gates remain assigned to remaining tasks.
Local commit follows; SHA will be recorded at next contract update.

## Task 11 contract (before implementation)

Task10 accepted and committed as `8e256fe` (`fix: child contracts`). Clean checkout
verified after signing completed. Scope16 decision items plus documentation1/3.
D39: retain first committed valid winner, including events received after deadline
before timer publication. Example rejects business-late events in Match using a
host clock; core must not silently change deadline precedence.
D40: unique unmatched/lost/canceled delivery IDs are immutable outcomes. Replays
require compatibility labels but no Match/Apply; new evidence needs a new ID.
D41: retain delivery ledger/no TTL erasure. Host transport admission/rate limits
bound unique input; no silent cap/fake ACK. Explicit rollover only at resolved gate.
D42: replace ignored JSON wait clone with value/maps/pointer copy, preserve memory
shape independently of validation; reuse parsed wait collections inside each read
phase while preserving separate pre-lease and post-lease authoritative reads.
D43: profile names deployment/recovery ownership, not a live scheduler. Preserve
same-store atomic authority; wrapped transactional implementation remains allowed.
D45: neutral ExecutionProgress replaces MigrationState without alias; historical
checkpoint errors already neutralized Task06. Fork-only errors remain fork-only.
D46: PrepareExecutionMigration explicitly requires valid addressed sealed source,
checks integrity before any transform, caller owns collection/source admission.
Remove redundant MigrationProvenance.Digest; SourceRevision/SourceDigest/Chain and
whole-envelope seal remain. Old persisted migration envelopes require drain/archive
or explicit offline conversion; no silent legacy decode/forged new source seal.
D47: retain one outgoing edge per descriptor and whole-registry linear validation,
no branch search/automatic version guessing. Add/update tests and explicit contract.
D48: retain embedded import artifact intentionally for self-contained validated
provenance and missing-artifact safety. Document repeated byte cost, limits and why
external host reference needs a future explicit reader/missing-reference contract;
no unverified pointer conversion or invented external-effect evidence.
D49: retain explicit fake default; inspectable fork may be nonresumable without
host policy. Live projections sanitize permissions/references, never copy approval.
D50: retain separate fork/rollover dependency gates; reset vs preserved accounting
is deliberate and validators are not assumed interchangeable.
D51: immutable rollover receipt returns original target creation revision, not latest;
current target load remains explicit. Preserve native anchors/fencing with tests.
D52: KeepLast0 retains live head without DeletePayload; retries count new deletions
only, ACK loss does not exactly-once metrics; MaxRecords is checked-cycle meaning.
D53: preserve permanent identities/fences/anchors against ABA. List metadata cost;
seal is integrity, not authorization against a host DBA controlling all records.
D57: rename DiscoveryRebuildPage.Rebuilt -> Processed (includes quarantined/deleted
heads), return confirmed partial cursor/count/diagnostics on later failure. More=true
on error means unfinished pass; failed current head may have unknown commit and can
be safely reprocessed under head lock. Native fault/partial retry coverage required.
D58: indexed discovery is query, not scheduler; Redis standalone snapshot/lease is
not ExecutionStore; memory helpers are nondurable. Native locks/no full-scan fallback
remain. Documentation1 clarifies fork payload availability versus independent
creation anchor; documentation3 makes lifecycle/indexed discovery current, with
historical origin distinguished. Fresh relevant root/PG race/lint and two independent
final reviews (18/18 coverage,100%/0open) precede acceptance/commit.

Task11 naming detail before edits: remaining shared ErrForkInvalid and
ErrForkSourceDigest become ErrExecutionLifecycleInvalid and ErrExecutionSourceDigest.
Fork-only policy/transform/target/unresolved errors retain their specific names.
ExecutionProgress keeps the same JSON progress fields; only removal of redundant
MigrationProvenance.Digest changes old migrated-envelope serialization.


### Task11 implementation evidence (acceptance pending)

Parent full root race15408 terminal0: root14.506s, all root packages completed.
PG tagged full race24854 terminal0:56.278s on own disposable PostgreSQL17
flowy-task28-pg11 port58029; no native test skip used as evidence.
Root lint10281 terminal0,0issues. PG tagged lint initial32532exit3 due to
concurrent tool lock (not code PASS); repeated95028terminal0,0issues.
A registry topology test was added after initial root command; fresh targeted
root59331/final lint27575 are pending and independent final reviews cover it.
Two reviewers are running; one found stale head-scan wording in current wait
contract, now corrected to indexed projections/head revalidation. No acceptance
or Task11 commit is claimed from pending commands/reviews.


## Task11 accepted

Both independent final-state reviews accepted: completeness100% (18/18),
correctness0 open. Reports reviews/task11-completeness.md and
reviews/task11-correctness.md include final storage-adapter Processed wording.
Independent completeness root27276terminal0,2.229s and nativepartial14180exit0,
2.435s; incorrect early PG selection excluded explicitly. Correctness root50914
exit0,1.977s, extra boundary74475exit0,1.638s; native lifecycle41475exit0,16.740s,
wait ownership95500exit0,5.400s,no skipped fixtures. Parent final-added root59331
exit0,1.654s and final lint27575exit0,0issues cover final registrytest; prior full
root14.506s and nativePG56.278s passed, taggedPGlint0. WhitespacecheckPASS.
The final doc-only correction was reviewed by BOTH agents after initial reports;
no runtime code changed after these gates. No aliases for removed Go names.
Own disposable PostgreSQL17 pg11 is retained for Task13 final gates; existing
user task22 containers were not touched. Overall tasks12/13 remain unfinished.
Local signed commit follows; SHA recorded with next task contract.


## Task12 contract (before implementation)

Task11 committed22517a3 fix: lifecycle contracts, clean checkout verified.
Scope7 criteria: D60/D61 and mandatory documentation2/4/5/6/7.
D60: add bounded meaningful fuzz properties for seal/address/tamper and snapshot
JSON/admission. Replace empty fuzz shell success with explicit module/package/probe
discovery, one exact fuzz name per invocation, propagate discovery/run errors,
fail if no probes. Default30s/probe, bounded worker count; report actual command
and duration, never infer all codecs/native/concurrency coverage from fuzz.
D61: current README/GoDoc/contracts/examples match HEAD, historical task22–27
remain historical. README begins installation/module paths/complete executable
BYOT hello world before API migration history; keep detailed migration guide.
Doc2: DurableRunner describes Start/Resume and Stream/ResumeStream with initial
commit-before-handle/node timing and WaitResult authority.
Doc4: cookbook indexes handoff/outbox and separate durable_agent blueprint, with
explicit fake ports, ordinary/memory smoke and actual PostgreSQL recovery gates.
Doc5: executable hello world uses host state/effect types and checked errors,
Go1.27.1 minimum/current unsuffixedv0/v1 module paths, no advertised unverifiedv2.
Doc6: docs/runtime-contract.md becomes canonical ownership/concurrency/callback,
panic and typed error/recovery index, including snapshot JSON wire, unknown ACK,
partial cleanup and unique decision semantics. Fix stale log-only cleanup and
pattern/observer/inline naming, old consumer guidance statuses.
Doc7: describe exactly six modules, native integration env/tags/DB privileges,
benchmark manifests and meaningful fuzz limits; root tests alone are insufficient.
Fresh root lint/race, hello executable, tool rejection fixtures and bounded real
fuzz runs precede two independent final reviews100%(7/7)/0open and signed commit.
Task13 remains full native/all-module/installability/release/growth gates.


### Task12 verification (acceptance pending)

Hello command93621terminal0 prints exactly two Hello, Sergey lines; Python
run_fuzz rejection/discovery fixtures4/4PASS. Full root41777terminal0 root14.466s,
checkpoint2.132s/allpackagespassed. Initial lint6096exit1(avgcyclop) led to split
fuzz property helpers, final91653terminal0,0issues. Addedhello to existing smoke
matrix16examples; finalroot14648/lint11100pending after that test-only change.
Timed fuzz65742terminal0:3s/probe,2workers, envelope1563execs and checkpoint48477,
no failure, bothPASS. Go emitted stat-cache permission warnings while discovering
replaced nested modules; commands still completed and both actual fuzzprobes ran.
This is bounded property evidence, not fullnative/allmodule/hostcodec coverage.
Both independent final reviews pending. Completeness requested removal of stale
'synchronous durable profile' restriction in canonical contract; corrected.


## Task12 accepted

Independent final reports reviews/task12-completeness.md (100%,7/7,0gaps),
reviews/task12-correctness.md (0open). Completeness own hello/4fixtures/actual
6module discovery and race seeds+16smoke90030terminal0; correctness own seeds
66601terminal0 root1.616s/checkpoint1.447s,hello/4fixtures/6moduleinventory,
zero-durationrejectexit2. Both reviewed final durable-profile wording.
Parent fullroot41777exit0root14.466s; finalseed/smoke14648exit0; final lint11100
exit0,0issues after helper split and smoke addition. Timed actualfuzz65742exit0
bothprobePASS; short duration proves executedproperties only. Whitespacecheck0.
Task13 remains all-six fresh race/lint/native/blueprint/benchmark/releasefixtures
and clean consumer installability; no overall/release acceptance asserted yet.
Local signedcommit follows; SHA recorded with Task13 contract.


## Task13 contract (before final verification)

Task12 committed e3dfa84 docs: runtime contracts; clean checkout verified.
Final scope: F01–F10/D01–D62/doc1–8 coverage audit plus complete DoD. No requirement
is waived through prior partial/seed/local-only results. Fresh race+lint all SIX
modules; tagged race/lint all four native adapters+actual PostgreSQL blueprint on
own disposable infrastructure. Keep user-owned task22 containers untouched.
Run fixed root15/PG14 benchmark manifests and rejection fixtures; never loosen
ceilings to obtain PASS. Native benchmark DB is separate from native test DB.
Run all isolated release fixtures, meaningful fuzz runner fixtures/probes and
cookbook/memory smoke. Verify all six intended module artifacts from one HEAD
via a disposable local Go module proxy, no localreplace/GOWORKoff/freshGOMODCACHE,
consumerbuild importing core+4adapters and separate goinstall blueprint. Add a
repeatable verification script; it builds artifacts/consumer only, no remoteref
mutation, repositorytagging or release. Dependency version edits stay in /tmp.
Record actual commands, terminal results, skipped/failure limits and benchmark
metrics in finaljournal/report. Two independent final reviewers must audit all80
criteria plus gates, completeness100% and correctness0open; any gap fixed then
BOTH rerun/review finalstate. Final short signed localcommit required. Stop/remove
only owned temporarycontainers after native/reviewchecks. No push/PR/release.


### Task13 final gates (review pending)

All six fresh uncached race sessions terminal0:11146root,24413PGcheckpoint,
34516Redischeckpoint,17431PGlease,3184Redislease,62164blueprint. Root/adapter
ordinary proof excludes taggednative tests; separate native63373terminal0 all
five modules, verbose logs native0..4,0skippedfixtures. NativePGcheckpoint65.195s;
PG17port58029 isolatedfinalDB, Redis7port55255 ownedtemporaryredis13. Taggedlint
49498terminal0 allfive0issues; makealllint4147terminal0 six0issues.
Python1753terminal0,25fixtures14.861s: release16+benchmark5+fuzz4.
Rootbenchmark64851terminal0/checker15cases; PG55855terminal0/checker14cases,
fixedmanifestsunchanged. Actual benchlogs final-root-bench/final-pg-bench retained.
Consumer firstattempt29911exit1 due persisted GOPRIVATE bypassing localproxy;
second55092exit1 libraryconsumerbuilt but cold blueprint package-query selected
parent module. Script now GOENVoff/nonmatching privatepatterns and explicitly
downloads/verifies each module zip before build/install; fresh thirdattemptpending.
Failed attempts are diagnostic, neverinstallabilityPASS. No source/refs/release
mutation fromconsumercheck; its sources are committede3dfa84. Runtime Go/dependency
sources have no Task13 delta; current changes are verification script/docs only.


Final consumer79866terminal0: frozen e3dfa84 source, six verified local-proxy zip
artifacts, freshbuild/module caches, GOWORK/GOENVoff/no localreplace, library
consumerbuild plus blueprintinstall. Result retained at
/var/folders/46/5ywmz5gj26n7mnky51gd60g00000gn/T/flowy-task28-consumer-b6to6e51/result.json.
Third50931exit0 alsopassed before the harness froze SHA once; neitherfailed first
attempt is accepted. Final runtime/application and dependency sources unchanged
since the verified commit. Task13 final ledger task28-final.md maps all80 unique
criteria without duplicates/omissions and records gates/limits. Final reports,
owned infrastructure cleanup and signedcommit still pending.


Task13 final commit journal detail: after the last signedcommit completes, write
all13 task/SHA/message/signature records to local ignored tmp/task28/commit-receipt.json.
This is a local audit receipt, excluded from publication like other tmp artifacts;
the final commit cannot contain its own hash without changing that hash. Prior12
SHAs remain recorded in sequential contracts. Final tool/docs code unchanged;
owned cleanup and finalcommit follow both reports.


## Task13 accepted

Final independent reports: reviews/task13-completeness.md100%(80/80,0gaps),
reviews/task13-correctness.md0open. Both rechecked final frozenSHA script and
final ledger/ignoredreceipt documentation. Independent completeness root2358
exit0,2.390s, boundary69641exit0,1.543s andPython43595exit0,25tests13.270s.
Correctness root95941exit0,count2/root2.594s,Python23935exit0,25tests12.711s,
nativeRediscount2exit0,1.509s/no skips and independent sixartifactfreeze probe.
Both validated fixedbenchmark outputs and actualfinalconsumer result.
All80criteria/gates documented task28-final.md. No runtime/dependencychanges
since e3dfa84; final consumers verify that exact application source. Final
whitespacecheck0; Python verification script compiles. Signed prior12commitsG.
Own PostgreSQL pg11 andRedisredis13 dockerstopterminal0; --rm containers removed,
verified absent. Existinguser flowy-task22-postgres/redis retained and untouched.
Final short signedlocalcommit follows; its exactSHA/all13signatures are recorded
in ignored localreceipt after completion. No push/release/PR performed.
