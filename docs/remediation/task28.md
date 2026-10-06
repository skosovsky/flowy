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
- Tasks 01–03 accepted below; tasks 04–13 pending.

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
