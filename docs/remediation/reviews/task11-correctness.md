# Task11 independent correctness review

Base: `8e256fe`. Reviewed tracked and new files against the Task11 contract:
D39–D43, D45–D53, D57–D58 and documentation requirements1/3. This reviewer
performed no implementation changes and did not rely on the other reviewer's
verdict. Final review includes the added disconnected-registry/cycle fixture and
the corrected current wait documentation describing indexed keyset discovery.

Verdict: **accepted; 0 open findings**.

- Wait arbitration still chooses the first valid committed winner. A late event
  can win before timer publication; an unmatched ID cannot later become accepted.
  Cached decisions retain compatibility checks and bypass Match/Apply. There is
  no ledger erasure, silent cap or invented acknowledgement. The host cutoff
  example states its evaluation-time limitation and requires a named matcher.
- Explicit wait copying detaches the decision map and cancellation pointer,
  retaining nil/empty shape and times outside JSON's domain. All remaining wait
  fields are value data. Both delivery and cancellation retain separate reads
  before and after lease acquisition. Each read validates/parses waits once;
  only its own parsed map is reused. Integrity, collection, source metadata,
  deployment profile, descriptor and graph-pointer admission remain enforced.
- ExecutionProgress and the shared lifecycle/source errors have no old Go alias
  or caller. The serialized progress shape is unchanged. Migration checks source
  integrity before any callback; runtime callers separately establish collection
  and source-metadata validity. Raw helper prerequisites are explicit. Removal of
  MigrationProvenance.Digest changes migrated-envelope canonical serialization:
  old seals cannot silently pass integrity under the new shape. Migration retains
  source revision/digest and chain, with the committed whole-envelope seal.
- Registry admission checks all entries for duplicate identities/outgoing edges
  before callbacks. Traversal detects missing hops and reachable cycles without
  selecting an arbitrary branch. Detached state/effects and atomic publication
  remain covered by the migration regressions.
- Embedded import artifact retention, fake fork default/current live policy,
  separate fork/rollover dependency gates and permanent anchor/fence metadata
  match implementation. The byte-growth and DBA limitations are explicit, not
  performance or authorization guarantees. A retained target's anchor survives
  source/creation payload pruning; exact source inspection still needs payload.
- Rollover receipts remain immutable creation tokens. Retention keeps a live
  head unless explicitly deleting its payload; repeats count new deletions and
  preserve ABA protection. Existing native transaction/ACK-loss and fence tests
  remain applicable rather than being replaced with memory-only evidence.
- PostgreSQL rebuild advances Processed/cursor/diagnostics only after a confirmed
  head transaction. Later errors return that progress with More=true. The failed
  current head may have committed; retry reprocesses it safely under the head
  lock. The native fixture exercises both second-BeginTx failure and lost commit
  ACK. Initial query failure has no confirmed head progress. Current discovery
  documentation correctly distinguishes indexed queries, explicit repair and
  host polling/scheduling; Redis and memory capabilities are not overstated.

Independent terminal checks, all exit0:

1. Root race selection: `go test -race -count=1 -timeout=5m -run
   'Test(ExecutionMigration|Migration|EffectsMigration|PrepareExecutionMigration|Wait|Durable.*Wait|Fork|Execution.*(Fork|Rollover|Retention)|Lifecycle|CloneDurableWait)' .`
   Session50914, root1.977s; log `/tmp/flowy-task28-task11-correctness-root.log`.
2. Additional root boundary/registry race selection: `go test -v -race -count=1
   -timeout=5m -run 'Test(Retention|LateRollover|OrdinaryCommitCannotReplaceRollover|StateAndEffectsMigration|MigrationRegistry)' .`
   Session74475, root1.638s; log `/tmp/flowy-task28-task11-correctness-boundaries.log`.
3. Real PostgreSQL race selection in its adapter module: `go test -race -tags
   integration -count=1 -timeout=5m -run
   'Test.*(Rebuild|Lineage|Rollover|Retention|Migration)' .`
   Session41475,16.740s; log `/tmp/flowy-task28-task11-correctness-pg.log`.
4. Additional real PostgreSQL wait race selection: `go test -v -race -tags
   integration -count=1 -timeout=5m -run
   'TestWait(EarlyDeliveryPersistent|StaleMatcher|ConcurrentDuplicate|CancellationPersistent)' .`
   Session95500,5.400s; log `/tmp/flowy-task28-task11-correctness-pg-waits.log`.
   Five native tests passed, including live stale-owner writes and early delivery;
   the verbose log contains no skipped fixture.
5. `git diff --check` exited0. Removed Go-name search returned no matches.

Commands used GOCACHE=/tmp/flowy-task28-go-cache. Native checks used the parent's
existing disposable PostgreSQL database at127.0.0.1:58029; the reviewer performed
no container, signing, configuration or user-data mutations.

Parent reports separately: full root race14.506s and full tagged PostgreSQL
race56.278s passed, with root/tagged PostgreSQL lint0 issues; the subsequent added
registry root check passed1.654s and final lint0 issues. These are parent evidence,
not this reviewer's independent full-suite claims.

Limits: focused independent selections establish Task11 regressions and actual
PostgreSQL behavior, not the final six-module/Redis/blueprint/release gate. No
stable benchmark threshold, scheduler liveness, business authorization or actual
offline conversion of legacy production migration records is claimed. Tasks12/13
and overall Task28 completion remain pending.

Final documentation-only followup: independently reviewed the final concrete API
paragraph in docs/storage-adapter-contract.md. Processed includes quarantined and
deleted heads; confirmed partial cursor/More=true and unknown-ACK retry under the
head lock match execution_discovery.go and the already-passed native fault cases.
The removed Rebuilt spelling no longer appears in that current document. No code
changed and no additional test run was necessary. Final verdict remains accepted,
0 open findings.
