# Task 06 — independent completeness acceptance

Reviewer: `/root/task06_completeness`. Reviewed the complete tracked diff and
new source/tests/docs against HEAD `714f5ed`, the Task28 source requirements and
Task06 contract. No implementation changes made by this reviewer.

Verdict: **100% (7/7 scope items), accepted for Task06 completeness**. No open
completeness findings. This does not accept Task07–13 or complete Task28.

| Scope | Result and evidence |
|---|---|
| F09 | Shared pure ValidateSnapshotHeader used by encode/decode and memory Save; PG preflights before BeginTx and enqueue, Redis before Lua/TTL. Save derives revision from OCC argument. Metadata matrices preserve healthy head/history, memory cloner count, Redis key set/TTL and live PG outbox. No host decoder invoked for header validation. |
| F10 | Invalid byte and truncated multibyte Reason rejected before storage/notification for wait cancellation, child cancellation and budget return. Valid Unicode and literal U+FFFD survive serialization and exact replay under a separately created runner; budget replay preserves digest and returned units. Other manual Reason/Error admission and stored record validators audited; runtime audit-only error text distinguished in runtime-contract. |
| D36 | decision-namespaces.md specifies five operation namespaces, complete/current assertions, duplicate rejection versus read-only replay and lost-ACK recovery. Existing current-token duplicate wait/confirmation and sibling outcome namespace fixtures agree with implementation; new cancellation/budget replay fixtures prove their distinct behavior. No global UUID ledger introduced. |
| D44 | CancelWait whole-execution failure documented. LifecycleWaitDelivery / wait_delivery replaces winner naming in all Go consumers, including OTEL; no old Go symbol/literal remains. Deadline.MarshalJSON guard rejects years -1 and 10000 through the explicit contract matrix. |
| D54 | Zero/absent/future/known missing payload errors aligned in memory and live PG, including latest head after payload loss. PG unsigned address beyond MaxInt64 rejects before SQL conversion. Interface GoDoc and storage admission table retain corrupt-envelope and lease distinctions. |
| D55 | Shared typed-nil detection applied to mandatory core/codecs/PG/Redis collaborators; plain and typed nil constructor matrices added. PG constructors and WithSanitizer clean-break with errors; owned callsites updated without production compatibility wrappers. JSON wire contract tested without calling host decoder; live PG JSONB NUL/numeric-domain rejection preserves head/history and suppresses enqueue. Extra JSONB surrogate limitation documented. |
| D56 | Redis millisecond and PG microsecond ceiling for both acquire and renew tested via native TTL and observable SQL arguments; memory nanosecond precision retained. Signed PG/Redis fence exhaustion returns capability error with retained counters/no installed lease; memory unsigned wrap guard and revision guard tested. Backend signed versus memory/Redis-snapshot unsigned domains documented separately. |

Independent executions (all returned exit 0):

- Root Unicode replay/admission targeted race: root 1.580s,
  `/tmp/flowy-task28-task06-completeness-root.log`.
- Root contract/constructor/metadata/decision targeted race: root 2.310s,
  checkpoint 1.213s, testutil 1.175s,
  `/tmp/flowy-task28-task06-completeness-contracts.log`.
- Real PostgreSQL admission/historical-address/JSONB-domain targeted race from
  its separate module with explicit disposable DSN: 1.758s,
  `/tmp/flowy-task28-task06-completeness-pg.log`.
- Real Redis lease exhaustion/fencing plus relevant targeted race from its
  separate module with explicit disposable address: 1.332s,
  `/tmp/flowy-task28-task06-completeness-redis.log`.
- `git diff --check`: PASS. Source scan confirms old wait observation symbol is
  absent from Go source.

Parent final logs inspected separately: root full race 10.067s and lint 0 issues;
PG unit race 1.717s and lint 0 issues; other adapter race/lint PASS; durable_agent
untagged build has no test files, therefore is only a build check. Full real PG
tagged race 43.697s and actual PG blueprint race 3.021s are separate successful
gates. Earlier module0/module1 failures are retained history and were not counted
as successful checks; final logs are root-race-final, root-lint-final,
module1-race-final, pg-lint-final, pg-integration-final and blueprint-final.

Limits: percent denotes completion of the seven enumerated Task06 requirements,
not proof of absence of every possible runtime bug. Independent targeted PG/Redis
runs used disposable local backends, not production services. No release, push,
performance baseline, cross-platform execution or Task13 acceptance is claimed.

Final-state addendum: reviewed the later test-only PG/Redis shadow-variable
renames, blueprint condition formatting and narrowly explained cognitive
complexity exclusions. They change no assertions or runtime behavior. All three
additional tagged lint gates completed with exit 0 and 0 issues:
`/tmp/flowy-task28-task06-tagged0-lint-final.log`,
`/tmp/flowy-task28-task06-tagged1-lint-final.log`, and
`/tmp/flowy-task28-task06-tagged2-lint-accepted.log`. Final diff check still PASS.
Completeness remains **100%**, with no open findings, on this final test state.
