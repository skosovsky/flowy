# Task 23 — independent completeness review

## Prerelease verdict: 90%

Nine of ten acceptance criteria are fully evidenced. AC9 remains partial until the
published semantic consumer gate succeeds against the new release. The portable
CI checkout/source-ref gate also remains running at this review checkpoint.
Partial criteria and running gates are not counted. Release/publication and issue
closeout are separate required deliverables and are not yet completed.

Scope: current working tree, `.cursor/docs/task23.md`, implementation/tests,
contract/migration/README, CI and actual `/tmp/flowy-task23-*.txt` logs. Initial
findings were addressed without narrowing the task: timeout, recoverable consumed
gap, wait generation/ACK, core-issued recovery token, API migration documentation,
strict complete receipt evidence and idempotent interrupted retry protocol.

| AC | Status | Authoritative evidence |
| --- | --- | --- |
| AC1 | Complete | Approval contract, runtime/wait contracts and migration guide agree. Host BYOT ports and separate optional module preserve boundaries; generic InspectExecutionResume is read-only and imports no concrete backend. Unsupported envelopes/source APIs fail closed. |
| AC2 | Complete | Identity/admission and envelope/mapping tests; distinct IDs for identical intentions, stable replay IDs, strict terminal receipt field completeness and contradictory evidence rejection. Process tests verify preserved CallID/OperationID/result and durable binding. |
| AC3 | Complete | allow/deny/edit/expired/revoked cycle; authenticated sender/address/input checks; duplicate/stale delivery; actual backend binding/expiry/revocation and final write serialization. Edit keeps intention and requires a fresh challenge/grant. |
| AC4 | Complete | Lost response, failed journal publication and state checkpoint faults in memory plus abrupt subprocess cases. Persisted terminal outcome is recovered by fresh processes; counters show one external dispatch/effect and replay has no additional write. |
| AC5 | Complete | Missing/running/unknown/incomplete/corrupt/conflicting evidence; canceled/deadline-exceeded probes; concurrent read-only reconciliation with unchanged operation snapshot, dispatch/effect/capture counters; explicit terminal business rejection. Strict raw/typed/envelope evidence agreement regression tests pass. |
| AC6 | Complete | Every original crash boundary has deterministic hook; process exit must be code 86 with CRASH and no ACK. after_arm compares persisted generation across restart. Consumed gap fails closed without receipt; TestConsumedGapWithVerifiedAbsence separately proves fenced absence permits addressed recovery and blocks a late old attempt. Retry-stage faults prove idempotent operator completion. |
| AC7 | Complete | Named runtime bounded policy with no automatic unknown retry, no nested backend loops. Required pre-capture failure has zero effect; post-effect capture suspends at its own persisted node, preserves authoritative result/identity and replays only capture. |
| AC8 | Complete | TestProcessCrashRecovery builds race-enabled executable, abruptly kills it at 18 boundaries and starts new OS processes using PostgreSQL execution/host evidence plus persistent backend file journal. Mandatory missing DB configuration fails. Process2 and checkout-local completed successfully. No claim of physical disk fault proof. |
| AC9 | Partial | make test2, make-race and make-lint4 passed all seven discovered modules; unit4 and mandatory process2 integration passed. checkout-local semantic suite passed. CI source-ref gate has not yet completed and published semantic gate requires postrelease verification. CI uses PostgreSQL, semantic fixtures, GOWORK=off and removes replacements for published mode. |
| AC10 | Complete | README and migration guide give preparation/checkpoint/auth/authorization/Inspect/retry/capture recipe and before/after InspectExecutionResume snippet. Host responsibilities and unsupported unfenced-tool recovery are explicit. |

## Completed verification logs

- `/tmp/flowy-task23-make-test2.txt`: seven-module make test PASS.
- `/tmp/flowy-task23-make-race.txt`: seven-module make test-race PASS.
- `/tmp/flowy-task23-make-lint4.txt`: seven-module make lint, zero issues.
- `/tmp/flowy-task23-unit4.txt`: approval semantic unit suite PASS (3.827s).
- `/tmp/flowy-task23-process2.txt`: mandatory race/count=1 process suite PASS (171.010s).
- `/tmp/flowy-task23-checkout-local.txt`: checkout semantic/process suite against
  explicit backend source commit `82e51316c272de6241cae429d70a33ecbfad5220`, PASS (169.016s).

Older failing logs are superseded only for the corresponding corrected checks;
they were never counted as successful acceptance. The default backend branch's
older incompatible constructor API fails compilation, honestly. CI fetches the
audited source commit portably; this is a source-ref check, not proof that the
default backend branch currently supports the consumer.

## Original crash matrix audit

| Required boundary | Evidence and observed recovery | Verdict |
| --- | --- | --- |
| Before intention checkpoint | before_intent; no preparation/claim/write; fresh start persists intention | PASS |
| After checkpoint; before/after challenge; before arm | before_prepare, after_challenge, before_arm; pure preparation replay retains identity and reaches approval | PASS |
| After arm commit; before registration ACK | after_arm process fault; no false ACK; same armed generation after restart | PASS |
| Before/after decision commit; before Resume | before_decision, after_grant, after_decision; saved signed event redelivers identically; core InspectExecutionResume provides validated address | PASS |
| Before/after grant consume; before dispatch | before_claim, after_claim, before_effect remain unknown without proof; fenced verified absence + addressed runtime resolution recovers consumed gap without old-attempt write | PASS |
| After dispatch; before terminal evidence | after_effect/before_finish unknown until independent receipt is verified by authenticated operator; one effect | PASS |
| After terminal evidence; before response/activity journal | after_finish/lost_delivery/journal_commit recover original terminal record through read-only inspection | PASS |
| After activity commit; before state/capture checkpoint | state_commit/after_capture replay completed outcome; separate idempotent capture recovery | PASS |

## Remaining acceptance

1. Complete portable checkout CI command and inspect terminal success.
2. Execute selected release target, verify actual publication, then run the same
   semantic/process consumer suite in published mode with GOWORK=off and no local
   replacements. A local proxy/build-only check cannot substitute for this gate.
3. Update this review from those actual logs; only then AC9 can count and the
   acceptance percentage can become 100%.
4. Verify release result and explicit author migration comment plus issue closure
   separately before declaring the full goal completed.
