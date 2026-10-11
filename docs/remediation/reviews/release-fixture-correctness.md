# Independent correctness review: sibling outcome release fixture

Reviewed the final child_outcome_siblings_test.go diff against HEAD95a9119 and
its before-fix contract in docs/remediation/release-followup.md. No implementation
or contract was edited by this reviewer.

Verdict: **accepted; 0 open correctness findings** in this fixture correction.

The dispatcher barrier requires all three callbacks to enter before a can return
its completed outcome. Atomic Add returns3 exactly once, so channel closure is
unique. Its wait does not hold the runtime/store mutex. Children b/c separately
wait for a's successful durable commit; dispatcher return alone cannot establish
that state. Both waits select cancellation, and the test context has a10s deadline.
No sleep, timing retry, lowered assertion or optimistic unknown conversion exists.

The store delegates first and signals only on successful CommitExecution. Its
JSON observation reads an invocation-owned serialized payload, not a concurrently
mutated child collection. sync.Once protects both persistent signals. It never
waits inside the commit callback: the runtime can therefore finish the mutex-held
publication and update its envelope without a lock/channel cycle.

RunChildren may still return after the first unknown and cancel its worker context.
The node then waits outside that runtime mutex, using its own active context, for
all three outcomes. This holds the parent session/lease while late bounded/fenced
finish completes. Cancellation of another child can produce unknown, consistent
with this fixture. The final readiness check explicitly requires calls3 and
completed/unknown/unknown, so missing settlement cannot masquerade as acceptance.
The fixture runner's minute lease and context-aware memory store remain unchanged.

Original assertions remain: addressed b resolution, duplicate decision ID on c
rejected, separate c decision accepted, no redispatch, unchanged completed sibling
and other sibling, unchanged plan/allocation specification, no invented budget
returns, three merged identities, and refusal to alter joined finality. Initial
barriers being already closed on Resume is harmless: no launchable children remain
and the resolved group is joined normally. No production runtime semantics,
fencing or callback contract were changed.

Independent executed checks, original handles confirmed terminal exit0:

- `GOCACHE=/tmp/flowy-task28-go-cache go test -race -count=100 -timeout=5m
  -run '^TestChildOutcomePreservesSiblingsAndDecisionNamespace$' .`
  Session55867, root6.888s; `/tmp/flowy-release-fixture-correctness-race.log`.
- The corresponding nonrace count100 command: session90688, root0.751s;
  `/tmp/flowy-release-fixture-correctness-repeat.log`.
- `git diff --check`: exit0.

Limits: these are100 repeats each of the targeted fixture, not proof of every
interleaving or a complete release gate. Parent-owned full make/lint checks,
short signed fix commit, artifact consumer verification and any authorized
publication must complete independently. The earlier release test failure remains
failure evidence; no publication result is inferred from this acceptance.
