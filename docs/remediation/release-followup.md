# Release follow-up: deterministic sibling outcome fixture

Source HEAD95a9119. User explicitly requested make release after completed Task28.
Selected make release-break because the accepted implementation removes public
APIs; no release ref was published. First make14613 exited2 during root test:
TestChildOutcomePreservesSiblingsAndDecisionNamespace observed only2 dispatcher
calls, a completed, b stillrunning, c unknown. Allsixlint0issues before failure.

## Contract before fix

RunChildren may return after the first unknown child and cancel its owned work
context; it does not promise every sibling callback entered or settled. Late
finish remains bounded/fenced. The fixture's declared starting state for manual
resolution must actually have onecompleted+twounknown children and all3callbacks
entered. Do not weaken runtime fencing, assertions, unknown semantics or add
sleeps/retries to hide the failure.

Use a dispatcher-entry barrier before a cancomplete; b/c wait for a's real durable
commit. After RunChildren returns, keep the parent node/lease alive until the
store observes all3 committedoutcomes; no wait inside CommitExecution while it
holds the runtime mutex. Bound fixture waits by testcontext, then assert exact
preparedstates/callbackcount before manual resolution. Preserve duplicate-ID,
sibling equality, allocation/plan and joined-finality checks. Repeat nonrace/race
and complete make gates; two independent reviews precede a short signed local
fixcommit. Only then retry the authorized breaking release, verify consumer
artifacts and atomically publish its exact allowlist. Original task28 evidence
remains historical; this follow-up records the newly observed failing path.

## Acceptance evidence

Original fixture failure reproduced with count=200 (exit 1;
/tmp/flowy-release-sibling-before.log). Fixed fixture passed count=1000
(3.586s) and race count=100 (6.443s). The final helper extraction passed
make lint (all six modules, zero issues), make test (all six modules),
and fresh root go test -race -count=1 ./... (exit 0).
Independent completeness review: 100% (7/7), zero gaps; independent
correctness review: zero open errors. Each reviewer ran both ordinary
and race repeats of count=100 on the final source. Reports are recorded
in reviews/release-fixture-{completeness,correctness}.md.

The initial read-only origin tag query stalled at Secretive SSH signing;
a bounded diagnostic retry ended with agent refusal and publickey denial.
This is an authentication blocker, not evidence of any published release.
No release attempt manifest or remote release refs have been created.
