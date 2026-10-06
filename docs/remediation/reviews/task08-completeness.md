# Task 08 — independent completeness acceptance

Base: `268f458` (`refactor: runner boundaries`). Reviewed final worktree changes
for D15–D21 against Task28 and the before-implementation Task08 contract in
`docs/remediation/task28.md`. Reviewer did not implement these changes.

Verdict: **100% complete — 7/7 required decision items**. No outstanding
completeness findings within Task08 scope.

| Item | Disposition and evidence | Complete |
|---|---|---|
| D15 | Clean-break BuildDispatchGraph replaces BuildSupervisor. BuildReAct names maxActionRetries and documents fallback rounds/maxActionRetries+1 action calls; evaluator corrections are distinct from transport retry. `TestPatternRetryBudgetsCountActionAndCorrectionFallbacks` asserts three producer/action/evaluation callbacks for two retries, six full-state updates and typed exhaustion. Pattern examples and current runtime documentation use admitted construction. | Yes |
| D16 | All three constructors reject missing required callbacks and nonpositive retry budgets before execution. Dispatch copies routes and captures workers, rejects reserved IDs/nil workers; Compile retains topology validation. ReAct evaluates the pending predicate in one Completed routing evaluation; non-Completed wrappers remain unchanged. Configuration rejection table, ownership mutation fixture, predicate-count fixture and effect/wrapper suites exercise these policies. Pure prompt concurrent callback requirements are documented. | Yes |
| D17 | Existing replacement reducers and fixed IDs retained intentionally. GoDoc and current runtime contract list react_reason/react_action, dispatch, generator/evaluator and require full-state updates. Tests assert replacement-state results and effect order. Persisted supervisor cursor migration/draining is explicit; no automatic rename or alias. | Yes |
| D18 | One slot per Go type retained; freeze-before-share is explicit discipline rather than new DI identity or reflected cloning. GoDoc/current contract define same-type collisions and unsupported concurrent mutation. `TestBindingsSameTypeSlotAndFrozenConcurrentReads` proves last setup value through both sentinels and eight concurrent readers. | Yes |
| D19 | WithLifecycleObserver/WithTelemetryBridge select per-context integrations; plain/typed nil explicitly disables, synchronized globals remain optional defaults. Context propagation through ordinary/durable entry points was inspected. Explicit OTel provider constructors avoid installation and core imports no mandatory telemetry SDK. Concurrent scoped observation, fallback/disable, detached carriers and isolated provider tests cover selection/privacy. Fresh resume scope reattachment and callback concurrency are documented. | Yes |
| D20 | Bridge methods are Capture/Restore throughout production/consumers, without old-method adapters. Installation returns provider errors without duplicate logging. Observer panic containment and synchronous prompt nonpanicking bridge policy are explicit; core detaches carrier maps in both directions. Constructor rejection/no-log and carrier ownership tests supply evidence. | Yes |
| D21 | Default metric attributes remain bounded operation/stage; bounded code mapping remains in tracing. W3C-only traceparent/tracestate propagation excludes baggage. Observation has no domain payload/evidence/raw error fields; arbitrary codes collapse to other. Existing thousand-address cardinality and baggage/privacy fixtures plus new provider privacy test pass. Current observation contract denies commit/delivery authority from telemetry. | Yes |

Independent executed checks:

- `GOCACHE=/tmp/flowy-task28-go-cache go test -race -count=1 -timeout=5m ./patterns ./ext/otel`: exit0; patterns 1.858s, OTel 2.065s. Log: `/tmp/flowy-task28-task08-completeness-patterns-otel.log`.
- `GOCACHE=/tmp/flowy-task28-go-cache go test -race -count=3 -timeout=5m -run 'Test(ContextObservationIsolationAndExplicitDisable|ContextBridgeIsolationDetachedCarriersAndDisable|BindingsSameTypeSlotAndFrozenConcurrentReads)$' .`: exit0. Log: `/tmp/flowy-task28-task08-completeness-scopes.log`.
- `git diff --check`: PASS. Go source search found no BuildSupervisor or old bridge method implementations (search exit1 denotes no matches).

Parent final evidence inspected separately: full root race exit0, root11.176s;
final lint log contains `0 issues.`. These are corroborating evidence, not claimed
as independently rerun full-module gates. Separate example module compile/race
is build evidence only because it has no untagged tests.

Limits: acceptance covers Task08, not Task28 completion. No adapters were changed
in Task08; native backend, all-six-module final gates, benchmarks and remaining
D22–D62/documentation requirements stay assigned to later tasks. Race results
cover executed paths; frozen bindings deliberately do not promise safe concurrent
mutation, and host bridge/observer implementations own the documented callback
requirements. No real release, push or PR was performed.
