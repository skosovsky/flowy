# Task 08 independent correctness review

Verdict: **ошибок не обнаружено; 0 open findings**.

Review role: independent correctness reviewer, not an implementer. Base commit:
`268f458` (`refactor: runner boundaries`). Reviewed the final tracked diff and
untracked Task08 regression tests against D15–D21 in
`.cursor/tasks/task28-flowy-review-remediation.md` and the Task08 contract in
`docs/remediation/task28.md`.

## Inspected boundaries

- D15–D17: pattern constructors reject missing callbacks/nonpositive budgets before
  execution; dispatch rejects reserved/nil worker definitions and snapshots routes
  and function values. Remaining graph topology is checked by Compile. ReAct
  evaluates the pending predicate once for Completed routing; non-Completed
  directives and effect wrappers remain preserved. Retry counts agree with the
  existing fallback accounting: budget two permits three consumer callbacks.
  Replacement updates/fixed IDs are documented; the dispatch cursor clean break
  has explicit migration/draining guidance rather than an alias.
- D18: type-only binding identity agrees with Go generic key identity. Same-type
  sentinels overwrite the same slot. Frozen reads share an immutable map; concurrent
  mutation is explicitly unsupported, with resource ownership retained by hosts.
- D19: context scope takes precedence over synchronized defaults; nil and typed nil
  scope disable integration explicitly. Slot locks end before callbacks, avoiding
  callback/setter reentrancy deadlocks. Contexts retain host instance selection
  through runtime derivations; a fresh resume worker must reattach scope as stated.
  OTel constructors use explicit providers without installing a global observer.
- D20: Capture/Restore names and consumers are consistent, with no old-method
  compatibility wrappers. Carrier maps are copied in both directions. OTel restore
  derives from the supplied context. Observer panics remain contained; prompt,
  synchronous, nonpanicking bridge policy is explicit. Installation errors return
  to the host without duplicate library logging.
- D21: metrics retain only bounded operation/stage labels; tracing uses bounded
  codes/status text and runtime identities, without raw error/evidence/payload.
  Default bridge remains W3C traceparent/tracestate only. Observation is explicitly
  correlation, not commit/delivery authority. No persistence, fencing, unknown
  outcome or outbox behavior was replaced by telemetry.

## Independently executed checks

All commands used `GOCACHE=/tmp/flowy-task28-go-cache` from repository root.

1. `go test -race -count=3 ./patterns ./ext/otel`: exit 0;
   patterns 1.264s, ext/otel 1.662s. Includes configuration/ownership/predicate,
   retry-count/effect-wrapper, provider-error/no-log, isolation/privacy, bounded
   metric and W3C carrier regression coverage.
2. `go test -race -count=3 -run 'Test(ContextObservation|ContextBridge|RunBindings|Telemetry|Durable.*Telemetry)' .`:
   exit 0, root 2.022s. Actual new matches include context observation and bridge
   isolation/disable; the binding/restoration checks were additionally selected
   explicitly in the following command rather than inferred from this selector.
3. `go test -race -count=3 -run 'Test(BindingsSameTypeSlotAndFrozenConcurrentReads|DurableResumeRestoresCarrierBeforeNodeAndTerminalReplay)' .`:
   exit 0, root 1.553s.
4. `go test -race -count=1 ./...`: exit 0; root 12.169s,
   checkpoint 1.825s, ext/otel 1.289s, patterns 1.229s, testutil 1.581s;
   durable_runtime and late_prompt_agent tests pass. Other listed root examples
   compile with no tests, including changed multi_agent/react_agent consumers.
5. `git diff --check`: no whitespace diagnostics. Reviewed final source diff,
   test declarations and runtime capture/restore call sites.

## Limits

This verdict applies to the Task08 source/doc change, not overall Task28 completion.
Separate native adapter modules and their live backend integration gates were not
rerun by this review because Task08 changes no adapter code; their final six-module
DoD remains Task13. No claim of backend PASS or release/installability PASS is made.
The separate durable_agent module build and parent lint are parent evidence, not
independently executed review checks. Deliberately violating documented host
callback/provider contracts (blocking forever, panicking bridges, concurrent
binding mutation, returning invalid OTel collaborators) is outside the supported
configuration contract; no hidden worker/preemption guarantees are inferred.
