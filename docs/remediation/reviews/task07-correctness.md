# Task 07 — independent correctness acceptance

Base: `1f08d04` (`fix: storage admission`). Final implementation inspected before
Task07 commit. Reviewer did not implement or modify runtime code.

Verdict: **ошибок не обнаружено; 0 незакрытых замечаний**.

## Evidence and reviewed boundaries

- D01: inspected all tracked diff and newly added runner split/capture/session
  files. Literal declaration comparison against HEAD confirms every moved
  runner declaration is preserved, except the explicitly required stateless and
  advisory constructor renames. The complete durable executionSession block
  matches the prior source exactly. Ordinary versus durable persistence selection,
  callback ordering, fencing, cancellation, cleanup and publication therefore
  retain their existing implementation, verified by full race tests.
- D02: production source contains no subgraphTestMode, fault implementations or
  context fault selector. The private generic factory allocates invocation-local
  storage; shipped constructors always allocate captureCheckpointer. Fault storage,
  test context selectors and construction are entirely in compose_fault_test.go.
  No mutable global hook or externally exposed injection API was added.
- D03/D04: renamed stateless adapters retain documented parent-only Suspend/Handoff
  with inner-entry restart. Existing restart test passes. Slot continuation keeps
  the persisted pointer/state/effect export cursor and seeds at expected revision
  zero, then resumes using the actual returned capture revision. It does not claim
  separate OCC or independent durable-child semantics. Durable-inline rejection,
  effect propagation and handoff boundaries pass in the full suite.
- D06: the advisory wrapper checks Holder then delegates DeleteIfIdle; it acquires
  no atomic/native fencing marker. Constructor naming/GoDoc and runtime contract
  explicitly describe the coordination race and native alternative. Transactional
  forwarding and nontransactional capability detection retain prior semantics.
- D11: EndNode registration is rejected at Compile, including entry-point use.
  Legitimate terminal routing remains supported; all existing builder/runtime tests
  pass. Fluent registration never silently hides the handler.
- D13: sorting reorders existing error values before errors.Join; it does not replace
  values or flatten their causes. Aggregate diagnostics retain all validation
  classes. Fifty alternating registration iterations assert identical complete
  diagnostic text with every expected missing-node/handler/router diagnostic.
- Current Go consumers use the new APIs. No production Go legacy declaration or
  call remains. README/runtime contract and affected example describe the names
  and stateless restart semantics. Historical remediation documents may mention
  superseded API names as history; no compatibility aliases were added.

## Independent executed checks

1. `GOCACHE=/tmp/flowy-task28-go-cache go test -race -count=3 -run
   'TestCompileRejectsReservedTerminalNode|TestCompileDiagnosticsAreStableAcrossRegistrationOrder|Test.*Subgraph|Test.*Stateless|Test.*LeaseGuard' ./...`
   — exit 0; root PASS 2.131s. Log:
   `/tmp/flowy-task28-task07-independent-correctness.log`.
2. `GOCACHE=/tmp/flowy-task28-go-cache go test -race -count=1 ./...`
   — exit 0; root PASS 10.744s; checkpoint, examples with tests, ext/otel,
   patterns and testutil also PASS. Log:
   `/tmp/flowy-task28-task07-independent-full-race.log`.
3. `git diff --check` — exit 0.
4. Production-source inventory and literal moved-declaration comparison — no
   unexpected semantic delta or shipped fault scaffolding.

Parent gate evidence additionally inspected: final root lint log reports 0 issues;
final root race log reports root PASS 12.105s; durable_agent example module reports
successful compilation with no tests. These are corroborating parent evidence,
not claimed as independently executed commands.

Limits: no separate native-backend integration or six-module final DoD rerun was
performed by this reviewer. Task07 changes no adapter persistence implementation
or schema; those broader requirements remain assigned to the final task. This
acceptance is scoped to D01/D02/D03/D04/D06/D11/D13 and the current Task07 diff.
