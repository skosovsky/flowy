# Task07 independent completeness acceptance

Reviewer: `/root/task07_completeness`; implementation was performed by the parent.
Scope: Task07 against the original task28 D01, D02, D03, D04, D06, D11 and D13,
its pre-implementation contract and current diff from `1f08d04`.

Verdict: **100% (7/7 checklist requirements)**. No completeness gaps remain in
this task. This is task acceptance, not acceptance of the remaining task28 matrix.

| Requirement | Credit | Evidence |
| --- | --- | --- |
| D01 explicit persistence profiles and file split | 1/1 | Ordinary runner is split into runner.go, runner_execution.go, runner_sessions.go, runner_recovery.go, runner_publication.go and runner_stream.go; capture lives in compose_capture.go. Durable ownership was separately extracted into execution_session.go. All 75 prior runner function declarations remain after intentional AsNode naming replacement; durable declarations also remain. Constructors still select snapshot versus aggregate execution explicitly. Runtime contract documents the profiles. |
| D02 remove shipped test scaffolding | 1/1 | subgraphTestMode, failingCaptureCheckpointer and bumpRevisionOnLoadCP are exclusively in compose_fault_test.go. Production subgraphNodeWithCheckpointer is a private invocation-local factory seam, and public constructors always provide ordinary capture storage. Fault test callsites use faultSubgraphNodeWithSlot. `go list` confirms the fault file appears only under TestGoFiles; no global hook or exported test mode exists. |
| D03 clarify inline continuation | 1/1 | Clean-break AsStatelessNode and StatelessSubgraphNode replace ambiguous names without aliases; SubgraphNodeWithSlot remains the explicitly continuing variant. GoDoc and runtime-contract state that Suspend/Handoff pause the parent and stateless reinvocation starts the inner entry. TestStatelessNodeRestartsInnerAfterSuspend proves restart, existing slot handoff/suspend tests prove continuation. Durable inline admission remains rejected. Current example and test consumers are updated. The permitted naming disposition deliberately retains useful parent boundary directives. |
| D04 invocation identifiers and revision | 1/1 | SubgraphSlot.Revision GoDoc and runtime contract explicitly retain it as informational capture progress, not an OCC authority. Synthetic identifiers and capture history are invocation-local. Slot resume seeds the new capture with expected revision zero and consumes the returned revision. Parent snapshot revision/fencing remains publication authority. |
| D06 advisory guard | 1/1 | NewAdvisoryLeaseGuardCheckpointer replaces the old constructor, including auto-wrap and testutil callsites. Documentation spells out separate Holder/delete race and native same-domain alternative. Guard does not implement native fencing marker; transactional forwarding still resolves the actual inner capability, preserving atomic-handoff admission. |
| D11 reserved EndNode | 1/1 | validateHandlers rejects EndNode registration at Compile, preserving fluent AddNode contract. TestCompileRejectsReservedTerminalNode exercises rejection with an otherwise admitted terminal graph. |
| D13 stable diagnostics | 1/1 | Compile sorts complete diagnostic texts before errors.Join. TestCompileDiagnosticsAreStableAcrossRegistrationOrder compares 50 alternate registration/map traversals and checks every node/edge diagnostic remains present. Existing joined diagnostics tests still run. |

Independent checks on the final source state:

- `GOCACHE=/tmp/flowy-task28-go-cache go test -race -count=2 -timeout=5m -run 'Test(Compile|.*Stateless|.*Subgraph|.*LeaseGuard)' ./...`: exit 0; root PASS 1.890s. Log `/tmp/flowy-task28-task07-completeness-targeted.log`. Other packages marked no tests to run are compilation evidence only, not targeted behavioral proof.
- `go list -f '{{.GoFiles}} | test files: {{.TestGoFiles}}' .`: exit 0; production package inventory includes extraction files and excludes test fault machinery.
- Production-source search for scaffolding types and removed API declarations: no matches. Current README/docs/examples search finds only the intentional prose migration list and new StatelessSubgraphNode spelling.
- `git diff --check`: exit 0.
- Inspected parent fresh final root race log: all root packages PASS (root 12.105s); lint log reports 0 issues; relevant root examples compiled. Inspected separate durable_agent example log showing build success. Parent owns those process exit-code verification records.

The intermediate missing-type run described in the journal is not counted as PASS.
No live backend rerun is demanded by this task: it changes no adapter protocol or
storage operation. Backend checks and all-six-module final DoD remain later gates.
