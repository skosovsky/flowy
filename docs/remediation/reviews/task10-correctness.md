# Task10 independent correctness review

Base: `7a0a8d3`. Scope: D30–D35, D37, D38 of the original Task28 source,
the before-implementation contract and its D30 amendment, final tracked and
untracked implementation/test/documentation changes. Reviewer did not implement
the changes. Final review includes the clean ErrChildInvalid rename.

Verdict: **ошибок не обнаружено; 0 открытых findings**.

Reviewed evidence:

- D30: ComputeChildBudgetReturn remains arithmetic only; every Go caller uses
  the new name. ErrChildInvalid now describes the broad invalid child contract
  boundary; all sentinel callers and error assertions were updated. No old Go
  symbol or compatibility alias remains. Stored error text is historical and
  terminal replay does not reconstruct live sentinel identities.
- D31/D33: group admission remains bounded by MaxConcurrency, with no new claim
  of host-wide goroutine preemption. Dispatcher context masking and inherited
  parent accounting capabilities match the stated serialization requirement.
  Allocation/used/returned units remain separate from monetary accounting.
- D32: explicit copy covers plan children and each input/allocation, record
  specs/results, all three child provenance pointers, merged IDs/result,
  capacity, cancel-request pointer, budget-return map and both nested maps.
  Scalar strings/time values are copied without JSON normalization. Nil and
  allocated-empty shapes survive; ownership mutation tests cover every current
  mutable field. Admission remains in the existing central validators.
- D34/D37: dispatcher error/panic/canceled invocation remains conservative
  unknown. Central flat DTO validation, settled-state rules, empty completed
  decoding and partial outcome state preservation match the documented matrix.
  No optimistic redispatch, presence-bit or new external-success inference.
- D35: typed merged Unmarshal occurs after JoinChildren publication. New fault
  tests distinguish inspect/redecode and immutable terminal-failure replay from
  an active node handling the error with a current exact cached join assertion.
  Both paths retain one dispatch and one merge; live ErrChildCodec identity is
  not promised on persisted ErrExecutionFailed replay.
- D38: late finish retains original invocation revision/incarnation and store
  fencing. WithoutCancel preserves values and adds a five-second deadline.
  The strengthened noncooperative test verifies bounded detached context and
  rejection of the obsolete worker write. Existing migration/reference tests
  cover exact original identity, missing/forged bindings and no redispatch.

Independent final command:

`GOCACHE=/tmp/flowy-task28-go-cache go test -race ./... -run 'Test(Child|TypedJoin|TypedChild|ProjectChild|DecodeChild)' -count=1`

Terminal session `63643`: **exit0**, root `2.866s`, no race/test failures.
Raw log: `/tmp/flowy-task28-task10-correctness-race-final.log`. Other packages in
this filtered command compiled successfully but report no matching tests; this
does not constitute a full-suite gate. `git diff --check` also exited0.
Intermediate pre-rename independent run `78073` exited0 (root2.570s), retained
only as intermediate evidence and superseded by the final command above.

Parent confirmed final renamed lint session89256 terminal exit0, 0 issues,
`/tmp/flowy-task28-task10-lint-renamed.log`. Earlier root95368/target90516/lint68070
were confirmed exit0 (17.504s/1.690s/0 issues) before the final rename and do not
replace the required fresh final root gate. Parent subsequently confirmed final
renamed root19196 terminal exit0, root16.461s and all root packages PASS,
`/tmp/flowy-task28-task10-root-race-renamed.log`.

Limits: this review verifies Task10 source and child/typed race regressions. It
does not claim all six modules, native PostgreSQL/Redis or blueprint integration,
release/installability fixtures, final aggregate DoD, or a stable performance
threshold. The copy benchmark is a JSON-representable memory fixture, not an
admitted execution or backend measurement. Those wider gates remain Task13.
