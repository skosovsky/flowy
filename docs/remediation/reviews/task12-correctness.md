# Task12 independent correctness review

Reviewed the final working tree against HEAD 22517a3 and the Task12 contract:
D60, D61 and documentation requirements 2/4/5/6/7. Verdict: **0 open errors**.
No implementation or contract files were edited by this reviewer.

The fuzz runner discovers all six go.mod modules and their packages, enumerates
actual probes, runs one anchored exact name per command and propagates discovery
and execution failures. Empty discovery fails; positive duration/worker admission
and explicit parallelism bound the configured execution. The two properties use
bounded inputs and assert header rejection, byte/address integrity and payload
tamper refusal or JSON record decode/address rejection. They do not claim host
codec completeness, JSONB server-domain coverage or exhaustive interleavings.

README's complete BYOT program matches the executable hello and actual API.
All six module paths and Go 1.27.1 minimum match go.mod files. Current ownership,
panic and typed recovery recipes agree with the implementations inspected;
unknown ACK does not become rollback or exactly-once delivery. Durable Start and
Stream call prepareStart/commitExecution before node execution or handle return;
Resume paths admit their saved boundary before continuation. Cookbook separates
ordinary stubs, nondurable memory smoke and the real PostgreSQL blueprint with
fake external ports. The canonical durable profile wording includes synchronous
and asynchronous entry points. Historical evidence remains historical.

Independent terminal checks:

- Race seed checks: `GOCACHE=/tmp/flowy-task28-go-cache go test -race -count=1
  -run '^FuzzExecutionEnvelopeIntegrity$' .` passed (1.616s), then the corresponding
  `-run '^FuzzSnapshotRecordAdmission$' ./checkpoint` passed (1.447s); session66601
  exited0 after both results. Initial empty compilation output was not acceptance.
- `python3 -m unittest discover -s scripts -p test_run_fuzz.py`: 4 tests passed,
  covering exact targets, empty discovery, discovery errors and execution errors.
- `go run ./examples/hello`: exited0, printed `Hello, Sergey` twice as documented.
- Actual module discovery returned exactly the root, four adapters and blueprint.
- `python3 scripts/run_fuzz.py --seconds 0 --workers 2`: rejected with exit2.
- `git diff --check`: exited0.

Also inspected the parent's terminal real timed-fuzz log
`/tmp/flowy-task28-task12-fuzz.log`: each of two probes ran for the requested3s
with2workers; envelope1563 executions and checkpoint48477 executions, both PASS,
followed by Completed2. Stat-cache permission warnings did not become a silent
empty success. This is observed parent evidence, separate from independent seeds.

Limits: this review did not run all-module native suites, release/installability
or growth gates; those remain Task13. Seed PASS is not timed fuzz coverage, and
passing fuzz properties do not prove absence of all bugs. No lint was launched
concurrently by this reviewer.
