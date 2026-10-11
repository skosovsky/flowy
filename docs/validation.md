# Validation contract

Go 1.27.1 or newer and golangci-lint 2.14 are the configured baseline. Makefile and
CI discover project modules by go.mod, excluding hidden/vendor paths.
Current inventory has exactly seven modules:

| Directory | Ordinary tests/lint | Tagged native gate |
| --- | --- | --- |
| `.` | core, checkpoint, patterns, OTel, cookbook/memory examples | no adapter imports in root native tests |
| `adapters/checkpointer/postgres` | snapshot and execution adapter | PostgreSQL snapshot/outbox/fence/lifecycle/discovery |
| `adapters/checkpointer/redis` | snapshot adapter | standalone Redis Lua/OCC/TTL |
| `adapters/lease/postgres` | lease adapter | independent PostgreSQL connection locks/fences |
| `adapters/lease/redis` | lease adapter | standalone Redis incarnation/atomic renew/release |
| `examples/durable_agent` | separate blueprint module | real PostgreSQL recovery across six fresh worker pools |
| `examples/approval_recovery` | optional authenticated approval consumer | PostgreSQL + file journal, abrupt OS process recovery |

`go test ./...` from root excludes every nested module. `make test`,
`make test-race`, `make test-goleak` and `make lint` visit all seven; test-goleak
repeats uncached suites that contain leak checks, not a universal leak proof.
Lint does not claim native coverage. Run tagged lint with `--build-tags=integration`
for changed adapters. `make verify-stress` targets ordinary handoff/resume/orphan
contracts; targeted deterministic fault tests remain separate.

## Native infrastructure

Use disposable PostgreSQL and standalone Redis. Set FLOWY_TEST_DATABASE_URL to
an explicit connection URL and FLOWY_TEST_REDIS_ADDR to host:port; use the port
actually allocated to that container. Optional test passwords/DB selection are
specified by each adapter fixture. Native suites may skip without infrastructure;
a skip is not backend acceptance. The blueprint instead fails on missing DSN and
needs CREATE/DROP SCHEMA permission: it creates/drops only its random schema.
Do not run acceptance against production data or stop unrelated containers.

```sh
export FLOWY_TEST_DATABASE_URL='postgres://user:password@127.0.0.1:5432/flowy_test?sslmode=disable'
export FLOWY_TEST_REDIS_ADDR='127.0.0.1:6379'
# Execute separately inside all four adapter directories and examples/durable_agent:
go test -v -race -tags=integration -count=1 -timeout=10m ./...
```

CI's integration matrix contains those five modules and supplies both services;
the root race/lint matrix includes all seven modules. Real PostgreSQL proves native
transactions/locks/anchors, not external provider behavior. The agent blueprint
uses fake model/tools while its execution persistence is PostgreSQL. Ordinary
cookbook smoke and durable_runtime memory smoke do not replace native gates.

## Fuzz scope and truthful discovery

`make fuzz` runs scripts/run_fuzz.py: discover every module/package, enumerate
actual Fuzz functions, then run each exact function separately for 30s with two
workers. Discovery or run failure fails the command; zero probes fails rather
than returning empty success. Packages without probes acquire no fuzz coverage.
For a bounded local sample:

```sh
python3 -m unittest discover -s scripts -p 'test_run_fuzz.py'
python3 scripts/run_fuzz.py --seconds 3 --workers 2
```

Current probes: FuzzExecutionEnvelopeIntegrity checks invalid UTF-8/empty IDs,
zero revisions, opaque state byte preservation, seal tamper and exact address;
FuzzSnapshotRecordAdmission checks JSON/header rejection, JSON record decoding and
address mismatch. Inputs above 1024 identity bytes or 8192 payload bytes are skipped
by these probes to bound work. Seed cases run in normal go test; timed fuzzing is
a separate gate. This covers executed properties, not arbitrary host codecs,
JSONB's server-specific domain, all interleavings, network failures or formal
proof of absence of bugs. Record duration/command and discovered names.

## Performance, release and installability

[Performance gates](performance-gates.md) require 15 root and 14 PostgreSQL cases
from the existing fixed manifests. Empty/incomplete/skipped results and allocation
ceiling violations fail; shared-host ns/op is observation, not SLA. Growth cases
at 16/64/256 include accumulated snapshot work; no hidden compaction is implied.
Copy microbenchmarks are local DTO fixtures, not backend performance evidence.

Run benchmark checker rejection fixtures and [isolated release fixtures](release.md).
Consumer installability must use all seven module paths from one intended commit,
without local replaces; test via a disposable local module proxy when the commit
is unpublished. This does not publish remote refs or prove a release occurred.
The final task28 acceptance journal records actual terminal results and review
verdicts. Historical task22–27 evidence is not rewritten as new acceptance.


Repeatable unpublished-consumer check:

```sh
python3 scripts/check_installability.py
```

The script archives committed HEAD only, edits dependency versions/replaces only
inside its temporary directory, packages each of seven modules separately and uses
a synthetic v0.0.0-task28 version served by its local file proxy. It builds a
consumer importing core and all four adapters and installs the blueprint binary.
GOWORK and persisted Go environment are disabled, caches are fresh and downloaded
first-party zip bytes must equal the intended local artifacts. Public pinned
dependencies may download from proxy.golang.org; GOSUMDB is disabled inside this
isolated synthetic-version check. This is source/module graph installability,
not verification of an actual published tag, checksum-log entry or remote release.
The temporary path/result.json is printed and retained for evidence. Repository
refs/index/worktree and user Go environment are not rewritten by this command.


## Approval semantic consumer gate

The `approval-consumer` CI job executes both checkout and published modes with a
real PostgreSQL service. Run `python3 scripts/check_approval_consumer.py --mode
checkout` or `--mode published` with FLOWY_TEST_DATABASE_URL set. Both run all
semantic tests and the required process integration suite with race/count=1.
Published mode downloads released dependencies with GOWORK=off and removes all
local replacements. The root test matrix discovers the seventh optional module;
its operation backend stays out of the root dependency graph. Missing infrastructure
fails this gate. No physical disk-crash acceptance is claimed.
