# Performance workloads and gates

CI requires measured benchmark cases and allocation ceilings from checked-in
manifests. Empty matches, skipped backend cases, zero iterations, incomplete or
non-finite measurements, missing PASS, failures and any over-limit repeat fail.
Negative fixtures live in `scripts/test_check_benchmarks.py`. Thresholds are fixed
inputs: CI never derives them from the output being checked. There is no absolute
ns/op gate on shared runners.

Baseline: Go 1.27.1, darwin/arm64, Apple M1 Max, GOMAXPROCS=10, PostgreSQL 17 in a
local container over loopback. No observer/exporter in hot/growth measurements.
Node/stream use normal benchmark calibration; growth/PostgreSQL use `1x`.
Discovery excludes fixture setup/cleanup; growth includes runtime, serialization
and publication. Database latency is not a portable timing baseline.

The manifests require 15 root and 14 PostgreSQL cases. Growth/discovery allocation
ceilings are measured B/op and allocs/op plus 35%, rounded up, allowing platform
variation while detecting substantial regressions. Node ceilings remain zero.
Streaming ceilings are 8192 B/op and 60 allocs/op against measured 6644 and 45.
Changing ceilings requires new measurements and review, not merely a CI failure.

## Workloads

- `BenchmarkExecuteNode_NoMiddleware` / `With5Middlewares`: actual `runNodeStep`
  handler, reducer, metadata, directive and event hooks, without sink/store.
  Scalar update and StepCount assertions prevent empty loops. Five middleware
  wrappers are constructed at compile time. Both measured zero allocations;
  this does not represent full Start/session/checkpoint cost.
- `BenchmarkOrdinaryStreaming`: real ordinary stream, drained events and checked
  completed result with no persisted token. Includes context/session/goroutine,
  channel and result overhead; does not represent provider token streaming.
- Root `BenchmarkExecutionGrowth` / `GrowthRollover`: activity journals and
  child fan-out at 16/64/256 records with 512-byte payloads. Reports allocations,
  commits, maximum aggregate, written/retained history. Rollover uses explicit
  16-record segments and retention. Full snapshots still grow with accumulated work.
- `BenchmarkPostgresExecutionGrowth`: the same sizes, activity/fan-out and
  with/without rollover, actual transactions and stored-byte/revision metrics.
  The gate also caps retained-revisions/op: 50/194/770 (activities),
  52/196/772 (children) without rollover, one retained payload revision with
  rollover. Separate metadata/tombstones are reported and remain stored.
- `BenchmarkIndexedDiscoveryTerminalArchive`: one due wait among 256 future
  waits with 0/20,000 terminal heads. Both must read one authoritative head/poll;
  the manifest enforces `heads/poll <= 1`. Single-poll allocations measured
  57488/67368 B/op and 512/552 allocs/op. Task26 separately verifies index plans
  and repeated polls; benchmarks do not replace correctness tests.

For 256 children root full-aggregate allocated about 7.86 GB versus 0.62 GB with
rollover; PostgreSQL measured about 11.30 GB versus 0.83 GB. These are cumulative
allocated bytes per workload, not resident memory or final stored payload size.

## Reproduce

```sh
set -o pipefail
python3 -m unittest discover -s scripts -p 'test_check_benchmarks.py'
go test -run='^$' -bench='BenchmarkExecuteNode_|BenchmarkOrdinaryStreaming' -benchtime=100ms -benchmem . | tee /tmp/flowy-root-bench.txt
go test -run='^$' -bench='BenchmarkExecutionGrowth' -benchtime=1x -benchmem . | tee -a /tmp/flowy-root-bench.txt
python3 scripts/check_benchmarks.py scripts/benchmarks-root.json /tmp/flowy-root-bench.txt
cd adapters/checkpointer/postgres
# FLOWY_TEST_DATABASE_URL must identify a disposable test database.
go test -tags=integration -run='^$' -bench='BenchmarkPostgresExecutionGrowth|BenchmarkIndexedDiscoveryTerminalArchive' -benchtime=1x -benchmem . | tee /tmp/flowy-postgres-bench.txt
python3 ../../../scripts/check_benchmarks.py ../../../scripts/benchmarks-postgres.json /tmp/flowy-postgres-bench.txt
```

CI runs root workloads once, not root-only benchmark names in every module. Its
PostgreSQL job uses an explicit database service and integration tag. Race suites,
backend tests, the PostgreSQL agent blueprint and ordinary cookbook smoke remain
separate correctness gates.
