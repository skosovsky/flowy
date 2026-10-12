# Ordinary stream admission evidence

Task: `.cursor/tasks/performance-memory-2026-10-12.md`.
Baseline: `8ad714f3954c4773a31aa9d723fff28c07673ef5`.
The frozen [18-requirement checklist](requirements.md) was written before implementation. Runtime admission documentation and regression assertions were written before the production change. Final review uses the commit containing this evidence; reviewer verdicts are recorded in the task chat against that SHA.

## Implementation

The single producer checks stop/event-context cancellation, then buffer capacity. Publication is a plain method called by the original producer-local sink closure. A full-buffer observation returns immediately without state/effect cloning, even when a consumer subsequently drains a slot. Otherwise pure host cloners synchronously detach state and present effects, cancellation is checked again, and publication uses the existing nonblocking select. Concurrent consumers only free capacity. Stream lifecycle, WaitResult, terminal-loss policy, durable journal encoding/history, public signatures and dependencies are unchanged.

The final regression retains the original audit measured invocation: stream/context/goroutine creation, 64-KiB source allocation, 1024/256/32 sink attempts, Wait, source mutation, drain and ownership assertions all remain inside each iteration. Only expectations changed from 1024 copies to 32. Static failure errors are returned to the parent test instead of calling Fatal from a producer goroutine. BenchmarkOrdinaryStreaming and ExecuteNode fixtures are unchanged.

## Reproduction and environment

Go: `go version go1.27.2 darwin/arm64`. Machine: Apple M1 Max (benchmark headers), default `-10` benchmark parallelism on both versions. Root module only. All Go invocations used `GOWORK=off GOCACHE=/tmp/flowy-memory-gocache`; lint also used writable `GOLANGCI_LINT_CACHE=/tmp/flowy-memory-lintcache`.

Baseline was unpacked with `git archive 8ad714f3954c4773a31aa9d723fff28c07673ef5 | tar -x -C /tmp/flowy-memory-baseline`, then the original `.cursor/tasks/evidence/performance-memory-2026-10-12/audit_performance_test.go` copied into that isolated root package. No baseline probe was copied into the working checkout. The original probe's diagnostic PASS characterizes the inefficiency.

Commands, run serially (benchmark invocations had no concurrent compilation, race, lint or agent Go workload):

```sh
# isolated baseline root
GOWORK=off GOCACHE=/tmp/flowy-memory-gocache go test -run TestAuditDroppedEventClones -v -count=1
GOWORK=off GOCACHE=/tmp/flowy-memory-gocache go test -run '^$' -bench 'BenchmarkAuditStreamSaturated|BenchmarkExecuteNode|BenchmarkOrdinaryStreaming' -benchmem -benchtime=100ms -count=3
# final candidate root
GOWORK=off GOCACHE=/tmp/flowy-memory-gocache go test -race -count=1 ./...
GOWORK=off GOCACHE=/tmp/flowy-memory-gocache go test -race -count=1 -run 'TestAuditDroppedEventClones|TestStreamAdmission|TestTask23ConcurrentMutableConsumer|TestTask23MutableEventAndStorageIsolation|TestStreamWaitResult' -v -timeout=3m
GOWORK=off GOCACHE=/tmp/flowy-memory-gocache go test -run '^$' -bench 'BenchmarkAuditStreamSaturated|BenchmarkExecuteNode|BenchmarkOrdinaryStreaming' -benchmem -benchtime=100ms -count=3
GOWORK=off GOCACHE=/tmp/flowy-memory-gocache GOLANGCI_LINT_CACHE=/tmp/flowy-memory-lintcache golangci-lint fmt --diff
GOWORK=off GOCACHE=/tmp/flowy-memory-gocache GOLANGCI_LINT_CACHE=/tmp/flowy-memory-lintcache golangci-lint run --allow-serial-runners ./...
```

Raw outputs: [baseline diagnostic](baseline-test.log), [baseline samples](baseline.log), [candidate samples](candidate.log), [full root race suite](race.log), [targeted race](targeted-race.log), [format](format.log), [lint](lint.log). Empty format output is PASS; lint output reports zero issues. Full race suite includes root contract tests, mutable ordinary/durable ownership, WaitResult/no-drain, cancellation and cleanup; goleak TestMain remains unchanged.

## Final short benchmark gates

Allocation gates PASS in each of three samples. Timing remains diagnostic; the long follow-up below assesses >10% regressions before acceptance.

|Benchmark|Baseline median ns/op|Candidate median ns/op|Change|Candidate max B/op|Candidate max allocs/op|
|---|---:|---:|---:|---:|---:|
|BenchmarkAuditStreamSaturated/clone=false/events=32|17427|19727|+13.20%|70384|17|
|BenchmarkAuditStreamSaturated/clone=false/events=256|51075|37568|-26.45%|70384|17|
|BenchmarkAuditStreamSaturated/clone=false/events=1024|164258|97432|-40.68%|70384|17|
|BenchmarkAuditStreamSaturated/clone=true/events=32|310793|280590|-9.72%|2167587|50|
|BenchmarkAuditStreamSaturated/clone=true/events=256|1.73543e+06|319034|-81.62%|2167602|50|
|BenchmarkAuditStreamSaturated/clone=true/events=1024|6.72041e+06|377045|-94.39%|2167581|50|
|BenchmarkExecuteNode_NoMiddleware|142.6|141.9|-0.49%|0|0|
|BenchmarkExecuteNode_With5Middlewares|172.1|170.5|-0.93%|0|0|
|BenchmarkOrdinaryStreaming|7515|7868|+4.70%|6957|49|

## Timing follow-up

An intermediate closure-factory refactor added 208 B/op and one allocation per stream and produced >10% short timing medians in several controls. [Its samples](superseded-closure-candidate.log) are retained for transparency. That extra closure was removed by using a plain publication method. Final candidate allocation gates are checked again. Baseline and final candidate are additionally compared with 1-second runs, six samples each, using benchstat; no release or dependency changes are required to run that tool.

Benchstat tool: `golang.org/x/perf/cmd/benchstat@v0.0.0-20261009192801-be2c69fb417e`, installed outside the repository with writable GOMODCACHE/GOBIN. Commands:

```sh
# once in isolated baseline, once in final candidate, sequentially
GOWORK=off GOCACHE=/tmp/flowy-memory-gocache go test -run '^$' -bench 'BenchmarkAuditStreamSaturated|BenchmarkExecuteNode|BenchmarkOrdinaryStreaming' -benchmem -benchtime=1s -count=6
/tmp/flowy-memory-bin/benchstat docs/evidence/performance-memory-2026-10-12/baseline-long.log docs/evidence/performance-memory-2026-10-12/candidate-long.log
```

### Long-run assessment and acceptance decision

[Baseline long samples](baseline-long.log), [final candidate long samples](candidate-long.log), [benchstat output](benchstat.log): six 1-second samples per benchmark, serial runs on the same toolchain/machine. All allocation gates PASS in every long sample as well: cloned 256/1024 stay at 50 allocs/op and approximately 2.168 MB/op; OrdinaryStreaming stays at 49 allocs/op and 6956 B/op; ExecuteNode controls stay at zero.

The short no-cloner/32 regression (+13.20%) becomes +8.44% in the longer run (19.30→20.93 µs, p=0.485). Long clone/32 has a higher point estimate (399.9→532.5 µs, p=0.093), and ExecuteNode_NoMiddleware does too (145.1→252.5 ns, p=0.310); variability is wide, including ±254% for the baseline node control. These comparisons are inconclusive for latency: lack of statistical significance does not prove equivalence or exclude a slowdown. The unchanged ExecuteNode fixture runs no streaming code. No external machine load control was claimed; isolation means no simultaneous agent benchmark/compile/race/lint workloads.

Acceptance decision: the required longer timing assessment is complete, allocation thresholds and deterministic ownership/cancellation contracts pass. The task defines no hard latency SLA. Accept the bounded allocation fix with the documented timing uncertainty; do not claim a latency guarantee or interpret these samples as cold-start measurements. The independent reviewers assess this decision against the frozen requirement 14.

## Limits

Allocation churn is reduced; these results do not establish a retained-heap leak or prove the absence of all leaks/errors. Adapter performance, live backends, consumer workloads and cold-start latency were not measured. Short samples do not define a latency SLA. Durable serialization/history is a separate workload. No release or consumer pins were changed.
