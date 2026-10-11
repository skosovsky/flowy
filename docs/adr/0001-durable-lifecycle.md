# ADR 0001: rollover and explicit retention

Decision: use an addressed atomic rollover as the single primary growth mechanism. Keep exact retained aggregates intact; do not compact runtime outcomes into invented replay placeholders. Explicit dependency-safe retention removes old full payloads while permanent small anchors retain ID/fence/lineage integrity.

## Measured baseline

6 October 2026, Apple M1 Max, darwin/arm64; Go benchmark uses detached MemoryExecutionStore and full persisted JSON envelopes, 512-byte input/result, one graph node, no middleware, fixed single child concurrency. `go test -run '^$' -bench '^BenchmarkExecutionGrowth$' -benchmem -benchtime=1x -count=1`. Source: execution_growth_benchmark_test.go. This is a single-sample baseline, not a latency or production storage SLA. PostgreSQL storage measurements and after-mechanism workloads remain required.

| Workload | Records | Max aggregate bytes | History bytes | Commits | Allocated bytes | Allocations | ns/op |
|---|---:|---:|---:|---:|---:|---:|---:|
| activities | 16 | 30206 | 779349 | 50 | 10723728 | 13200 | 13138125 |
| activities | 64 | 118627 | 11601824 | 194 | 154021752 | 133126 | 140274791 |
| activities | 256 | 472378 | 182244155 | 770 | 2354577680 | 1869846 | 1939840625 |
| children | 16 | 51663 | 2238284 | 52 | 35523800 | 27030 | 34059208 |
| children | 64 | 203024 | 33553837 | 196 | 500764840 | 295667 | 371821375 |
| children | 256 | 808464 | 528010739 | 772 | 7858635488 | 4267516 | 5310280292 |

With each 4x record increase, retained full history grows about 15–16x. This reproduces quadratic serialization/history cost for these workloads; it does not claim every real workflow has a performance defect. Rollover permits bounded cycles, independent raw aggregate identities and reclamation of completed source payloads. It cannot reduce the cost of a single huge fan-out; host must bound that work too.

Compaction was rejected as the primary path because cached opaque results and immutable key/reference semantics would require additional blob or replay-index contracts. Removing outcomes would permit duplicate dispatch or make retained snapshots dependent on an external record store. A new generic storage engine is outside task25.

Atomic source/target publication is essential. Existing fork creates a target but retains source execution authority and therefore is insufficient. Rollover gets a distinct storage capability/anchor and does not add a compatibility wrapper around fork.

See ../durable-lifecycle.md for state/effects compatibility, safe boundary, retention/tombstone semantics and required backend evidence. Until those are implemented and tested, this ADR is not task acceptance.

## Memory result with bounded cycles

Same machine, serializers, 512-byte input/result and single-child concurrency;
`BenchmarkExecutionGrowthRollover`, `-benchtime=1x -count=1`. Each cycle contains
16 records. After transfer, source payloads are deleted; current execution keeps
one exact checkpoint. Total logical records match the baseline. Rollover commits
both envelopes, so the commit count includes both published revisions.

| Workload | Total records | Max complete aggregate B | Written history B | Retained payload B | Permanent metadata B | Commits | Allocated B | Allocations | ns/op |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| activities | 16 | 30349 | 784115 | 30349 | 105 | 50 | 10788496 | 13989 | 17192084 |
| activities | 64 | 32309 | 3366193 | 31241 | 6556 | 203 | 46410912 | 67989 | 104094542 |
| activities | 256 | 32313 | 13698353 | 31237 | 32366 | 815 | 190316320 | 287679 | 415459541 |
| children | 16 | 51706 | 2240746 | 51706 | 105 | 52 | 36206080 | 28131 | 41882000 |
| children | 64 | 53666 | 9262448 | 52600 | 6556 | 211 | 152031464 | 130046 | 316928166 |
| children | 256 | 53666 | 37353238 | 52600 | 32368 | 847 | 617343544 | 542755 | 945873958 |

Memory metadata is serialized heads, fence counters, lease identities and expiry,
fork creation anchors, and full incoming/outgoing receipts, including map keys.
It excludes Go map overhead; allocations are reported separately by the benchmark.
The retained payload count is exactly one for every row. The small differences
from the baseline's 16-record row include the explicit effects codec label and
execution IDs; latency is a single observation, not an SLA.

At 256 records, written history falls from 182 MB to 13.7 MB for activities and
from 528 MB to 37.4 MB for children. Retained full payload no longer accumulates
across completed cycles. Permanent anchors still grow linearly with execution IDs;
this design deliberately does not promise bounded total metadata or ID reuse.
Host admission must bound per-cycle fan-out, opaque state/effects and cumulative
budget/retry key dimensions. The measured policy is 16 records, 131072 complete
source bytes, 8192 complete fresh target bytes, with one retained active cycle.
These are benchmark inputs, not universal recommended production limits.
PostgreSQL storage measurements and final adversarial gates remain required.


## PostgreSQL measured comparison

Actual PostgreSQL 17 on local port55666, DBflowy_task25. Same machine, 512-byte input/result, one node, single-child concurrency. `go test -tags=integration -run '^$' -bench '^BenchmarkPostgresExecutionGrowth$' -benchtime=1x -count=1 -timeout=5m` from adapters/checkpointer/postgres. Terminal PASS97.173s. Both modes use the current codec labels and identical workloads; rollover splits into 16-record cycles and keeps one active exact revision.

| Rollover | Workload | Records | Max aggregate JSON B | Written JSON B | Retained SQL payload B | SQL head metadata B | Retained revisions | Commits | Allocated B | Allocations | ns/op |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| false | activities | 16 | 30706 | 793793 | 796098 | 220.0 | 50.00 | 50.00 | 14969912 | 21136 | 720987542 |
| false | activities | 64 | 120482 | 11789175 | 11798104 | 221.0 | 194.0 | 194.0 | 221418256 | 166940 | 2623036250 |
| false | activities | 256 | 479590 | 185052475 | 185087900 | 221.0 | 770.0 | 770.0 | 3583878744 | 2048963 | 22702442208 |
| false | children | 16 | 51743 | 2242518 | 2244915 | 220.0 | 52.00 | 52.00 | 48797304 | 35549 | 910864917 |
| false | children | 64 | 203103 | 33569878 | 33578899 | 221.0 | 196.0 | 196.0 | 715907824 | 336917 | 3730760958 |
| false | children | 256 | 808544 | 528072471 | 528107988 | 221.0 | 772.0 | 772.0 | 11284336440 | 4456758 | 42088812541 |
| true | activities | 16 | 30702 | 793721 | 30755 | 220.0 | 1.000 | 50.00 | 14967976 | 20800 | 285146625 |
| true | activities | 64 | 32852 | 3419669 | 31845 | 7818 | 1.000 | 203.0 | 68419928 | 101019 | 1296409291 |
| true | activities | 256 | 32852 | 13928524 | 31851 | 38244 | 1.000 | 815.0 | 282184368 | 423286 | 6848238292 |
| true | children | 16 | 51741 | 2242516 | 51794 | 220.0 | 1.000 | 52.00 | 48070120 | 35763 | 603189084 |
| true | children | 64 | 53789 | 9275846 | 52786 | 7818 | 1.000 | 211.0 | 205336016 | 164405 | 2664164125 |
| true | children | 256 | 53789 | 37413644 | 52786 | 38250 | 1.000 | 847.0 | 834606528 | 680913 | 11571025666 |

SQL bytes measure JSONB text via octet_length, not physical storage/WAL. Head metadata measures every retained head row with IDs/revision/fence/lease/creation and transfer anchors, including deleted source heads. Written JSON measures complete sealed envelope serialization at every committed revision, including both sides of transfer. At 256 records, SQL retained payload falls from185MB to31.9KB for activities and528MB to52.8KB for children; permanent metadata remains38.3KB across16heads. Baseline retains all history; rollover reclaims it with an explicit host maintenance call. Latency and allocations include runtime, serialization, transactions, measurement and maintenance; these single samples are observations and carry no production SLA.
