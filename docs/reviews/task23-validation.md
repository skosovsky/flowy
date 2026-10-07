# Task 23 validation evidence

Implementation scope: authenticated host approval/recovery recipe in an optional
consumer module; generic read-only core `InspectExecutionResume` helper; independent
semantic CI lanes and seven-module installability. Backend binding checks are real;
production authorization and storage remain host responsibilities.

## Pre-publication results

Executed with isolated Go/build/lint caches and a disposable PostgreSQL service.

| Gate | Actual result / retained local evidence |
| --- | --- |
| `make lint` | PASS, 0 issues in all seven modules; `/tmp/flowy-task23-make-lint4.txt` |
| `make test` | PASS all seven modules; `/tmp/flowy-task23-make-test2.txt` |
| `make test-race` | PASS all seven modules; `/tmp/flowy-task23-make-race.txt` |
| Tagged example lint | PASS 0 issues; `/tmp/flowy-task23-lint6.txt` |
| Example semantic race tests after review fixes | PASS; `/tmp/flowy-task23-unit4.txt` |
| Abrupt-process matrix + persistent attempt counter | PASS, 171.010s; `/tmp/flowy-task23-process2.txt` |
| Current local source semantic/integration consumer | PASS, 169.016s; `/tmp/flowy-task23-checkout-local.txt` |
| Release script rejection/recovery fixtures | PASS, 16 tests; `/tmp/flowy-task23-release-fixtures.txt` |

The process matrix reached all 18 named boundaries. Successful recovery retains
one external-service dispatch attempt and one actual effect; consumed gaps without
proof remain unknown with zero writes. The unit consumed-gap test verifies the
separate fenced-absence retry protocol; interrupted retry stages replay safely.
Counter regression independently proves rejected duplicate attempts are counted.
Read-only reconciliation verifies mandatory receipt fields and agreement of raw,
typed and delivery representations. Missing/null fields, deadline/cancellation,
unknown evidence, stale mappings and conflicting bindings never confirm success.

The current source consumer used an explicit host checkout. Portable CI fetches
the audited backend source commit implementing the declared named-config contract,
and published CI independently downloads the pinned artifact set without local
replacements and with GOWORK=off. An older backend default branch lacks that API;
a deliberate probe failed compilation instead of installing compatibility shims.
The example does not claim compatibility with that unsupported constructor.

Lint was run with the tool's supported allow-parallel-runners flag and isolated
cache to avoid an unrelated global runner lock; no linter checks were disabled.
No skipped backend test is counted as passing. Memory cases do not prove durable
process recovery. No physical disk loss, live external provider or distributed
exactly-once guarantee is claimed.

## Release decision

Select `make release-break`: the public read-only recovery-token contract is added
alongside a substantial supported approval/recovery composition and new semantic
acceptance gates. Root and existing adapters retain BYOT independence; the new
consumer is an optional executable, not a required core dependency. No compatibility
shim or silent approval/retry fallback is retained.

Publication, clean artifact installability, the exact published semantic consumer
and issue closeout are pending until their recorded commands succeed. The separate
review reports do not substitute for these results.
