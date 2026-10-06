# PostgreSQL durable agent blueprint

This is a separate example module. Its model/tool ports, parent state, child
input/result and effects belong to the host. It uses deterministic fake services;
there are no model credentials, provider SDK calls or implicit core scheduler.

```sh
cd examples/durable_agent
export FLOWY_TEST_DATABASE_URL='postgres://user:password@localhost:5432/flowy_test?sslmode=disable'
go run .
go test -race -tags=integration ./...
```

The database role needs CREATE/DROP SCHEMA privileges. Every run creates a random
schema and drops only that schema after closing all six worker pools. The
integration command fails if its database URL is absent. Ordinary module tests
do not stand in for this backend check; `../durable_runtime` is a separate memory
smoke example.

The scenario loses a tool-outcome reply **after** PostgreSQL commits it, resumes
on a fresh pool without redispatch, then awaits approval. The bounded host
component reads one indexed discovery page and drains authenticated input before
the due timer. The first committed decision wins: duplicate approval replays,
and the timer loses. This policy accepts late input; a host needing a different
deadline policy must implement that policy explicitly.

Two children then run under separate durable execution identities and write
their own completed heads. A barrier lets both finish before the host rejects a
parent outcome publication. Recovery exposes unresolved ownership; the host
reads sealed child terminals and supplies addressed `ResolveChildOutcome`
decisions. It does not relaunch the children. The final join orders children by
ID, returns their allocation usage and produces `[4, 6]`. A final new worker
replays the exact final token.

The external fake survives replacement workers and counts dispatches separately
from applied writes. Its idempotency map is a downstream contract, not a runtime
promise. Core does not authenticate approvals/operators or prove usage/evidence.
`UseBudget` records measured units at an acknowledged step; it reserves no money
or external capacity. Unknown children keep their allocation until justified
resolution. The fake model produces the initial plan before Start; this example
does not claim durable model-invocation accounting.

The integration test installs the OTel adapter with a test span recorder and
proves trace correlation through fresh contexts/pools, activity replay, wait
delivery, child resolution/join and terminal replay. The executable has no
exporter by default; the host owns provider/exporter setup and shutdown. Payload,
arbitrary failure text and operator evidence are absent from default observations.
Graph steps, two-child concurrency, one discovery page and explicit deliveries
bound this demonstration. It is a recovery blueprint, not a deployment platform.

See [the scenario contract](../../docs/durable-agent-blueprint-contract.md) and
[runtime observations](../../docs/runtime-observation-contract.md).
