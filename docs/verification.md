# Repository verification

Make and Bash release follow ragy commit
`91e3ff2cdc87c49b5ad9d1cf0afa23d35832521e`. Make discovers every `go.mod`, excluding
hidden directories and vendor. There is no publication inventory, project hook,
aggregate runner or generated configuration. All targets use `GOWORK=off`.

| Command | Scope |
|---|---|
| `make modules` | Discovered modules: root and four checkpointer/lease adapters. |
| `make lint` | Validate config, show formatting differences, lint every module. |
| `make test` | Fresh ordinary tests with race detector, including existing goleak checks. |
| `make test-integration` | `integration` build tag and `TestIntegration` prefix. |
| `make test-e2e` | `e2e` build tag and `TestE2E` prefix; runnable example smoke scenarios. |
| `make test-live` | `live` build tag and `TestLive` prefix; currently no live-provider tests. |
| `make fix` | Go fixes, formatting and automatic lint fixes; edits files. |
| `make bench` / `make cover` | Benchmarks with allocations / per-module coverage. |
| `make fuzz` | Each discovered fuzz target separately for 30 seconds. |
| `make verify-stress` | Separate 20-repeat race campaign for handoff/resume/orphan contracts. |

CI and the source release gate execute lint, unit, integration and e2e sequentially.
Benchmarks, fuzz campaigns, stress and paid provider calls are outside that gate.
The former `test-race` and `test-goleak` entrypoints are replaced by `make test`.
Historical task22 reports retain the commands and test names used at recording time.

## Prerequisites

Use Go 1.27.2, golangci-lint 2.14.0, Git, Make, Bash and a C compiler for `-race`.
CI pins the tools; Make uses PATH without a separate version-check target.
`actionlint .github/workflows/ci.yml` validates the workflow independently.

Integration requires a working Docker CLI and daemon. Backend fixtures create
isolated PostgreSQL 16 / Redis 7 containers using image digests pinned in
`internal/testdocker`, random published ports, bounded readiness checks and cleanup. Backend data uses
bounded tmpfs mounts, retained across connection restarts for the fixture lifetime;
cleanup also removes any anonymous volumes.
Fixtures explicitly select the daemon platform, independent of a global
`DOCKER_DEFAULT_PLATFORM` intended for application builds.
No `FLOWY_TEST_DATABASE_URL` or `FLOWY_TEST_REDIS_ADDR` setup is needed. Restart
scenarios reconnect to the original fixture, preserving its state. Missing Docker,
failed startup and unavailable ports are failures, not skipped tests. Ordinary
unit tests retain their in-memory fakes/miniredis and do not require Docker.

For a TCP Docker daemon, `DOCKER_HOST` supplies the host for published backend
ports. With a Unix socket the tests connect through loopback; a Linux test container
must share the daemon host's network namespace so those ports are reachable.
No PDF runtime, Python or RAGY variables are involved.

## Profile contracts

Tags select source files; name prefixes select tests. Both are required. For example:

```sh
GOWORK=off go test -race -count=1 -tags=integration -run '^TestIntegration' ./...
GOWORK=off go test -race -count=1 -tags=e2e -run '^TestE2E' ./...
```

A module without tests for a profile may report no tests. The repository as a whole
must execute its backend integration, release integration and example e2e scenarios.
The paired lease/checkpointer checks belong to integration even though older names
used `TestE2E`. Tagged code should also be checked with `golangci-lint run
--build-tags=integration` (and `e2e` for the root).

Release tests in `scripts/` use temporary bare repositories and the real Bash
release script. Fixture Make gates are intentionally small to avoid recursive
release tests. They cover source/candidate separation, atomic publication,
recovery, failed gates, collisions and preservation of the caller checkout.
See the [release runbook](release/runbook.md).
