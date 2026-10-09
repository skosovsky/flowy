# Shell release

The common release implementation uses Bash, Git, Make and the standard Go CLI.
There is no Python release runner or additional Go executable.

```sh
make lint
make test
make test-integration
make test-e2e
make release-patch
make release-break
# Optional older source on main:
make release-patch RELEASE_SOURCE=<full-commit-sha>
make release-inspect
make release-resume
make release-finish
```

The caller must be on local `main` with a clean tracked/index state. Untracked
files are preserved and excluded. Source defaults to HEAD; an explicit full SHA
must be an ancestor of local main. Origin must have exactly one push destination
and an existing main that can fast-forward to the selected source. An older
source cannot rewind remote main. The caller's branch, HEAD, files and tags remain
unchanged throughout the transaction.

Versions come from remote root tags. Patch increments patch; break increments
minor before v1. A breaking transition from v1 to v2 is blocked until an import-path
migration is implemented. The selected source's `make modules` discovers
all modules without a separate publication inventory. Root tags
use `vX.Y.Z`; nested module tags use their directory prefix.

The release runs the selected source's lint, fresh unit tests,
integration tests and e2e tests sequentially in an independent checkout.
It updates internal requirements to the new version and removes internal development
replacements only there. Only go.mod/go.sum files may change in the release commit.
A failed prerequisite, source gate, preparation or candidate identity check prevents push.

After displaying the source, candidate, destination and exact refs for confirmation,
one atomic push publishes:

- source SHA to `refs/heads/main`, keeping its development replacements;
- candidate SHA to the exact root/module release tags, with prepared manifests.

There is no force, broad `--tags`, or non-atomic fallback. Remote main is rechecked
before push; a concurrent incompatible update rejects publication. Remote tags must
match exact candidate identities. Remote main must equal source or contain it in
its history. Publication completes after these remote-ref checks; no consumer
installation or proxy polling runs after push. Artifact and consumer checks remain
ordinary Go integration/e2e tests in the source gate.

## Recovery

Format-3 plain-text records live under `library-releases/active` in the Git common
directory. They retain destination, branch, observed remote main, source, candidate,
version, phase and publication status. Records are never evaluated as shell code.
A local lock prevents concurrent releases; after process termination, verify the
process is gone before manually removing a stale lock.

`inspect` refreshes remote observations. `resume` reuses the same version and
candidate; unknown results require inspection first. A lost response after push is
resolved by reading refs. Later forward movement of remote main is acceptable if
source ancestry is verified; a different tag is always a collision. Recovery never
rewinds main or overwrites/deletes a tag. `finish` archives only a verified release.

Historical records remain untouched. An active record under an older release state
directory, or an unsupported record format, blocks new publication: finish/recover
it using its original tooling revision before using the new script.

## Verification

Go integration tests in `scripts/` exercise the common protocol against disposable bare repositories,
including libraries with nested modules outside `adapters/`. The fixtures verify exact source/candidate refs and prepared manifests; they do not
claim Go proxy installability. Runnable consumer examples are checked by `make test-e2e`.
These checks never publish to the production remote. CI runs the same lint/unit/integration/e2e commands.

## flowy prerequisites

Run from a clean local main checkout with Bash, Git, Make, Go 1.27.2,
golangci-lint 2.14.0, a C compiler for the race detector, and a working Docker CLI
and daemon. Integration tests start disposable PostgreSQL and Redis containers;
no database URL, API key, Python or PDF runtime is needed. Missing prerequisites
fail the gate. The daemon must support the pinned images and published TCP ports.

The implementation follows ragy commit
`91e3ff2cdc87c49b5ad9d1cf0afa23d35832521e`. No production release is part of the
infrastructure migration. Recovery tests publish only to temporary local bare
repositories. See [verification](../verification.md) for the current commands;
older task reports record historical evidence, not current gate results.
