# Release contract and runbook

## Preparation and publication

Run from the repository root with Python 3.9+ (Git and the module-required Go
version must be installed): `make release-patch` or `make release-break`.
The Make targets run lint/tests first; the script itself does not replace those
checks. MODULES must contain every tracked module exactly once, including
`examples/durable_agent` and the optional `examples/approval_recovery` executable. Tracked worktree/index changes reject preparation;
untracked files and unrelated local tags are preserved and never published.

The script reads the push destination of origin and published root semver tags.
It prepares the exact source HEAD in an isolated clone. Only tracked `go.mod`
files are staged: internal requirements move to the new version, internal local
replaces are removed, and Go formats the files. Source branch, HEAD, index,
worktree, local tags and unrelated remote tags remain untouched. No branch is
pushed. Root `vX.Y.Z` and each module-directory `vX.Y.Z` tag form the exact ref
allowlist; an atomic push publishes them together. An origin without atomic push
support rejects the release; there is no non-atomic fallback.

Patch increments patch; break increments minor while v0, then major after v1.
Versions v2+ are explicitly rejected until semantic import version paths and
consumers are migrated. The module editor uses Python and Go, with no BSD sed
dependency. Release preparation must pass the seven-module clean consumer gate
before real publication: build consumers against prepared module artifacts in a
local Go module proxy, with `GOWORK=off`, no local replace directives and fresh
module caches. The final remediation verification records this evidence; local
Git fixtures alone do not prove real package installability.

For preparation before consumer verification, pass `--prepare-only` to
`scripts/release.sh patch "<all tracked module directories>"`. This writes the
manifest and clone without pushing. Verify those artifacts, then use the resume
command below to publish that exact commit. Direct make release targets require
the caller to have already verified the candidate's release dependency graph.

## Failure and exact recovery

Before push, preparation writes `manifest.json` in the printed attempt directory.
It records destination, version, commit and every release ref; its sibling `repo`
is the prepared clone. Preserve both until recovery/verification is complete.
Before manifest creation, a preparation failure has no publication effects: the
source checkout is untouched and the incomplete directory can be removed.
After manifest creation, retry that exact attempt:

```sh
python3 scripts/release.py --resume /absolute/path/to/flowy-release-attempt
```

Resume checks the prepared clone and refs against the manifest, then queries the
remote. Publication results:

| State | Meaning and action |
|---|---|
| none | No recorded ref exists; resume the same version/commit |
| partial | Some recorded refs exist at the expected commit; atomic resume adds the rest |
| unknown | Remote cannot be queried; retain the attempt and restore connectivity before resuming |
| conflicting | A recorded ref points elsewhere; stop and inspect ownership, never force overwrite |
| complete | Every recorded ref matches; repeat is read-only, including recovery after a lost push ACK |

A failed push is followed by a fresh query. Exit success requires complete remote
publication; a push error alone does not prove that no ref was published. No refs
are deleted based on a network error. Do not prepare a new release while an attempt
is unresolved: resume its recorded version. Keep the directory off public storage;
it includes the repository clone and the origin destination. After complete
verification or confirmed abandonment, remove the attempt directory manually.

## Local verification

`python3 -m unittest discover -s scripts -p test_release.py` creates only disposable
local bare origins. It checks exact scope, source preservation, atomic rejection,
failures before/during/after tagging, partial/unknown/conflicting outcomes, exact
retry, portable module edits and v2 rejection. It does not contact the real origin
or execute a real release.


The approval example is published as an optional executable module, never imported
by core or adapters. Its semantic published-consumer gate must pass after release:
`python3 scripts/check_approval_consumer.py --mode published --version <released-tag>`.
Run the checkout gate before publication. A tag/build alone does not prove its
approval, binding, capture or process-recovery semantics.
