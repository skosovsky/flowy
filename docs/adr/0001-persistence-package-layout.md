# ADR 0001: Persistence package layout (`checkpoint/` vs `adapters/checkpointer`)

## Status

Accepted

## Context

An early task spec placed persistence DTOs and storage interfaces under `adapters/checkpointer`. The repository instead ships a dedicated subpackage `github.com/skosovsky/flowy/checkpoint` for types and serializers, with concrete backends (filesystem, DB, etc.) living under `adapters/` or user code.

## Decision

- Keep **all persistence types and codecs** in the `checkpoint` subpackage so they are `go doc`-discoverable and importable without coupling the root `flowy` package to storage.
- Keep **`package flowy` free of imports** from `checkpoint` or any adapter — execution stays stateless; callers wire `Stream` + save/load.
- Optional adapters remain **adapters** in the filesystem sense (pluggable backends), not the only home for shared DTOs.

## Consequences

- Positive: Single module, clear import path, matches common Go layout (library + subpackage for optional concerns).
- Neutral: Wording in older docs may say “adapters only”; README and this ADR document the actual layout.
