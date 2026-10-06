# Task22 Spec-First chronology evidence

This document records retrieved native chat events, not file modification times or a retrospectively rewritten Git history. Thread: `01a102ac-0d74-7ac1-8269-697af1030b69`.

The native `read_thread` history contains completed turn `01a10674-c653-7741-a13b-ec0d4e5cf17f`, started `2026-10-04T10:27:58Z`, completed `2026-10-04T10:52:23Z`. Item indices below are zero-based positions in that turn's returned `items` array. They establish event order; individual file-change timestamps were not returned.

| Order | Completed file-change event | Evidence |
|---|---|---|
| 7 | `exec-5f2a6d2b-cd04-4398-b815-8ca15f73835f` | Added `docs/runtime-contract.md`, `docs/task22-requirements.json`, `docs/task22-requirements.md` together. |
| 11 | `exec-244e3a50-75f4-4421-8d4e-ea652a167469` | First production changes to Redis lease and PostgreSQL lease/delete SQL in this implementation turn. |
| 12 | `exec-1accd097-ed14-44ee-a3c5-3f5de3ef573c` | PostgreSQL checkpointer/lease implementation updates. |
| 43 | `exec-27e661a6-f40a-449b-9fb0-844fa5c82753` | Updated runtime contract before subsequent durable implementation. |
| 58 | `exec-15718d4e-ae6d-4f4f-9ee4-8ee22d81a3a0` | Added `execution_contract.go` and its tests. |
| 61 | `exec-6f4a108a-0c1d-413f-b9c8-d2845a5da14f` | Added execution store interface and memory conformance store/tests. |
| 100 | `exec-47d99f19-c485-4bd8-953a-f5fde93be0ed` | Added PostgreSQL execution store. |

The next implementation turn `01a1068b-211a-7930-8973-a8403098a5be` starts after that contract/registry turn. Its completed events `exec-c74f77af-f467-475a-bc78-9fc89258d20d` and `exec-8ef8094b-3de6-4477-bf6a-86d45b517820` add `durable_runner.go` and `activity.go`, respectively.

Retrieval used native `read_thread`, ten turns per page, traversing genuine returned cursors to the beginning. A reproducible earlier-page starting cursor is:

```json
{"requestedThreadId":"01a102ac-0d74-7ac1-8269-697af1030b69","rolloutOrdinal":8467,"includeAnchor":false,"scope":{"kind":"turns"}}
```

Follow `page.nextCursor` until the specified turns appear; do not guess rollout ordinals. This proves that the contract and numbered registry files were created before the production changes listed above. The returned file-change records identify paths and ordering, but do not reproduce each original patch's complete contents. It is not evidence that every later refinement was anticipated initially, nor a replacement for inspecting current contracts and independently verifying their adequacy. Auditors decide the requirement's status from the evidence; this note assigns no percentage.
