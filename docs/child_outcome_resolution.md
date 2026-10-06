# Addressed child outcome resolution

`DurableRunner.ResolveChildOutcome(ctx, token, ChildOutcomeResolution)` accepts host evidence for a completed or failed remote child without dispatching work. The exact current parent token, original group node/activation/key, group compatibility label and exact child ID/execution ID/revision are mandatory. `DecisionID`, `Reason` and `Evidence` are nonempty valid UTF-8; payload bytes are opaque. Completed has no error; failed has a nonempty valid UTF-8 error; neither carries a wait ID. Runtime validates structure and identity; the host authenticates the operator and establishes the truth of the evidence.

The operation acquires a new live lease, then validates aggregate integrity, collections, descriptor, token, pointer and current group binding before accepting the decision. It performs no node, codec, dispatcher, merge or Resume calls. An incompatible descriptor is not migrated implicitly. Original group identity remains stable after cursor migration; the current `ChildGroupReferences` binding must point to it.

| Current child | Decision |
|---|---|
| unknown | Accept completed/failed with exact revision |
| running under an older incarnation | Normalize the abandoned outcome and resolve atomically; retain running prior state/incarnation |
| running under current/newer incarnation | Reject |
| planned, queued, waiting, canceled | Reject |
| already resolved by identical decision | Read-only replay, subject to exact current token and unjoined/current group |
| terminal without the same resolution | Reject |
| joined or no longer current group | Reject |

A cancellation request does not establish absence of a result and does not bar evidence-backed completion. Confirmed cancellation does. Child terminal state, payload, decision provenance and one increment of child revision commit atomically under OCC/fencing. Provenance includes the prior state/revision/incarnation, source parent revision, new lease incarnation, timestamp and canonical request digest. Input, allocation, immutable identity, siblings and existing cancellation requests remain unchanged. The journal validates the resolution against the terminal record and original group on every read. No automatic budget return occurs; a separate usage claim must address the new child revision.

| Replay request | Outcome |
|---|---|
| stale parent token, including after a lost commit acknowledgement | `ErrConcurrencyConflict`; inspect authoritative latest and resubmit explicitly |
| exact current parent token, same addressed decision and identical payload/reason/evidence | Return current token, no write or revision increment |
| same decision ID with changed content or reused for another child in this parent | `ErrChildRevision` |
| changed child revision or foreign group/child/contract | `ErrChildRevision` |
| malformed input | `ErrChildJoinInvalid` |
| malformed persisted provenance/collections | `ErrExecutionCorrupt` |

Decision IDs are unique within the child outcome resolution namespace of one parent, including retained groups. Concurrent completion, confirmation, wait resolution, migration and join have one committed winner; late callbacks cannot overwrite a resolved revision. A failed commit proves no success to the caller; a lost acknowledgement requires inspecting authoritative storage. Lease expiry is ownership evidence, never evidence that remote work did not occur. Unknown children are never relaunched by this API.

Adding optional resolution provenance leaves unresolved prior child journals meaningful. A missing provenance record does not claim manual resolution. New manually resolved records require complete validated provenance; partial records are rejected. Existing activity retry format transition is specified separately; no legacy schedule fallback is introduced by this boundary.

Opaque payloads of zero length use the same canonical representation, whether the caller supplies nil or an empty slice. Their identical replay digest is preserved through JSON persistence.
