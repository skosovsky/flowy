# Typed child composition

Typed helpers adapt the existing persisted child protocol; they do not introduce a second runtime or change ordinary sequential execution. All parent, child input, child result and merged result types/codecs belong to the host. ChildGroupPlan.Label covers projection and input/result codec compatibility; MergeLabel covers the pure merge and its result codec. Changing a format without changing the corresponding label violates the host contract. No provider/domain schema is imported by core.

ProjectChildSpecs serializes the parent once, decodes a fresh parent for each child projection and serializes each projected input into detached bytes. Codecs must produce independent decoded objects. Projection may mutate its own parent copy, not the caller's parent or another child's input. Duplicate/empty IDs and invalid allocation reject before any projection. Projection/encoding failure returns no partial plan; it performs no persistence or dispatch.

TypedChildDispatcher decodes only the committed isolated child input, invokes the host worker and encodes its typed result. The callback receives stable runtime address/revision and its own typed input. Codec errors before the worker mean zero worker calls. Encoding failure after a worker outcome remains an ambiguous dispatcher error; it must not imply that the external action failed or authorize blind retry. Runtime keeps its existing journal/fencing/unknown guarantees.

Dispatcher codecs are called concurrently under bounded child execution. Host implementations must support concurrent calls or provide their own synchronized adapter; mutable shared codec buffers/decoded objects must not leak between children.

DecodeChildOutcomes returns ordered typed results/errors/pending wait addresses, retaining state and an explicit HasResult flag rather than interpreting a zero value as success. Completed outcomes are decoded even when their codec's valid payload is empty. Other states without payload have no result; partial payloads may belong to failed/canceled/unknown outcomes and their state remains explicit. It invokes no worker.

JoinTypedChildren runs typed decoding and pure merge within the existing once-only join boundary, then encodes the merged result for the same aggregate commit. Committed replay decodes the cached merge bytes and invokes no merge or child worker. Decode/merge/encode failure retains committed child outcomes. Host state application belongs to the declared replay-safe parent step, not to an external effect inside merge.
