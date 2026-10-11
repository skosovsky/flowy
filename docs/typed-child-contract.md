# Typed child composition

Typed helpers adapt the existing persisted child protocol; they do not introduce a second runtime or change ordinary sequential execution. All parent, child input, child result and merged result types/codecs belong to the host. ChildGroupPlan.Label covers projection and input/result codec compatibility; MergeLabel covers the pure merge and its result codec. Changing a format without changing the corresponding label violates the host contract. No provider/domain schema is imported by core.

ProjectChildSpecs serializes the parent once, decodes a fresh parent for each child projection and serializes each projected input into detached bytes. Codecs must produce independent decoded objects. Projection may mutate its own parent copy, not the caller's parent or another child's input. Duplicate/empty IDs and invalid allocation reject before any projection. Projection/encoding failure returns no partial plan; it performs no persistence or dispatch.

TypedChildDispatcher decodes only the committed isolated child input, invokes the host worker and encodes its typed result. The callback receives stable runtime address/revision and its own typed input. Codec errors before the worker mean zero worker calls, but their dispatcher error
still commits unknown under the conservative core contract. No definitely-not-sent
classification or automatic redispatch is inferred. Encoding failure after a worker outcome remains an ambiguous dispatcher error; it must not imply that the external action failed or authorize blind retry. Runtime keeps its existing journal/fencing/unknown guarantees.

Dispatcher codecs are called concurrently under bounded child execution. Host implementations must support concurrent calls or provide their own synchronized adapter; mutable shared codec buffers/decoded objects must not leak between children.

DecodeChildOutcomes returns ordered typed results/errors/pending wait addresses, retaining state and an explicit HasResult flag rather than interpreting a zero value as success. Completed outcomes are decoded even when their codec's valid payload is empty. Other states without payload have no result; partial payloads may belong to failed/canceled/unknown outcomes and their state remains explicit. It invokes no worker.

JoinTypedChildren runs typed decoding and pure merge within the existing once-only join boundary, then encodes the merged result for the same aggregate commit. Committed replay decodes the cached merge bytes and invokes no merge or child worker. Decode/merge/encode failure retains committed child outcomes. Host state application belongs to the declared replay-safe parent step, not to an external effect inside merge.


A merged-codec Unmarshal error can occur **after the join commit succeeded**. This
is different from decoding child outcomes before merge or encoding before commit.
Load the latest validated execution, inspect the original bound group's MergedIDs
and MergedResult, and decode a detached copy with the declared merge codec after
repairing availability. While the node activation is still executing and handles the decode error,
PrepareChildren with the
same bound plan/capacity returns the current exact group assertion; JoinChildren
with that group returns cached bytes without invoking its required merge callback.
JoinTypedChildren also decodes those cached bytes without rerunning the merge.
Do not use the pre-join group assertion, change MergeLabel to bypass compatibility,
redispatch children, or perform an external effect in the merge callback. The
committed bytes remain authority even when the parent returned ErrChildCodec.
TestTypedJoinDecodeFailureAfterCommitReplaysWithoutMerge exercises inspection,
redecode and immutable terminal-failure replay with one dispatch and one merge.
An unhandled codec error may commit a terminal failure: Resume then reproduces
that failure as PersistedExecutionError/ErrExecutionFailed, not a new node invocation.
Original codec/host error identities are live-only and are not inferred from stored text. TestTypedJoinDecodeHandledInActivationReadsCachedJoin
proves active-node recovery with PrepareChildren and raw cached join. Do not use
Suspend to retry the same logical work implicitly: selecting a different activation
is a separate execution boundary and may create new child identities.

Completed with zero bytes is a valid host codec representation and HasResult is
true; zero-byte decode must still run. Other states without bytes have no typed
result. Nonempty partial payloads can be decoded from failed/canceled/unknown
records, with HasResult true but their original State/Error retained. This does
not convert them to successful children. The flat persisted DTO and centralized
validation remain; no separate presence bit is added to wire format.


The Task28 memory-copy fixture benchmark on Go1.27.1 darwin/arm64 (Apple M1 Max,
100ms calibration, one run) measured detached copying at 3,257ns/op, 3,200B/op,
23 allocations versus JSON roundtrip at 29,103ns/op, 5,771B/op, 50 allocations.
Both JSON-representable fixtures retain one child's nested maps/bytes/provenance. These are local
allocation/copy measurements, not backend latency or a stable regression baseline.
Ownership/domain tests additionally prove values outside JSON's domain survive a
memory copy unchanged; admission rejects those values separately.


ErrChildInvalid rejects invalid child/group/typed-composition contract input;
it is broader than a join error. ErrChildRevision rejects stale child assertions,
ErrChildMergeConflict reports merge/publication conflicts, and ErrChildrenUnresolved
requires resolution of outstanding work. ErrChildJoinInvalid is removed in this
clean break, with no compatibility alias. Existing stored terminal messages remain
historical text and replay through ErrExecutionFailed.
