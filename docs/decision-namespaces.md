# Child decision identity and replay

The parent execution stores addressed child decisions. Each operation has its own
namespace and token gate; a DecisionID is not a global UUID across all commands.
All identity, reason and evidence strings must contain valid UTF-8. Literal U+FFFD
is valid text. Malformed bytes are rejected before persistence or notification;
they are never repaired for comparison.

| Operation | Namespace and assertion | Duplicate behavior | Lost acknowledgement recovery |
|---|---|---|---|
| `CancelChildren` | One cancellation request per current group; request ID, reason and complete detached group must match | Exact request returns the stored group and may notify unresolved children again; changed request is rejected | Reload the current group, compare its CancelRequest, then redeliver the same notice if needed; notification acknowledgement does not settle a child |
| `ReturnChildBudget` | One return per child in the current group; child revision, decision ID, reason, evidence and Used must match | Exact claim is read-only and returns recorded Returned units; changed claim is rejected | Reload the current group and compare the stored BudgetReturns entry; replay cannot credit capacity twice |
| `ResolveChildWait` | Addressed child, wait ID and child revision under an exact current parent token | A settled child or changed revision is rejected, including the same decision ID; no read-only duplicate API | Load the current parent and inspect the child's WaitResolution and terminal result before issuing another command |
| `ConfirmChildCancellation` | Addressed child and cancellation request under an exact current parent token | A confirmed child or changed child revision is rejected; no read-only duplicate API | Inspect CancelConfirmation, request ID and child state in the current parent; never infer stopping from notification ACK |
| `ResolveChildOutcome` | Decision ID is unique across all stored child groups of the execution; addressed child and request digest must match | Exact recorded decision is read-only under a current parent token; ID reuse for another child or changed request is rejected | Inspect the recorded OutcomeResolution and request digest; acquire a current token explicitly before replay |

Current parent tokens are not advanced implicitly. Reading a newer checkpoint
does not authorize a different decision. A failed publication or lost ACK must be
resolved against persisted authority before another dispatch. Host authorization,
the truth of evidence and downstream idempotency remain host responsibilities.
