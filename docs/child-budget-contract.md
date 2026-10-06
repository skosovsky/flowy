# Structured child allocation and return contract

Named counters are non-monetary host units. Core never interprets provider prices, invoices, tokens or external balances. A monetary reservation/release is a host-owned idempotent port invoked through the durable activity boundary; its evidence and opaque receipts belong to the host, not to a mandatory shared financial schema.

The parent provides a fixed available named-counter capacity for one activation. Group allocations are detached and fixed before admission. All groups in that activation share the capacity: group-by-group validation alone must not allow oversubscription. A newly requested capacity inconsistent with the persisted one is a typed conflict. Negative/unknown counters and arithmetic overflow reject before dispatch.

A return claim addresses parent/group/child and the exact child revision, with stable decision ID, reason/evidence and explicit host-reported used units. Only completed, failed or confirmed-canceled children are settled; unknown/running/waiting retain their entire allocation. Host evidence defines actual consumption, not the runtime result classification. Completion does not imply zero consumption; cancellation does not imply rollback or unused external reservation.

Used units must explicitly name every allocated counter (including zero consumption), be nonnegative, name no other counters and not exceed the child's allocation. Missing usage is not implicitly zero. Returned units are exactly allocation minus used. Core validates arithmetic and addresses, not truth of host usage. ComputeChildBudgetReturn is a pure calculation yielding a detached map; it never
releases capacity or persists a claim. ReturnChildBudget is the publication boundary.

ReturnChildBudget stores claim, used/returned units, source child/aggregate revisions, fence and timestamp together. Each child allocation is returned at most once. A committed repeated claim against the current detached group replays the recorded result; a stale group or different claim for an already returned child is rejected. A failed commit gives no return acknowledgement and leaves capacity reserved. Parent/group OCC and the live fence govern publication. Step/join/cancellation alone do not create a return.

Across crash/restart, reservations and return decisions remain aggregate source-of-truth. Outstanding unknown/non-cooperative work retains allocations until explicit reconciliation/confirmation and a valid return claim. Returned capacity can fund another group within the same activation; consumed units remain accounted for. Host allocation for a new activation must come from its remaining run budget rather than a fabricated replenishment.

Capacity is retained in each group's sealed record and checked for equality across the activation. The aggregate ledger subtracts each child's allocation minus its committed return; consumed units remain accounted for. Subtraction from bounded remaining units avoids summation overflow. Runtime validation rejects malformed return provenance/arithmetic and oversubscribed aggregate records before domain decoding. No mandatory monetary model or external ledger is added to core.

Acceptance covers multiple-group oversubscription rejection, unknown-work retention, exact partial return and replay, stale/duplicate/mismatched claims, commit faults, independent persistent-backend recovery and host monetary-port recovery through activities. TestChildBudgetLedgerPartialReturnReplayAndCommitFault, TestChildBudgetReturnUnknownRetainsAllocation and TestChildBudgetReturnRejectsStaleAndChangedClaims verify runtime return; TestChildBudgetPersistentPartialReturnReplayAcrossPoolRestart verifies independent-pool recovery. TestChildHostReservationPortRecoversWithoutRepeatingExternalEffects verifies a host-owned monetary port through activities without a core financial schema. These are distinct pieces of evidence: a budget arithmetic test alone does not prove external reservation recovery. Final aggregate acceptance and current gate results are tracked in task22-progress.md and the fixed requirement registry.


Dispatcher contexts mask parent activity/group/lease capabilities but retain parent
run metadata for context propagation. UseBudget on that context mutates parent
counters and requires serialized access within the activation; concurrent child
workers must establish their own run metadata/accounting or coordinate explicitly.
Allocation is activation capacity, a used claim is host consumption evidence, and
returned units are allocation minus used. None is a price, balance, prepayment or
external release receipt. Global concurrency/quota and noncooperative job termination
remain host duties, beyond a single group's MaxConcurrency.
