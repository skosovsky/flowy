# Storage admission and limits

Standalone snapshots require nonempty UTF-8 ThreadID and ExecutionPointer and a
positive stored revision. Save derives the next revision from expectedRevision;
the revision supplied inside a snapshot is not an OCC authority. The shared
ValidateSnapshotHeader checks only structural metadata and does not call a host
decoder. Invalid metadata is rejected before cloning/encoding, writes, TTL changes
or transactional outbox callbacks. Revisions never wrap.

StateSerializer.Marshal must return a JSON value. BYOT applies to the typed state
and effects, not arbitrary wire bytes. EncodeRecord rejects malformed JSON and
does not call Unmarshal to validate it. PostgreSQL stores the record as JSONB,
which additionally rejects string U+0000, invalid Unicode surrogate escapes and
numbers outside PostgreSQL numeric range. These backend errors roll back the
transaction, including head/history and outbox writes. Redis stores JSON bytes;
its wire domain does not acquire PostgreSQL's extra JSONB restrictions.

Mandatory constructor dependencies reject both nil interfaces and typed nils.
PostgreSQL NewCheckpointer, NewExecutionStore and NewLeaseManager and
checkpoint.WithSanitizer return errors; callers must handle these errors before
use. Optional clocks may use their documented default. Configuration errors are
ErrConfiguration in Redis adapters and ErrExecutionCapability in PostgreSQL/core
constructors; neither means the collaborator was installed successfully.

| Historical address | Error |
|---|---|
| Revision zero | ErrInvalidSnapshot |
| Absent identity or revision beyond its published head | ErrThreadNotFound |
| Known revision whose payload was pruned/deleted | ErrExecutionCheckpointUnavailable |
| Malformed payload or inconsistent address/digest | Existing envelope/integrity error; never absence |
| PostgreSQL revision beyond signed 64-bit range | ErrExecutionCapability before SQL conversion |

Positive fractional lease durations round upward to native precision: Redis
milliseconds and PostgreSQL microseconds. Memory helpers retain the exact Go
duration. Snapshot retention TTL is a separate ceil-millisecond policy in Redis.

PostgreSQL revisions and fences and Redis lease INCR fences use positive signed
64-bit counters. Exhaustion returns ErrExecutionCapability without installing a
new lease, resetting the counter or publishing a new revision. Memory counters
and Redis snapshot revision decimal strings have an unsigned 64-bit domain;
their own maximum also rejects advancement. Lease tokens outside PostgreSQL's
signed range are rejected as ErrLeaseLost. These capacity limits are operational
boundaries and are not evidence of an ordinary near-term failure.

CancelWait terminates the entire execution with a failure record. A wait Deadline
must be nonzero, have UTC offset zero and be representable by time.Time JSON
(year 0 through 9999). LifecycleWaitDelivery / `wait_delivery` observes every
delivery attempt; Stage and Code distinguish accepted, unmatched, lost and failed
attempts. The name alone does not assert a winner.
