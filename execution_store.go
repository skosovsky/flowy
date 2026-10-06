package flowy

import (
	"context"
	"time"
)

// ExecutionLease identifies one acquisition, even when an owner name is reused.
// Incarnation is monotonically increasing and must not be reset by expiry,
// release, or checkpoint retention. Only storage decides whether it is live.
type ExecutionLease struct {
	ExecutionID string    `json:"execution_id"`
	Owner       string    `json:"owner"`
	Incarnation uint64    `json:"incarnation"`
	ExpiresAt   time.Time `json:"expires_at"`
}

// ExecutionStore persists raw envelopes without selecting a domain codec.
// Commit must atomically validate expectedRevision and the live lease
// incarnation and store the entire aggregate. It returns a detached, committed
// envelope. Losing ownership returns ErrLeaseLost; revision conflict returns
// ErrConcurrencyConflict. A commit cannot mutate any historical revision.
// Fork creation lineage cannot be added, removed or changed after revision one;
// validate it atomically with the predecessor and return ErrExecutionCorrupt.
// Retain an independent creation anchor; loads and later commits must reject
// inconsistent lineage even when the envelope has a recomputed valid digest.
//
// Acquire rejects an active lease even with the same owner. Calls
// using a stale incarnation may not mutate a successor's lease or checkpoint.
type ExecutionStore interface {
	LoadExecution(ctx context.Context, executionID string) (ExecutionEnvelope, error)
	CommitExecution(
		ctx context.Context,
		expectedRevision uint64,
		lease ExecutionLease,
		envelope ExecutionEnvelope,
	) (ExecutionEnvelope, error)
	AcquireExecution(ctx context.Context, executionID, owner string, ttl time.Duration) (ExecutionLease, error)
	RenewExecution(ctx context.Context, lease ExecutionLease, ttl time.Duration) (ExecutionLease, error)
	ReleaseExecution(ctx context.Context, lease ExecutionLease) error
}

// ExecutionHistoryStore is optional exact historical loading. Missing or pruned
// revisions return ErrThreadNotFound; zero revisions are invalid, never latest.
// Retained envelopes must be immutable and self-contained with their blobs.
type ExecutionHistoryStore interface {
	LoadCheckpoint(ctx context.Context, executionID string, revision uint64) (ExecutionEnvelope, error)
}
