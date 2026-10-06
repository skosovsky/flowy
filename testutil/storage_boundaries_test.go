package testutil

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/skosovsky/flowy"
)

func TestMemoryHistoricalAddressErrorsAndCounterExhaustion(t *testing.T) {
	t.Parallel()
	// Arrange: fixed time makes the native unsigned fence limit deterministic.
	now := time.Date(2026, 10, 6, 0, 0, 0, 0, time.UTC)
	store := NewMemoryExecutionStore(func() time.Time { return now })
	store.fences["exhausted"] = ^uint64(0)
	// Act.
	_, zeroErr := store.LoadCheckpoint(context.Background(), "missing", 0)
	_, missingErr := store.LoadCheckpoint(context.Background(), "missing", 1)
	_, exhaustionErr := store.AcquireExecution(context.Background(), "exhausted", "worker", time.Minute)
	_, revisionErr := store.CommitExecution(context.Background(), ^uint64(0), flowy.ExecutionLease{},
		flowy.ExecutionEnvelope{})
	// Assert.
	if !errors.Is(zeroErr, flowy.ErrInvalidSnapshot) || !errors.Is(missingErr, flowy.ErrThreadNotFound) ||
		!errors.Is(exhaustionErr, flowy.ErrExecutionCapability) ||
		!errors.Is(revisionErr, flowy.ErrExecutionCapability) ||
		store.fences["exhausted"] != ^uint64(0) ||
		len(store.leases) != 0 {
		t.Fatalf("zero=%v missing=%v exhaustion=%v leases=%v", zeroErr, missingErr, exhaustionErr, store.leases)
	}
	lease, err := store.AcquireExecution(context.Background(), "fraction", "worker", time.Nanosecond)
	if err != nil || !lease.ExpiresAt.Equal(now.Add(time.Nanosecond)) {
		t.Fatalf("lease=%+v err=%v", lease, err)
	}
}

func TestMemoryHistoricalMissingPayloadIsNotAbsence(t *testing.T) {
	t.Parallel()
	// Arrange: retain the published head while payload retention removes history.
	store := NewMemoryExecutionStore(nil)
	store.heads["retained"] = 2
	store.history["retained"] = make(map[uint64][]byte)
	for _, revision := range []uint64{1, 2} {
		// Act.
		_, err := store.LoadCheckpoint(context.Background(), "retained", revision)
		// Assert.
		if !errors.Is(err, flowy.ErrExecutionCheckpointUnavailable) {
			t.Fatalf("known revision %d: %v", revision, err)
		}
	}
	_, futureErr := store.LoadCheckpoint(context.Background(), "retained", 3)
	_, latestErr := store.LoadExecution(context.Background(), "retained")
	if !errors.Is(futureErr, flowy.ErrThreadNotFound) ||
		!errors.Is(latestErr, flowy.ErrExecutionCheckpointUnavailable) {
		t.Fatalf("future=%v latest=%v", futureErr, latestErr)
	}
}
