package testutil

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"time"

	"github.com/skosovsky/flowy"
)

// MemoryExecutionStore is a detached-copy conformance store, not crash-durable
// persistence. A caller-supplied clock permits deterministic lease expiry tests.
type MemoryExecutionStore struct {
	mu      sync.Mutex
	now     func() time.Time
	history map[string][][]byte
	leases  map[string]flowy.ExecutionLease
	fences  map[string]uint64
	lineage map[string]flowy.ForkLineage
}

// NewMemoryExecutionStore creates an empty execution store. Nil clock means now.
func NewMemoryExecutionStore(clock func() time.Time) *MemoryExecutionStore {
	if clock == nil {
		clock = time.Now
	}
	return &MemoryExecutionStore{
		mu:      sync.Mutex{},
		now:     clock,
		history: make(map[string][][]byte),
		leases:  make(map[string]flowy.ExecutionLease),
		fences:  make(map[string]uint64),
		lineage: make(map[string]flowy.ForkLineage),
	}
}

func (s *MemoryExecutionStore) LoadExecution(ctx context.Context, executionID string) (flowy.ExecutionEnvelope, error) {
	if err := ctx.Err(); err != nil {
		return flowy.ExecutionEnvelope{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	items := s.history[executionID]
	if len(items) == 0 {
		return flowy.ExecutionEnvelope{}, flowy.ErrThreadNotFound
	}
	return s.decodeAnchoredExecution(items[len(items)-1], executionID, uint64(len(items)))
}

func (s *MemoryExecutionStore) LoadCheckpoint(
	ctx context.Context,
	executionID string,
	revision uint64,
) (flowy.ExecutionEnvelope, error) {
	if err := ctx.Err(); err != nil {
		return flowy.ExecutionEnvelope{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	items := s.history[executionID]
	if revision == 0 || revision > uint64(len(items)) {
		return flowy.ExecutionEnvelope{}, flowy.ErrThreadNotFound
	}
	return s.decodeAnchoredExecution(items[revision-1], executionID, revision)
}

func (s *MemoryExecutionStore) CommitExecution(
	ctx context.Context,
	expectedRevision uint64,
	lease flowy.ExecutionLease,
	envelope flowy.ExecutionEnvelope,
) (flowy.ExecutionEnvelope, error) {
	if err := ctx.Err(); err != nil {
		return flowy.ExecutionEnvelope{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.held(lease) || envelope.ExecutionID != lease.ExecutionID {
		return flowy.ExecutionEnvelope{}, flowy.ErrLeaseLost
	}
	if err := envelope.Descriptor.Validate(); err != nil {
		return flowy.ExecutionEnvelope{}, err
	}
	if envelope.Progress.ExecutionPointer == "" {
		return flowy.ExecutionEnvelope{}, flowy.ErrInvalidSnapshot
	}
	items := s.history[envelope.ExecutionID]
	if uint64(len(items)) != expectedRevision {
		return flowy.ExecutionEnvelope{}, flowy.ErrConcurrencyConflict
	}
	var previous *flowy.ExecutionEnvelope
	if len(items) > 0 {
		stored, err := s.decodeAnchoredExecution(items[len(items)-1], envelope.ExecutionID, expectedRevision)
		if err != nil {
			return flowy.ExecutionEnvelope{}, err
		}
		previous = &stored
	}
	if err := flowy.ValidateExecutionForkTransition(previous, envelope); err != nil {
		return flowy.ExecutionEnvelope{}, err
	}
	envelope.Revision = expectedRevision + 1
	var sealErr error
	envelope, sealErr = flowy.SealExecutionEnvelope(envelope)
	if sealErr != nil {
		return flowy.ExecutionEnvelope{}, sealErr
	}
	encoded, err := json.Marshal(envelope)
	if err != nil {
		return flowy.ExecutionEnvelope{}, err
	}
	result, err := decodeExecution(encoded, envelope.ExecutionID, envelope.Revision)
	if err != nil {
		return flowy.ExecutionEnvelope{}, err
	}
	s.history[envelope.ExecutionID] = append(items, encoded)
	if expectedRevision == 0 && result.Fork != nil {
		s.lineage[envelope.ExecutionID] = *result.Fork
	}
	return result, nil
}

// Caller holds mu; anchor lifetime is independent of leases/history payloads.
func (s *MemoryExecutionStore) decodeAnchoredExecution(
	data []byte,
	id string,
	revision uint64,
) (flowy.ExecutionEnvelope, error) {
	envelope, err := decodeExecution(data, id, revision)
	if err != nil {
		return flowy.ExecutionEnvelope{}, err
	}
	var anchor *flowy.ForkLineage
	if lineage, exists := s.lineage[id]; exists {
		anchor = &lineage
	}
	if err = flowy.ValidateExecutionForkAnchor(envelope, anchor); err != nil {
		return flowy.ExecutionEnvelope{}, err
	}
	return envelope, nil
}

func (s *MemoryExecutionStore) AcquireExecution(
	ctx context.Context,
	executionID, owner string,
	ttl time.Duration,
) (flowy.ExecutionLease, error) {
	if err := ctx.Err(); err != nil {
		return flowy.ExecutionLease{}, err
	}
	if executionID == "" || owner == "" || ttl <= 0 {
		return flowy.ExecutionLease{}, errors.New("flowy: invalid lease acquisition")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	current := s.leases[executionID]
	if current.ExpiresAt.After(s.now()) {
		if current.Owner == owner {
			return flowy.ExecutionLease{}, flowy.ErrThreadLeaseBusy
		}
		return flowy.ExecutionLease{}, flowy.ErrLeaseHeld
	}
	s.fences[executionID]++
	lease := flowy.ExecutionLease{
		ExecutionID: executionID,
		Owner:       owner,
		Incarnation: s.fences[executionID],
		ExpiresAt:   s.now().Add(ttl),
	}
	s.leases[executionID] = lease
	return lease, nil
}

func (s *MemoryExecutionStore) RenewExecution(
	ctx context.Context,
	lease flowy.ExecutionLease,
	ttl time.Duration,
) (flowy.ExecutionLease, error) {
	if err := ctx.Err(); err != nil {
		return flowy.ExecutionLease{}, err
	}
	if ttl <= 0 {
		return flowy.ExecutionLease{}, errors.New("flowy: invalid lease renewal")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.held(lease) {
		return flowy.ExecutionLease{}, flowy.ErrLeaseLost
	}
	lease.ExpiresAt = s.now().Add(ttl)
	s.leases[lease.ExecutionID] = lease
	return lease, nil
}

func (s *MemoryExecutionStore) ReleaseExecution(ctx context.Context, lease flowy.ExecutionLease) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	current, exists := s.leases[lease.ExecutionID]
	if !exists {
		if s.fences[lease.ExecutionID] == lease.Incarnation && lease.Incarnation != 0 {
			return nil
		}
		return flowy.ErrLeaseLost
	}
	if current.Owner != lease.Owner || current.Incarnation != lease.Incarnation {
		return flowy.ErrLeaseLost
	}
	delete(s.leases, lease.ExecutionID)
	return nil
}

func (s *MemoryExecutionStore) held(lease flowy.ExecutionLease) bool {
	current := s.leases[lease.ExecutionID]
	return lease.Incarnation != 0 && current.Owner == lease.Owner && current.Incarnation == lease.Incarnation &&
		current.ExpiresAt.After(s.now())
}

func decodeExecution(data []byte, id string, revision uint64) (flowy.ExecutionEnvelope, error) {
	var result flowy.ExecutionEnvelope
	err := json.Unmarshal(data, &result)
	if err != nil {
		return flowy.ExecutionEnvelope{}, errors.Join(flowy.ErrExecutionCorrupt, err)
	}
	return result, flowy.ValidateExecutionIntegrity(result, id, revision)
}

var _ flowy.ExecutionStore = (*MemoryExecutionStore)(nil)
