package testutil

import (
	"context"
	"encoding/json"

	"github.com/skosovsky/flowy"
)

func (s *MemoryExecutionStore) LoadRollover(ctx context.Context, sourceID string) (*flowy.RolloverReceipt, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	receipt, exists := s.outgoing[sourceID]
	if !exists {
		return nil, nil //nolint:nilnil // An absent optional receipt is the explicit LoadRollover contract.
	}
	if err := receipt.Validate(); err != nil {
		return nil, err
	}
	return &receipt, nil
}

func (s *MemoryExecutionStore) CommitRollover(
	ctx context.Context,
	lease flowy.ExecutionLease,
	source flowy.HistoricalCheckpointReference,
	target flowy.ExecutionEnvelope,
) (flowy.RolloverReceipt, error) {
	if err := ctx.Err(); err != nil {
		return flowy.RolloverReceipt{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.held(lease) || lease.ExecutionID != source.ExecutionID {
		return flowy.RolloverReceipt{}, flowy.ErrLeaseLost
	}
	if _, exists := s.outgoing[source.ExecutionID]; exists {
		return flowy.RolloverReceipt{}, flowy.ErrExecutionRolloverConflict
	}
	items := s.history[source.ExecutionID]
	if s.heads[source.ExecutionID] != source.Revision {
		return flowy.RolloverReceipt{}, flowy.ErrConcurrencyConflict
	}
	if source.Revision == 0 {
		return flowy.RolloverReceipt{}, flowy.ErrThreadNotFound
	}
	current, err := s.decodeAnchoredExecution(items[source.Revision], source.ExecutionID, source.Revision)
	if err != nil {
		return flowy.RolloverReceipt{}, err
	}
	if current.Digest != source.Digest {
		return flowy.RolloverReceipt{}, flowy.ErrExecutionRolloverConflict
	}
	if s.heads[target.ExecutionID] != 0 || s.fences[target.ExecutionID] != 0 {
		return flowy.RolloverReceipt{}, flowy.ErrExecutionRolloverConflict
	}
	transferred, created, receipt, err := flowy.PrepareExecutionRolloverPublication(current, target, lease)
	if err != nil {
		return flowy.RolloverReceipt{}, err
	}
	sourceBytes, err := json.Marshal(transferred)
	if err != nil {
		return flowy.RolloverReceipt{}, err
	}
	targetBytes, err := json.Marshal(created)
	if err != nil {
		return flowy.RolloverReceipt{}, err
	}
	// All validation/serialization completes before mutating either aggregate.
	items[transferred.Revision] = sourceBytes
	s.heads[source.ExecutionID] = transferred.Revision
	s.history[created.ExecutionID] = map[uint64][]byte{1: targetBytes}
	s.heads[created.ExecutionID] = 1
	s.outgoing[source.ExecutionID] = receipt
	s.incoming[created.ExecutionID] = receipt
	return receipt, nil
}

var _ flowy.ExecutionRolloverStore = (*MemoryExecutionStore)(nil)
