package testutil

import (
	"context"
	"slices"

	"github.com/skosovsky/flowy"
)

func (s *MemoryExecutionStore) RetainExecution(
	ctx context.Context,
	request flowy.ExecutionRetentionRequest,
) (flowy.ExecutionRetentionReceipt, error) {
	if err := ctx.Err(); err != nil {
		return flowy.ExecutionRetentionReceipt{}, err
	}
	request.Policy.ProtectedRevisions = slices.Clone(request.Policy.ProtectedRevisions)
	if err := request.Validate(); err != nil {
		return flowy.ExecutionRetentionReceipt{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.leases[request.ExecutionID].ExpiresAt.After(s.now()) {
		return flowy.ExecutionRetentionReceipt{}, flowy.ErrThreadLeaseBusy
	}
	items := s.history[request.ExecutionID]
	if s.heads[request.ExecutionID] == 0 {
		return flowy.ExecutionRetentionReceipt{}, flowy.ErrThreadNotFound
	}
	if s.heads[request.ExecutionID] != request.Revision {
		return flowy.ExecutionRetentionReceipt{}, flowy.ErrConcurrencyConflict
	}
	receipt := flowy.ExecutionRetentionReceipt{
		ExecutionID:      request.ExecutionID,
		Revision:         request.Revision,
		DeletedRevisions: 0,
		DeletedBytes:     0,
	}
	if len(items[request.Revision]) == 0 {
		if request.Policy.DeletePayload {
			return receipt, nil
		}
		return flowy.ExecutionRetentionReceipt{}, flowy.ErrExecutionCheckpointUnavailable
	}
	source, err := s.decodeAnchoredExecution(items[request.Revision], request.ExecutionID, request.Revision)
	if err != nil {
		return flowy.ExecutionRetentionReceipt{}, err
	}
	if err = flowy.ValidateExecutionLifecycleBoundary(source); err != nil {
		return flowy.ExecutionRetentionReceipt{}, err
	}
	if request.Policy.DeletePayload && source.Terminal == nil {
		return flowy.ExecutionRetentionReceipt{}, flowy.ErrExecutionLifecycleUnsafe
	}
	if err = s.validateRetentionProtected(request, items); err != nil {
		return flowy.ExecutionRetentionReceipt{}, err
	}
	for index, data := range items {
		if len(data) == 0 || flowy.ExecutionRevisionRetained(request, index) {
			continue
		}
		receipt.DeletedRevisions++
		receipt.DeletedBytes += int64(len(data))
		delete(items, index)
	}
	return receipt, nil
}

var _ flowy.ExecutionRetentionStore = (*MemoryExecutionStore)(nil)

func (s *MemoryExecutionStore) validateRetentionProtected(
	request flowy.ExecutionRetentionRequest,
	items map[uint64][]byte,
) error {
	for _, revision := range request.Policy.ProtectedRevisions {
		if len(items[revision]) == 0 {
			return flowy.ErrExecutionCheckpointUnavailable
		}
		if _, err := s.decodeAnchoredExecution(items[revision], request.ExecutionID, revision); err != nil {
			return err
		}
	}
	return nil
}
