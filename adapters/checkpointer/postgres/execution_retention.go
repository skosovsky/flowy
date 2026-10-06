package postgres

import (
	"context"
	"errors"
	"math"
	"slices"

	"github.com/jackc/pgx/v5"

	"github.com/skosovsky/flowy"
)

func (s *ExecutionStore) RetainExecution(
	ctx context.Context,
	request flowy.ExecutionRetentionRequest,
) (flowy.ExecutionRetentionReceipt, error) {
	request.Policy.ProtectedRevisions = slices.Clone(request.Policy.ProtectedRevisions)
	if request.Revision > math.MaxInt64 || request.Policy.KeepLast < 0 {
		return flowy.ExecutionRetentionReceipt{}, flowy.ErrExecutionLifecycleUnsafe
	}
	if err := request.Validate(); err != nil {
		return flowy.ExecutionRetentionReceipt{}, err
	}
	tx, err := s.begin(ctx)
	if err != nil {
		return flowy.ExecutionRetentionReceipt{}, err
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	var revision uint64
	var leased, deleted bool
	err = tx.QueryRow(ctx, `SELECT revision,COALESCE(lease_expiry>clock_timestamp(),false),payload_deleted
FROM flowy_executions WHERE execution_id=$1 FOR UPDATE`, request.ExecutionID).Scan(&revision, &leased, &deleted)
	if errors.Is(err, pgx.ErrNoRows) {
		return flowy.ExecutionRetentionReceipt{}, flowy.ErrThreadNotFound
	}
	if err != nil {
		return flowy.ExecutionRetentionReceipt{}, err
	}
	if leased {
		return flowy.ExecutionRetentionReceipt{}, flowy.ErrThreadLeaseBusy
	}
	if revision != request.Revision {
		return flowy.ExecutionRetentionReceipt{}, flowy.ErrConcurrencyConflict
	}
	receipt := flowy.ExecutionRetentionReceipt{
		ExecutionID:      request.ExecutionID,
		Revision:         revision,
		DeletedRevisions: 0,
		DeletedBytes:     0,
	}
	if deleted {
		if request.Policy.DeletePayload {
			return receipt, nil
		}
		return flowy.ExecutionRetentionReceipt{}, flowy.ErrExecutionCheckpointUnavailable
	}
	if err = validateRetentionDependencies(ctx, tx, request); err != nil {
		return flowy.ExecutionRetentionReceipt{}, err
	}
	firstRetained, protected, selectionErr := retentionSelection(request, revision)
	if selectionErr != nil {
		return flowy.ExecutionRetentionReceipt{}, selectionErr
	}
	err = tx.QueryRow(ctx, `WITH removed AS (
DELETE FROM flowy_execution_history WHERE execution_id=$1
AND ($2 OR (revision<$3 AND NOT(revision=ANY($4::bigint[]))))
RETURNING octet_length(payload::text) AS bytes)
SELECT count(*),COALESCE(sum(bytes),0) FROM removed`, request.ExecutionID, request.Policy.DeletePayload, firstRetained, protected).Scan(&receipt.DeletedRevisions, &receipt.DeletedBytes)
	if err != nil {
		return flowy.ExecutionRetentionReceipt{}, err
	}
	if request.Policy.DeletePayload {
		if _, err = tx.Exec(
			ctx,
			`UPDATE flowy_executions SET payload_deleted=true WHERE execution_id=$1`,
			request.ExecutionID,
		); err != nil {
			return flowy.ExecutionRetentionReceipt{}, err
		}
	}
	if err = tx.Commit(ctx); err != nil {
		return flowy.ExecutionRetentionReceipt{}, err
	}
	return receipt, nil
}

var _ flowy.ExecutionRetentionStore = (*ExecutionStore)(nil)

func validateRetentionDependencies(ctx context.Context, tx pgx.Tx, request flowy.ExecutionRetentionRequest) error {
	source, err := loadAnchoredEnvelope(
		tx.QueryRow(
			ctx,
			`SELECT e.revision,h.payload,e.fork_lineage,e.rollover_incoming,e.rollover_outgoing,e.payload_deleted
FROM flowy_executions e LEFT JOIN flowy_execution_history h ON h.execution_id=e.execution_id AND h.revision=e.revision
WHERE e.execution_id=$1`,
			request.ExecutionID,
		),
		request.ExecutionID,
		request.Revision,
	)
	if err != nil {
		return err
	}
	if err = flowy.ValidateExecutionLifecycleBoundary(source); err != nil {
		return err
	}
	if request.Policy.DeletePayload && source.Terminal == nil {
		return flowy.ErrExecutionLifecycleUnsafe
	}
	for _, protected := range request.Policy.ProtectedRevisions {
		if _, err = loadAnchoredEnvelope(
			tx.QueryRow(
				ctx,
				`SELECT $2::bigint,h.payload,e.fork_lineage,e.rollover_incoming,e.rollover_outgoing,e.payload_deleted
FROM flowy_executions e LEFT JOIN flowy_execution_history h ON h.execution_id=e.execution_id AND h.revision=$2
WHERE e.execution_id=$1`,
				request.ExecutionID,
				protected,
			),
			request.ExecutionID,
			protected,
		); err != nil {
			return err
		}
	}
	return nil
}

func retentionSelection(request flowy.ExecutionRetentionRequest, revision uint64) (uint64, []int64, error) {
	if request.Policy.KeepLast < 0 || revision > math.MaxInt64 {
		return 0, nil, flowy.ErrExecutionLifecycleUnsafe
	}
	// KeepLast is validated int; subtraction cannot underflow after this clamp.
	firstRetained := uint64(1)
	if uint64(request.Policy.KeepLast) < revision {
		firstRetained = revision - uint64(request.Policy.KeepLast) + 1
	}
	if !request.Policy.DeletePayload && firstRetained > revision {
		firstRetained = revision
	}
	protected := make([]int64, len(request.Policy.ProtectedRevisions))
	for index, value := range request.Policy.ProtectedRevisions {
		if value > math.MaxInt64 {
			return 0, nil, flowy.ErrExecutionLifecycleUnsafe
		}
		protected[index] = int64(value)
	}
	return firstRetained, protected, nil
}
