package postgres

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/jackc/pgx/v5"

	"github.com/skosovsky/flowy"
)

func validateStoredLifecycleAnchors(envelope flowy.ExecutionEnvelope, incomingPayload, outgoingPayload []byte) error {
	var incoming *flowy.RolloverReceipt
	var outgoing *flowy.RolloverReceipt
	if len(incomingPayload) != 0 {
		if err := json.Unmarshal(incomingPayload, &incoming); err != nil {
			return errors.Join(flowy.ErrExecutionCorrupt, err)
		}
	}
	if len(outgoingPayload) != 0 {
		if err := json.Unmarshal(outgoingPayload, &outgoing); err != nil {
			return errors.Join(flowy.ErrExecutionCorrupt, err)
		}
	}
	return flowy.ValidateExecutionLifecycleAnchors(envelope, incoming, outgoing)
}

func (s *ExecutionStore) LoadRollover(ctx context.Context, sourceID string) (*flowy.RolloverReceipt, error) {
	var payload []byte
	err := s.db.QueryRow(ctx, `SELECT rollover_outgoing FROM flowy_executions WHERE execution_id=$1`, sourceID).
		Scan(&payload)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil //nolint:nilnil // LoadRollover explicitly returns an absent optional immutable receipt.
	}
	if err != nil {
		return nil, err
	}
	if len(payload) == 0 {
		return nil, nil //nolint:nilnil // LoadRollover explicitly returns an absent optional immutable receipt.
	}
	var receipt *flowy.RolloverReceipt
	if err = json.Unmarshal(payload, &receipt); err != nil {
		return nil, errors.Join(flowy.ErrExecutionCorrupt, err)
	}
	if receipt == nil || receipt.Validate() != nil || receipt.Lineage.Source.ExecutionID != sourceID {
		return nil, flowy.ErrExecutionCorrupt
	}
	return receipt, nil
}

func (s *ExecutionStore) CommitRollover(
	ctx context.Context,
	lease flowy.ExecutionLease,
	source flowy.HistoricalCheckpointReference,
	target flowy.ExecutionEnvelope,
) (flowy.RolloverReceipt, error) {
	tx, err := s.begin(ctx)
	if err != nil {
		return flowy.RolloverReceipt{}, err
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	current, err := lockRolloverSource(ctx, tx, lease, source)
	if err != nil {
		return flowy.RolloverReceipt{}, err
	}
	transferred, created, receipt, err := flowy.PrepareExecutionRolloverPublication(current, target, lease)
	if err != nil {
		return flowy.RolloverReceipt{}, err
	}
	var exists bool
	if err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM flowy_executions WHERE execution_id=$1)`, created.ExecutionID).
		Scan(&exists); err != nil {
		return flowy.RolloverReceipt{}, err
	}
	if exists {
		return flowy.RolloverReceipt{}, flowy.ErrExecutionRolloverConflict
	}
	sourcePayload, err := json.Marshal(transferred)
	if err != nil {
		return flowy.RolloverReceipt{}, err
	}
	targetPayload, err := json.Marshal(created)
	if err != nil {
		return flowy.RolloverReceipt{}, err
	}
	receiptPayload, err := json.Marshal(receipt)
	if err != nil {
		return flowy.RolloverReceipt{}, err
	}
	lineagePayload, err := json.Marshal(receipt)
	if err != nil {
		return flowy.RolloverReceipt{}, err
	}
	tag, err := tx.Exec(ctx, `INSERT INTO flowy_executions(execution_id,revision,rollover_incoming)
VALUES($1,1,$2::jsonb) ON CONFLICT DO NOTHING`, created.ExecutionID, json.RawMessage(lineagePayload))
	if err != nil {
		return flowy.RolloverReceipt{}, err
	}
	if tag.RowsAffected() != 1 {
		return flowy.RolloverReceipt{}, flowy.ErrExecutionRolloverConflict
	}
	if _, err = tx.Exec(
		ctx,
		`INSERT INTO flowy_execution_history(execution_id,revision,payload) VALUES($1,$2,$3::jsonb),($4,1,$5::jsonb)`,
		source.ExecutionID,
		transferred.Revision,
		json.RawMessage(sourcePayload),
		created.ExecutionID,
		json.RawMessage(targetPayload),
	); err != nil {
		return flowy.RolloverReceipt{}, err
	}
	tag, err = tx.Exec(ctx, `UPDATE flowy_executions SET revision=$1,rollover_outgoing=$2::jsonb
WHERE execution_id=$3 AND fence=$4 AND lease_owner=$5 AND lease_expiry>clock_timestamp()`, transferred.Revision, json.RawMessage(receiptPayload), source.ExecutionID, lease.Incarnation, lease.Owner)
	if err != nil {
		return flowy.RolloverReceipt{}, err
	}
	if tag.RowsAffected() != 1 {
		return flowy.RolloverReceipt{}, flowy.ErrLeaseLost
	}
	if err = replaceDiscoveryProjection(ctx, tx, transferred); err != nil {
		return flowy.RolloverReceipt{}, err
	}
	if err = replaceDiscoveryProjection(ctx, tx, created); err != nil {
		return flowy.RolloverReceipt{}, err
	}
	if err = tx.Commit(ctx); err != nil {
		return flowy.RolloverReceipt{}, err
	}
	return receipt, nil
}

func lockRolloverSource(
	ctx context.Context,
	tx pgx.Tx,
	lease flowy.ExecutionLease,
	source flowy.HistoricalCheckpointReference,
) (flowy.ExecutionEnvelope, error) {
	if lease.ExecutionID != source.ExecutionID || lease.Owner == "" || lease.Incarnation == 0 {
		return flowy.ExecutionEnvelope{}, flowy.ErrLeaseLost
	}
	var revision uint64
	var held bool
	err := tx.QueryRow(ctx, `SELECT revision,COALESCE(lease_owner=$2 AND fence=$3 AND lease_expiry>clock_timestamp(),false)
FROM flowy_executions WHERE execution_id=$1 FOR UPDATE`, source.ExecutionID, lease.Owner, lease.Incarnation).
		Scan(&revision, &held)
	if errors.Is(err, pgx.ErrNoRows) || (err == nil && !held) {
		return flowy.ExecutionEnvelope{}, flowy.ErrLeaseLost
	}
	if err != nil {
		return flowy.ExecutionEnvelope{}, err
	}
	if revision != source.Revision {
		return flowy.ExecutionEnvelope{}, flowy.ErrConcurrencyConflict
	}
	envelope, err := loadAnchoredEnvelope(
		tx.QueryRow(
			ctx,
			`SELECT e.revision,h.payload,e.fork_lineage,e.rollover_incoming,e.rollover_outgoing,e.payload_deleted
FROM flowy_executions e LEFT JOIN flowy_execution_history h ON h.execution_id=e.execution_id AND h.revision=e.revision
WHERE e.execution_id=$1`,
			source.ExecutionID,
		),
		source.ExecutionID,
		source.Revision,
	)
	if err != nil {
		return flowy.ExecutionEnvelope{}, err
	}
	if envelope.Digest != source.Digest {
		return flowy.ExecutionEnvelope{}, flowy.ErrExecutionRolloverConflict
	}
	return envelope, nil
}

var _ flowy.ExecutionRolloverStore = (*ExecutionStore)(nil)
