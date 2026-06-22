package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/skosovsky/flowy"
	"github.com/skosovsky/flowy/checkpoint"
)

// TxBeginner is implemented by *pgxpool.Pool and pgx.Tx.
type TxBeginner interface {
	Begin(ctx context.Context) (pgx.Tx, error)
}

// SaveWithOutbox persists the snapshot and runs enqueueFn in one transaction.
// When enqueueFn fails the insert is rolled back.
func (c *Checkpointer[T, E]) SaveWithOutbox(
	ctx context.Context,
	expectedRevision uint64,
	snapshot flowy.Snapshot[T, E],
	enqueueFn func(ctx context.Context, tx flowy.TransactionHandle, savedRevision uint64) error,
) (uint64, error) {
	beginner, ok := c.db.(TxBeginner)
	if !ok {
		return 0, errors.New("postgres: database does not support transactions")
	}
	tx, err := beginner.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("postgres: begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	newRevision := expectedRevision + 1
	snapshot.Revision = newRevision
	stored, err := checkpoint.EncodeRecord(snapshot, c.serializer)
	if err != nil {
		return 0, err
	}
	row := tx.QueryRow(ctx, saveOccSQL, pgx.NamedArgs{
		"thread_id":         stored.ThreadID,
		"expected_revision": expectedRevision,
		"node_id":           stored.NodeID,
		"state_payload":     stored.StatePayload,
		"run_meta":          stored.RunMeta,
		"effects":           stored.Effects,
		"updated_at":        stored.UpdatedAt,
	})
	var inserted uint64
	if err := row.Scan(&inserted); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return 0, flowy.ErrConcurrencyConflict
		}
		return 0, err
	}
	if enqueueFn != nil {
		if err := enqueueFn(ctx, tx, inserted); err != nil {
			return 0, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("%w: %w", flowy.ErrTransactionalHandoffCommitFailed, err)
	}
	return inserted, nil
}

var _ flowy.TransactionalCheckpointer[any, any] = (*Checkpointer[any, any])(nil)
