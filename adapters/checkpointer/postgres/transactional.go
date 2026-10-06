package postgres

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/skosovsky/flowy"
)

// SaveWithOutbox persists the snapshot and runs enqueueFn in one transaction.
// When enqueueFn fails the insert is rolled back.
func (c *Checkpointer[T, E]) SaveWithOutbox(
	ctx context.Context,
	expectedRevision uint64,
	snapshot flowy.Snapshot[T, E],
	enqueueFn func(ctx context.Context, tx flowy.TransactionHandle, savedRevision uint64) error,
) (uint64, error) {
	options := pgx.TxOptions{} //nolint:exhaustruct_v5 // driver defaults; isolation is specified explicitly
	options.IsoLevel = pgx.ReadCommitted
	tx, err := c.db.BeginTx(ctx, options)
	if err != nil {
		return 0, fmt.Errorf("postgres: begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if lockErr := lockCheckpointThread(ctx, tx, snapshot.ThreadID); lockErr != nil {
		return 0, lockErr
	}
	if fenceErr := validateLeaseWrite(ctx, tx, snapshot.ThreadID); fenceErr != nil {
		return 0, fenceErr
	}

	inserted, saveErr := saveCheckpointInTx(ctx, tx, expectedRevision, snapshot, c.serializer)
	if saveErr != nil {
		return 0, saveErr
	}
	if enqueueFn != nil {
		if err := enqueueFn(ctx, tx, inserted); err != nil {
			return 0, err
		}
	}
	if fenceErr := validateLeaseWrite(ctx, tx, snapshot.ThreadID); fenceErr != nil {
		return 0, fenceErr
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("%w: %w", flowy.ErrTransactionalHandoffCommitFailed, err)
	}
	return inserted, nil
}

var _ flowy.TransactionalCheckpointer[any, any] = (*Checkpointer[any, any])(nil)
