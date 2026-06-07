package postgres

import (
	"context"

	"github.com/jackc/pgx/v5"

	"github.com/skosovsky/flowy"
)

// PgxTxFromContext returns the active pgx.Tx from SaveWithOutbox enqueueFn context.
func PgxTxFromContext(ctx context.Context) (pgx.Tx, bool) {
	tx, ok := flowy.OutboxTxFromContext(ctx)
	if !ok {
		return nil, false
	}
	pgxTx, ok := tx.(pgx.Tx)
	return pgxTx, ok
}
