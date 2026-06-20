package postgres

import (
	"github.com/jackc/pgx/v5"

	"github.com/skosovsky/flowy"
)

// PgxTxFromHandle returns the active pgx.Tx from SaveWithOutbox enqueueFn handle.
func PgxTxFromHandle(tx flowy.TransactionHandle) (pgx.Tx, bool) {
	pgxTx, ok := tx.(pgx.Tx)
	return pgxTx, ok
}
