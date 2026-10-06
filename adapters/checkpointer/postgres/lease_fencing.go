package postgres

import (
	"context"
	"math"

	"github.com/jackc/pgx/v5"

	"github.com/skosovsky/flowy"
)

const checkpointThreadLockSQL = `SELECT pg_advisory_xact_lock(hashtextextended(@thread_id::text, 0))`

const validateLeaseWriteSQL = `SELECT /* flowy:lease-fence */ CASE
WHEN @has_handle::boolean THEN EXISTS (
    SELECT 1 FROM flowy_leases WHERE thread_id = @thread_id
    AND owner = @lease_owner AND incarnation = @lease_incarnation
    AND expires_at > clock_timestamp()
)
ELSE NOT EXISTS (
    SELECT 1 FROM flowy_leases WHERE thread_id = @thread_id
    AND expires_at > clock_timestamp()
)
END`

func lockCheckpointThread(ctx context.Context, tx pgx.Tx, id string) error {
	_, err := tx.Exec(ctx, checkpointThreadLockSQL, pgx.NamedArgs{threadIDArgument: id})
	return err
}

// Call only while holding the shared thread lock. Check again before commit,
// because host encoding/enqueue work can consume the remaining lease lifetime.
func validateLeaseWrite(ctx context.Context, tx pgx.Tx, id string) error {
	lease, supplied := flowy.ExecutionLeaseFromContext(ctx)
	if supplied &&
		(lease.ExecutionID != id || lease.Owner == "" || lease.Incarnation == 0 || lease.Incarnation > math.MaxInt64) {
		return flowy.ErrLeaseLost
	}
	var allowed bool
	err := tx.QueryRow(ctx, validateLeaseWriteSQL, pgx.NamedArgs{
		threadIDArgument:    id,
		"has_handle":        supplied,
		"lease_owner":       lease.Owner,
		"lease_incarnation": lease.Incarnation,
	}).Scan(&allowed)
	if err != nil {
		return err
	}
	if !allowed {
		return flowy.ErrLeaseLost
	}
	return nil
}
