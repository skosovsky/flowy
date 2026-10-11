// Package postgres provides PostgreSQL LeaseManager for flowy thread leases.
package postgres

import (
	"context"
	_ "embed"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/skosovsky/flowy"
	"github.com/skosovsky/flowy/internal/nilvalue"
)

//go:embed sql/upsert_lease.sql
var upsertLeaseSQL string

//go:embed sql/renew_lease.sql
var renewLeaseSQL string

//go:embed sql/release_lease.sql
var releaseLeaseSQL string

//go:embed sql/schema.sql
var schemaSQL string

// SchemaSQL returns the lease adapter schema without requiring a checkpointer.
// Existing owner-only schemas need an explicit offline migration; CREATE TABLE
// IF NOT EXISTS does not upgrade them or reconstruct retained fence history.
func SchemaSQL() string { return schemaSQL }

// DB captures pgx methods used by the lease manager.
type DB interface {
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	BeginTx(ctx context.Context, options pgx.TxOptions) (pgx.Tx, error)
}

// LeaseManager stores thread leases in flowy_leases (same table as checkpointer DeleteIfIdle).
type LeaseManager struct {
	db DB
}

// NewLeaseManager creates a PostgreSQL-backed lease manager.
func NewLeaseManager(db DB) (*LeaseManager, error) {
	if nilvalue.IsNil(db) {
		return nil, flowy.ErrExecutionCapability
	}
	return &LeaseManager{db: db}, nil
}

const (
	threadIDArgument    = "thread_id"
	ownerArgument       = "owner"
	incarnationArgument = "incarnation"
)

// Matches the checkpoint adapter's DeleteIfIdle lock, including absent lease rows.
const threadLockSQL = `SELECT pg_advisory_xact_lock(hashtextextended(@thread_id::text, 0))`

func (m *LeaseManager) lockedTx(ctx context.Context, threadID string) (pgx.Tx, error) {
	options := pgx.TxOptions{} //nolint:exhaustruct_v5 // driver defaults; isolation is specified explicitly
	options.IsoLevel = pgx.ReadCommitted
	tx, err := m.db.BeginTx(ctx, options)
	if err != nil {
		return nil, err
	}
	if _, err := tx.Exec(ctx, threadLockSQL, pgx.NamedArgs{threadIDArgument: threadID}); err != nil {
		_ = tx.Rollback(context.WithoutCancel(ctx))
		return nil, err
	}
	return tx, nil
}

func (m *LeaseManager) Acquire(
	ctx context.Context,
	threadID, owner string,
	ttl time.Duration,
) (flowy.ExecutionLease, error) {
	if threadID == "" || owner == "" {
		return flowy.ExecutionLease{}, errors.New("flowy: lease acquire requires threadID and owner")
	}
	if ttl <= 0 {
		return flowy.ExecutionLease{}, errors.New("flowy: lease ttl must be positive")
	}

	tx, err := m.lockedTx(ctx, threadID)
	if err != nil {
		return flowy.ExecutionLease{}, err
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()

	tag, err := tx.Exec(ctx, upsertLeaseSQL, pgx.NamedArgs{
		threadIDArgument: threadID,
		ownerArgument:    owner,
		"ttl_seconds":    leaseTTLSeconds(ttl),
	})
	if err != nil {
		var storageErr *pgconn.PgError
		if errors.As(err, &storageErr) && storageErr.Code == "22003" {
			return flowy.ExecutionLease{}, fmt.Errorf("%w: %w", flowy.ErrExecutionCapability, err)
		}
		return flowy.ExecutionLease{}, err
	}
	if tag.RowsAffected() == 0 {
		var holder string
		if scanErr := tx.QueryRow(ctx, "SELECT owner FROM flowy_leases WHERE thread_id = @thread_id", pgx.NamedArgs{threadIDArgument: threadID}).
			Scan(&holder); scanErr != nil {
			return flowy.ExecutionLease{}, scanErr
		}
		if holder != owner {
			return flowy.ExecutionLease{}, fmt.Errorf("%w: %s", flowy.ErrLeaseHeld, holder)
		}
		return flowy.ExecutionLease{}, fmt.Errorf("%w: %s", flowy.ErrThreadLeaseBusy, holder)
	}
	lease, err := acquiredLease(ctx, tx, threadID, owner)
	if err != nil {
		return flowy.ExecutionLease{}, err
	}
	if commitErr := tx.Commit(ctx); commitErr != nil {
		return flowy.ExecutionLease{}, commitErr
	}
	return lease, nil
}

func (m *LeaseManager) Renew(
	ctx context.Context,
	lease flowy.ExecutionLease,
	ttl time.Duration,
) (flowy.ExecutionLease, error) {
	if lease.ExecutionID == "" || lease.Owner == "" || lease.Incarnation == 0 || lease.Incarnation > math.MaxInt64 ||
		ttl <= 0 {
		return flowy.ExecutionLease{}, flowy.ErrLeaseLost
	}
	tx, err := m.lockedTx(ctx, lease.ExecutionID)
	if err != nil {
		return flowy.ExecutionLease{}, err
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	tag, err := tx.Exec(ctx, renewLeaseSQL, pgx.NamedArgs{
		threadIDArgument:    lease.ExecutionID,
		ownerArgument:       lease.Owner,
		incarnationArgument: lease.Incarnation,
		"ttl_seconds":       leaseTTLSeconds(ttl),
	})
	if err != nil {
		return flowy.ExecutionLease{}, err
	}
	if tag.RowsAffected() == 0 {
		return flowy.ExecutionLease{}, flowy.ErrLeaseLost
	}
	renewed, err := acquiredLease(ctx, tx, lease.ExecutionID, lease.Owner)
	if err != nil {
		return flowy.ExecutionLease{}, err
	}
	if commitErr := tx.Commit(ctx); commitErr != nil {
		return flowy.ExecutionLease{}, commitErr
	}
	return renewed, nil
}

func (m *LeaseManager) Release(ctx context.Context, lease flowy.ExecutionLease) error {
	if lease.ExecutionID == "" || lease.Owner == "" || lease.Incarnation == 0 || lease.Incarnation > math.MaxInt64 {
		return flowy.ErrLeaseLost
	}
	tx, err := m.lockedTx(ctx, lease.ExecutionID)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	tag, err := tx.Exec(ctx, releaseLeaseSQL, pgx.NamedArgs{
		threadIDArgument:    lease.ExecutionID,
		ownerArgument:       lease.Owner,
		incarnationArgument: lease.Incarnation,
	})
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		var exists bool
		if scanErr := tx.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM flowy_leases WHERE thread_id = @thread_id)", pgx.NamedArgs{threadIDArgument: lease.ExecutionID}).
			Scan(&exists); scanErr != nil {
			return scanErr
		}
		if exists {
			return flowy.ErrLeaseLost
		}
	}
	return tx.Commit(ctx)
}

func acquiredLease(ctx context.Context, tx pgx.Tx, id, owner string) (flowy.ExecutionLease, error) {
	lease := flowy.ExecutionLease{ExecutionID: id, Owner: owner, Incarnation: 0, ExpiresAt: time.Time{}}
	err := tx.QueryRow(ctx, "SELECT incarnation, expires_at FROM flowy_leases WHERE thread_id = @thread_id", pgx.NamedArgs{threadIDArgument: id}).
		Scan(&lease.Incarnation, &lease.ExpiresAt)
	if err != nil {
		return flowy.ExecutionLease{}, err
	}
	if lease.Incarnation == 0 {
		return flowy.ExecutionLease{}, flowy.ErrExecutionCapability
	}
	return lease, nil
}

func (m *LeaseManager) IsHeld(ctx context.Context, threadID string) (bool, error) {
	_, held, err := m.Holder(ctx, threadID)
	return held, err
}

func (m *LeaseManager) Holder(ctx context.Context, threadID string) (string, bool, error) {
	var owner string
	var expiresAt time.Time
	err := m.db.QueryRow(ctx,
		`SELECT owner, expires_at FROM flowy_leases WHERE thread_id = @thread_id`,
		pgx.NamedArgs{threadIDArgument: threadID},
	).Scan(&owner, &expiresAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	if !expiresAt.After(time.Now()) {
		return "", false, nil
	}
	return owner, true, nil
}

// NativeLeaseManager marks storage-backed lease records in flowy_leases.
func (*LeaseManager) NativeLeaseManager() {}

var _ flowy.LeaseManager = (*LeaseManager)(nil)
var _ flowy.NativeLeaseManager = (*LeaseManager)(nil)

// PostgreSQL timestamp/interval precision is microseconds.
func leaseTTLSeconds(ttl time.Duration) float64 {
	micros := ttl.Microseconds()
	if ttl%time.Microsecond != 0 {
		micros++
	}
	return float64(micros) / float64(time.Second/time.Microsecond)
}
