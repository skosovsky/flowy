// Package postgres provides PostgreSQL LeaseManager for flowy thread leases.
package postgres

import (
	"context"
	_ "embed"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/skosovsky/flowy"
)

//go:embed sql/upsert_lease.sql
var upsertLeaseSQL string

//go:embed sql/renew_lease.sql
var renewLeaseSQL string

//go:embed sql/release_lease.sql
var releaseLeaseSQL string

// DB captures pgx methods used by the lease manager.
type DB interface {
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// LeaseManager stores thread leases in flowy_leases (same table as checkpointer DeleteIfIdle).
type LeaseManager struct {
	db DB
}

// NewLeaseManager creates a PostgreSQL-backed lease manager.
func NewLeaseManager(db DB) *LeaseManager {
	return &LeaseManager{db: db}
}

func (m *LeaseManager) Acquire(ctx context.Context, threadID, owner string, ttl time.Duration) error {
	if threadID == "" || owner == "" {
		return errors.New("flowy: lease acquire requires threadID and owner")
	}
	if ttl <= 0 {
		return errors.New("flowy: lease ttl must be positive")
	}

	holder, held, err := m.Holder(ctx, threadID)
	if err != nil {
		return err
	}
	if held {
		if holder != owner {
			return fmt.Errorf("%w: %s", flowy.ErrLeaseHeld, holder)
		}
		return fmt.Errorf("%w: %s", flowy.ErrThreadBusy, holder)
	}

	tag, err := m.db.Exec(ctx, upsertLeaseSQL, pgx.NamedArgs{
		"thread_id":   threadID,
		"owner":       owner,
		"ttl_seconds": int(ttl.Seconds()),
	})
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		holder, held, holderErr := m.Holder(ctx, threadID)
		if holderErr != nil {
			return holderErr
		}
		if held && holder != owner {
			return fmt.Errorf("%w: %s", flowy.ErrLeaseHeld, holder)
		}
		return fmt.Errorf("%w: %s", flowy.ErrThreadBusy, holder)
	}
	return nil
}

func (m *LeaseManager) Renew(ctx context.Context, threadID, owner string, ttl time.Duration) error {
	tag, err := m.db.Exec(ctx, renewLeaseSQL, pgx.NamedArgs{
		"thread_id":   threadID,
		"owner":       owner,
		"ttl_seconds": int(ttl.Seconds()),
	})
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		holder, held, holderErr := m.Holder(ctx, threadID)
		if holderErr != nil {
			return holderErr
		}
		if held && holder != owner {
			return fmt.Errorf("%w: %s", flowy.ErrLeaseHeld, holder)
		}
		return fmt.Errorf("%w: %s", flowy.ErrLeaseHeld, holder)
	}
	return nil
}

func (m *LeaseManager) Release(ctx context.Context, threadID, owner string) error {
	_, err := m.db.Exec(ctx, releaseLeaseSQL, pgx.NamedArgs{
		"thread_id": threadID,
		"owner":     owner,
	})
	return err
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
		pgx.NamedArgs{"thread_id": threadID},
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
