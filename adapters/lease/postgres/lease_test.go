package postgres

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/skosovsky/flowy"
)

type leaseFakeDB struct {
	leases map[string]struct {
		owner     string
		expiresAt time.Time
	}
	execRows int64
}

func (d *leaseFakeDB) Exec(_ context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	named, _ := args[0].(pgx.NamedArgs)
	threadID, _ := named["thread_id"].(string)
	owner, _ := named["owner"].(string)
	ttlSec, _ := named["ttl_seconds"].(int)

	if strings.Contains(sql, "INSERT INTO flowy_leases") {
		if d.leases == nil {
			d.leases = map[string]struct {
				owner     string
				expiresAt time.Time
			}{}
		}
		now := time.Now()
		rec, ok := d.leases[threadID]
		if ok && rec.expiresAt.After(now) && rec.owner != owner {
			return pgconn.CommandTag{}, nil
		}
		d.leases[threadID] = struct {
			owner     string
			expiresAt time.Time
		}{owner: owner, expiresAt: now.Add(time.Duration(ttlSec) * time.Second)}
		d.execRows = 1
		return pgconn.NewCommandTag("INSERT 1"), nil
	}
	if strings.Contains(sql, "UPDATE flowy_leases") {
		rec, ok := d.leases[threadID]
		if !ok || rec.owner != owner || !rec.expiresAt.After(time.Now()) {
			return pgconn.CommandTag{}, nil
		}
		rec.expiresAt = time.Now().Add(time.Duration(ttlSec) * time.Second)
		d.leases[threadID] = rec
		return pgconn.NewCommandTag("UPDATE 1"), nil
	}
	if strings.Contains(sql, "DELETE FROM flowy_leases") {
		rec, ok := d.leases[threadID]
		if ok && rec.owner == owner {
			delete(d.leases, threadID)
			return pgconn.NewCommandTag("DELETE 1"), nil
		}
		return pgconn.CommandTag{}, nil
	}
	return pgconn.CommandTag{}, nil
}

func (d *leaseFakeDB) QueryRow(_ context.Context, _ string, args ...any) pgx.Row {
	named, _ := args[0].(pgx.NamedArgs)
	threadID, _ := named["thread_id"].(string)
	rec, ok := d.leases[threadID]
	if !ok || !rec.expiresAt.After(time.Now()) {
		return leaseFakeRow{err: pgx.ErrNoRows}
	}
	return leaseFakeRow{values: []any{rec.owner, rec.expiresAt}}
}

type leaseFakeRow struct {
	values []any
	err    error
}

func (r leaseFakeRow) Scan(dest ...any) error {
	if r.err != nil {
		return r.err
	}
	for i := range dest {
		switch d := dest[i].(type) {
		case *string:
			*d = r.values[i].(string)
		case *time.Time:
			*d = r.values[i].(time.Time)
		case *bool:
			*d = r.values[i].(bool)
		default:
			return fmt.Errorf("unsupported scan type %T", dest[i])
		}
	}
	return nil
}

func TestLeaseManagerAcquireConflict(t *testing.T) {
	t.Parallel()
	db := &leaseFakeDB{}
	lm := NewLeaseManager(db)
	ctx := context.Background()
	if err := lm.Acquire(ctx, "th-1", "a", time.Minute); err != nil {
		t.Fatalf("acquire a: %v", err)
	}
	if err := lm.Acquire(ctx, "th-1", "b", time.Minute); !errors.Is(err, flowy.ErrLeaseHeld) {
		t.Fatalf("expected ErrLeaseHeld, got %v", err)
	}
	if err := lm.Acquire(ctx, "th-1", "a", time.Minute); !errors.Is(err, flowy.ErrThreadBusy) {
		t.Fatalf("expected ErrThreadBusy, got %v", err)
	}
	if err := lm.Release(ctx, "th-1", "a"); err != nil {
		t.Fatalf("release: %v", err)
	}
}
