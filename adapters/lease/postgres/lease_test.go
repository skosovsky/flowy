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
		owner       string
		expiresAt   time.Time
		incarnation uint64
	}
	fences   map[string]uint64
	execRows int64
	ttlArgs  []float64
}

type leaseFakeTx struct {
	pgx.Tx

	db *leaseFakeDB
}

func (d *leaseFakeDB) BeginTx(_ context.Context, options pgx.TxOptions) (pgx.Tx, error) {
	if options.IsoLevel != pgx.ReadCommitted {
		return nil, errors.New("expected read committed")
	}
	return &leaseFakeTx{db: d}, nil
}

func (tx *leaseFakeTx) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	return tx.db.Exec(ctx, sql, args...)
}

func (tx *leaseFakeTx) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	return tx.db.QueryRow(ctx, sql, args...)
}

func (*leaseFakeTx) Commit(context.Context) error   { return nil }
func (*leaseFakeTx) Rollback(context.Context) error { return nil }

func (d *leaseFakeDB) Exec(_ context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	named, _ := args[0].(pgx.NamedArgs)
	threadID, _ := named["thread_id"].(string)
	owner, _ := named["owner"].(string)
	ttlSec, _ := named["ttl_seconds"].(float64)
	if _, exists := named["ttl_seconds"]; exists {
		d.ttlArgs = append(d.ttlArgs, ttlSec)
	}
	incarnation, _ := named["incarnation"].(uint64)

	if strings.Contains(sql, "INSERT INTO flowy_leases") {
		if d.leases == nil {
			d.leases = map[string]struct {
				owner       string
				expiresAt   time.Time
				incarnation uint64
			}{}
		}
		now := time.Now()
		rec, ok := d.leases[threadID]
		if ok && rec.expiresAt.After(now) {
			return pgconn.CommandTag{}, nil
		}
		if d.fences == nil {
			d.fences = make(map[string]uint64)
		}
		d.fences[threadID]++
		d.leases[threadID] = struct {
			owner       string
			expiresAt   time.Time
			incarnation uint64
		}{owner: owner, expiresAt: now.Add(time.Duration(ttlSec * float64(time.Second))), incarnation: d.fences[threadID]}
		d.execRows = 1
		return pgconn.NewCommandTag("INSERT 1"), nil
	}
	if strings.Contains(sql, "UPDATE flowy_leases") {
		rec, ok := d.leases[threadID]
		if !ok || rec.owner != owner || rec.incarnation != incarnation || !rec.expiresAt.After(time.Now()) {
			return pgconn.CommandTag{}, nil
		}
		rec.expiresAt = time.Now().Add(time.Duration(ttlSec * float64(time.Second)))
		d.leases[threadID] = rec
		return pgconn.NewCommandTag("UPDATE 1"), nil
	}
	if strings.Contains(sql, "DELETE FROM flowy_leases") {
		rec, ok := d.leases[threadID]
		if ok && rec.owner == owner && rec.incarnation == incarnation {
			delete(d.leases, threadID)
			return pgconn.NewCommandTag("DELETE 1"), nil
		}
		return pgconn.CommandTag{}, nil
	}
	return pgconn.CommandTag{}, nil
}

func (d *leaseFakeDB) QueryRow(_ context.Context, sql string, args ...any) pgx.Row {
	named, _ := args[0].(pgx.NamedArgs)
	threadID, _ := named["thread_id"].(string)
	rec, ok := d.leases[threadID]
	if strings.Contains(sql, "SELECT EXISTS") {
		return leaseFakeRow{values: []any{ok}}
	}
	if !ok || !rec.expiresAt.After(time.Now()) {
		return leaseFakeRow{err: pgx.ErrNoRows}
	}
	if strings.Contains(sql, "SELECT incarnation") {
		return leaseFakeRow{values: []any{rec.incarnation, rec.expiresAt}}
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
		case *uint64:
			*d = r.values[i].(uint64)
		default:
			return fmt.Errorf("unsupported scan type %T", dest[i])
		}
	}
	return nil
}

func TestLeaseManagerAcquireConflict(t *testing.T) {
	t.Parallel()
	db := &leaseFakeDB{}
	lm, constructorErr := NewLeaseManager(db)
	if constructorErr != nil {
		t.Fatal(constructorErr)
	}
	ctx := context.Background()
	lease, err := lm.Acquire(ctx, "th-1", "a", time.Minute)
	if err != nil {
		t.Fatalf("acquire a: %v", err)
	}
	if _, acquireErr := lm.Acquire(ctx, "th-1", "b", time.Minute); !errors.Is(acquireErr, flowy.ErrLeaseHeld) {
		t.Fatalf("expected ErrLeaseHeld, got %v", acquireErr)
	}
	if _, acquireErr := lm.Acquire(ctx, "th-1", "a", time.Minute); !errors.Is(acquireErr, flowy.ErrThreadLeaseBusy) {
		t.Fatalf("expected ErrThreadLeaseBusy, got %v", acquireErr)
	}
	if releaseErr := lm.Release(ctx, lease); releaseErr != nil {
		t.Fatalf("release: %v", releaseErr)
	}
}
