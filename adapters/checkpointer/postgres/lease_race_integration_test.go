//go:build integration

package postgres

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/skosovsky/flowy"
	"github.com/skosovsky/flowy/checkpoint"
)

type lockBarrierDB struct {
	*pgxpool.Pool

	locked  chan struct{}
	proceed chan struct{}
}

func (db *lockBarrierDB) BeginTx(ctx context.Context, opts pgx.TxOptions) (pgx.Tx, error) {
	tx, err := db.Pool.BeginTx(ctx, opts)
	if err != nil {
		return nil, err
	}
	return &lockBarrierTx{Tx: tx, db: db}, nil
}

type lockBarrierTx struct {
	pgx.Tx

	db *lockBarrierDB
}

func (tx *lockBarrierTx) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	tag, err := tx.Tx.Exec(ctx, sql, args...)
	if err == nil && strings.Contains(sql, "pg_advisory_xact_lock") {
		close(tx.db.locked)
		select {
		case <-tx.db.proceed:
		case <-ctx.Done():
			return tag, ctx.Err()
		}
	}
	return tag, err
}

func racePool(t *testing.T) (context.Context, *pgxpool.Pool) {
	t.Helper()
	dsn := os.Getenv("FLOWY_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("FLOWY_TEST_DATABASE_URL not set; backend race not verified")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	t.Cleanup(cancel)
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	if _, err := pool.Exec(ctx, SchemaSQL()); err != nil {
		t.Fatal(err)
	}
	return ctx, pool
}

func awaitLock(ctx context.Context, t *testing.T, barrier *lockBarrierDB) {
	t.Helper()
	select {
	case <-barrier.locked:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
}

func TestAcquireSameOwnerRace(t *testing.T) {
	for _, owner := range []string{"same-owner", "other-owner"} {
		t.Run(owner, func(t *testing.T) { assertAcquireConcurrentOwners(t, owner) })
	}
}

func assertAcquireConcurrentOwners(t *testing.T, secondOwner string) {
	t.Helper()
	// Arrange: two independent transactions, first held after acquiring the lock.
	ctx, pool := racePool(t)
	thread := testThreadID(t)
	barrier := &lockBarrierDB{Pool: pool, locked: make(chan struct{}), proceed: make(chan struct{})}
	first := mustPostgresLeaseManager(t, barrier)
	second := mustPostgresLeaseManager(t, pool)
	a, b := make(chan error, 1), make(chan error, 1)
	go func() { _, err := first.Acquire(ctx, thread, "same-owner", time.Minute); a <- err }()
	awaitLock(ctx, t, barrier)
	// Act.
	go func() { _, err := second.Acquire(ctx, thread, secondOwner, time.Minute); b <- err }()
	close(barrier.proceed)
	// Assert: SQL rejects active same-owner takeover as well as other owners.
	if err := <-a; err != nil {
		t.Fatal(err)
	}
	expected := flowy.ErrThreadLeaseBusy
	if secondOwner != "same-owner" {
		expected = flowy.ErrLeaseHeld
	}
	if err := <-b; !errors.Is(err, expected) {
		t.Fatalf("duplicate Acquire succeeded: %v", err)
	}
	if _, err := second.Acquire(ctx, thread, "other-owner", time.Minute); !errors.Is(err, flowy.ErrLeaseHeld) {
		t.Fatalf("other owner acquired: %v", err)
	}
}

func TestDeleteIfIdleAcquireRace(t *testing.T) {
	for _, firstOp := range []string{"acquire", "delete"} {
		t.Run(firstOp, func(t *testing.T) {
			assertDeleteAcquireOrder(t, firstOp)
		})
	}
}

func assertDeleteAcquireOrder(t *testing.T, firstOp string) {
	t.Helper()
	// Arrange: a checkpoint, no lease row, independent DB connections.
	ctx, pool := racePool(t)
	thread := testThreadID(t)
	cp := mustCheckpointer[intState, string](t, pool, checkpoint.JSONSerializer[intState]{})
	if _, err := cp.Save(ctx, 0, testSnapshot(thread, 1, 42)); err != nil {
		t.Fatal(err)
	}
	barrier := &lockBarrierDB{Pool: pool, locked: make(chan struct{}), proceed: make(chan struct{})}
	acquired, deleted := make(chan error, 1), make(chan error, 1)
	// Act: stop the first operation after its lock, before its decision.
	if firstOp == "acquire" {
		go func() {
			_, err := mustPostgresLeaseManager(t, barrier).Acquire(ctx, thread, "B", time.Minute)
			acquired <- err
		}()
		awaitLock(ctx, t, barrier)
		go func() { deleted <- cp.DeleteIfIdle(ctx, thread) }()
	} else {
		lockedCP := mustCheckpointer[intState, string](t, barrier, checkpoint.JSONSerializer[intState]{})
		go func() { deleted <- lockedCP.DeleteIfIdle(ctx, thread) }()
		awaitLock(ctx, t, barrier)
		go func() {
			_, err := mustPostgresLeaseManager(t, pool).Acquire(ctx, thread, "B", time.Minute)
			acquired <- err
		}()
	}
	close(barrier.proceed)
	acquireErr, deleteErr := <-acquired, <-deleted
	// Assert: either deletion linearizes first, or active B blocks deletion.
	if acquireErr != nil {
		t.Fatalf("Acquire: %v", acquireErr)
	}
	snap, _, loadErr := cp.Load(ctx, thread)
	if firstOp == "acquire" {
		if !errors.Is(deleteErr, flowy.ErrThreadLeaseBusy) || loadErr != nil || snap.State.Value != 42 {
			t.Fatalf("active checkpoint lost: delete=%v snapshot=%+v load=%v", deleteErr, snap, loadErr)
		}
		return
	}
	if deleteErr != nil {
		t.Fatal(deleteErr)
	}
	if !errors.Is(loadErr, flowy.ErrThreadNotFound) {
		t.Fatalf("expected deletion before Acquire: %v", loadErr)
	}
}
