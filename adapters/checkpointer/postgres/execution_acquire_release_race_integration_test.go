//go:build integration

package postgres

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/skosovsky/flowy"
)

type executionHolderBarrierDB struct {
	DB

	reading chan struct{}
	proceed chan struct{}
}

func (db *executionHolderBarrierDB) BeginTx(ctx context.Context, options pgx.TxOptions) (pgx.Tx, error) {
	tx, err := db.DB.BeginTx(ctx, options)
	if err != nil {
		return nil, err
	}
	return &executionHolderBarrierTx{Tx: tx, barrier: db}, nil
}

type executionHolderBarrierTx struct {
	pgx.Tx

	barrier *executionHolderBarrierDB
}

func (tx *executionHolderBarrierTx) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	if strings.HasPrefix(strings.TrimSpace(sql), "SELECT") && strings.Contains(sql, "lease_owner") {
		close(tx.barrier.reading)
		select {
		case <-tx.barrier.proceed:
		case <-ctx.Done():
		}
	}
	return tx.Tx.QueryRow(ctx, sql, args...)
}

func TestIntegrationExecutionAcquireRefusedThenOwnerReleasedReturnsTypedContention(t *testing.T) {
	// Arrange: deterministically release between refused UPDATE and holder inspection.
	ctx, pool := racePool(t)
	if _, err := pool.Exec(ctx, ExecutionSchemaSQL()); err != nil {
		t.Fatal(err)
	}
	store := NewExecutionStore(pool)
	id := testThreadID(t)
	lease, err := store.AcquireExecution(ctx, id, "active-owner", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	barrier := &executionHolderBarrierDB{DB: pool, reading: make(chan struct{}), proceed: make(chan struct{})}
	defer func() {
		select {
		case <-barrier.proceed:
		default:
			close(barrier.proceed)
		}
	}()
	contender := NewExecutionStore(barrier)
	type outcome struct {
		lease flowy.ExecutionLease
		err   error
	}
	done := make(chan outcome, 1)
	go func() {
		got, acquireErr := contender.AcquireExecution(ctx, id, "contender", time.Minute)
		done <- outcome{lease: got, err: acquireErr}
	}()
	select {
	case <-barrier.reading:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	// Act: the read now observes a NULL owner, without fabricating acquisition.
	if err = store.ReleaseExecution(ctx, lease); err != nil {
		t.Fatal(err)
	}
	close(barrier.proceed)
	var got outcome
	select {
	case got = <-done:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	// Assert: no scan error, no lease, and the next genuine acquisition advances fencing.
	if !errors.Is(got.err, flowy.ErrLeaseHeld) || got.lease != (flowy.ExecutionLease{}) {
		t.Fatalf("released owner confused failed acquisition: %+v", got)
	}
	fresh, err := store.AcquireExecution(ctx, id, "contender", time.Minute)
	if err != nil || fresh.Incarnation != lease.Incarnation+1 {
		t.Fatalf("retry lost fencing: %+v err=%v", fresh, err)
	}
	if err = store.ReleaseExecution(ctx, fresh); err != nil {
		t.Fatal(err)
	}
}
