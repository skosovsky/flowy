package postgres

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/skosovsky/flowy"
)

type ttlQueryProbe struct {
	DB

	ttls []float64
}

func (db *ttlQueryProbe) QueryRow(_ context.Context, _ string, args ...any) pgx.Row {
	named, _ := args[0].(pgx.NamedArgs)
	if ttl, ok := named["ttl"].(float64); ok {
		db.ttls = append(db.ttls, ttl)
		return fakeRow{err: pgx.ErrNoRows}
	}
	return fakeRow{values: []any{""}}
}

func (db *ttlQueryProbe) BeginTx(context.Context, pgx.TxOptions) (pgx.Tx, error) {
	return &fakeTx{db: db}, nil
}

func (*ttlQueryProbe) Exec(context.Context, string, ...any) (pgconn.CommandTag, error) {
	return pgconn.NewCommandTag("INSERT 1"), nil
}

func TestExecutionLeaseFractionalTTLRoundedBeforeSQL(t *testing.T) {
	t.Parallel()
	// Arrange: SQL arguments remain observable even when no lease row is acquired.
	db := &ttlQueryProbe{}
	store := mustExecutionStore(t, db)
	// Act.
	_, acquireErr := store.AcquireExecution(
		context.Background(),
		"fraction",
		"worker",
		time.Microsecond+time.Nanosecond,
	)
	_, renewErr := store.RenewExecution(context.Background(),
		flowy.ExecutionLease{ExecutionID: "fraction", Owner: "worker", Incarnation: 1},
		time.Microsecond+time.Nanosecond)
	// Assert.
	if !errors.Is(acquireErr, flowy.ErrLeaseHeld) || !errors.Is(renewErr, flowy.ErrLeaseLost) ||
		len(db.ttls) != 2 || db.ttls[0] != 0.000002 || db.ttls[1] != 0.000002 {
		t.Fatalf("acquire=%v renew=%v ttl=%v", acquireErr, renewErr, db.ttls)
	}
}
