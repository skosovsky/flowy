//go:build integration

package postgres

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

type rebuildFaultDB struct {
	DB

	begins  int
	lostAck bool
}

func (db *rebuildFaultDB) BeginTx(ctx context.Context, options pgx.TxOptions) (pgx.Tx, error) {
	db.begins++
	if db.begins == 2 && !db.lostAck {
		return nil, errors.New("second rebuild head unavailable")
	}
	tx, err := db.DB.BeginTx(ctx, options)
	if err != nil {
		return nil, err
	}
	if db.begins == 2 {
		return discoveryFaultTx{Tx: tx, lostAck: true}, nil
	}
	return tx, nil
}

func TestDiscoveryRebuildReturnsConfirmedPartialProgress(t *testing.T) {
	for _, lostAck := range []bool{false, true} {
		t.Run(
			map[bool]string{false: "before-second-begin", true: "second-commit-lost-ack"}[lostAck],
			func(t *testing.T) {
				testDiscoveryRebuildPartial(t, lostAck)
			},
		)
	}
}

func testDiscoveryRebuildPartial(t *testing.T, lostAck bool) {
	t.Helper()
	// Arrange: two adjacent IDs needing explicit projection repair.
	ctx, pool := racePool(t)
	if _, err := pool.Exec(ctx, ExecutionSchemaSQL()); err != nil {
		t.Fatal(err)
	}
	profile := postgresWaitProfile()
	profile.Label = testThreadID(t)
	store, err := NewWaitExecutionStore(pool, profile)
	if err != nil {
		t.Fatal(err)
	}
	base := testThreadID(t)
	var calls atomic.Int32
	for _, suffix := range []string{"a", "b"} {
		if _, err = postgresWaitRunner(t, store, postgresWaitSpec(time.Now().UTC().Add(time.Hour)),
			&calls).Start(ctx, base+suffix, intState{}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err = pool.Exec(ctx, `UPDATE flowy_executions SET discovery_projected=false
 WHERE execution_id=$1 OR execution_id=$2`, base+"a", base+"b"); err != nil {
		t.Fatal(err)
	}
	fault := mustExecutionStore(t, &rebuildFaultDB{DB: pool, lostAck: lostAck})
	// Act: interrupt the second head; resume from the last confirmed cursor.
	partial, faultErr := fault.RebuildDiscovery(ctx, base, 2)
	retry, retryErr := store.RebuildDiscovery(ctx, partial.AfterExecutionID, 1)
	var projected bool
	queryErr := pool.QueryRow(ctx, `SELECT discovery_projected FROM flowy_executions
 WHERE execution_id=$1`, base+"b").Scan(&projected)
	// Assert: an unknown second ACK is not counted; retry safely reprocesses it.
	if faultErr == nil || partial.Processed != 1 || partial.AfterExecutionID != base+"a" || !partial.More ||
		retryErr != nil || retry.Processed != 1 || retry.AfterExecutionID != base+"b" ||
		queryErr != nil || !projected || calls.Load() != 2 {
		t.Fatalf("partial=%+v/%v retry=%+v/%v projected=%v/%v calls=%d",
			partial, faultErr, retry, retryErr, projected, queryErr, calls.Load())
	}
}
