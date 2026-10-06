//go:build integration

package postgres

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/skosovsky/flowy"
	"github.com/skosovsky/flowy/checkpoint"
)

func TestSnapshotAndOutboxWriteRejectStaleLease(t *testing.T) {
	// Arrange: takeover without a checkpoint advance makes OCC alone insufficient.
	ctx, pool := racePool(t)
	id := testThreadID(t)
	cp := mustCheckpointer[intState, string](t, pool, checkpoint.JSONSerializer[intState]{})
	manager := mustPostgresLeaseManager(t, pool)
	if _, err := cp.Save(ctx, 0, testSnapshot(id, 1, 1)); err != nil {
		t.Fatal(err)
	}
	old, err := manager.Acquire(ctx, id, "same-owner", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if _, expireErr := pool.Exec(
		ctx,
		"UPDATE flowy_leases SET expires_at = clock_timestamp() - interval '1 second' WHERE thread_id = $1",
		id,
	); expireErr != nil {
		t.Fatal(expireErr)
	}
	current, err := manager.Acquire(ctx, id, "same-owner", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	enqueues := 0
	enqueue := func(context.Context, flowy.TransactionHandle, uint64) error { enqueues++; return nil }
	// Act.
	for _, writeCtx := range []context.Context{flowy.WithExecutionLease(ctx, old), ctx} {
		if _, saveErr := cp.Save(writeCtx, 1, testSnapshot(id, 2, 2)); !errors.Is(saveErr, flowy.ErrLeaseLost) {
			t.Fatalf("snapshot accepted: %v", saveErr)
		}
		if _, writeErr := cp.SaveWithOutbox(
			writeCtx,
			1,
			testSnapshot(id, 2, 2),
			enqueue,
		); !errors.Is(
			writeErr,
			flowy.ErrLeaseLost,
		) {
			t.Fatalf("outbox accepted: %v", writeErr)
		}
	}
	// Assert: no enqueue callback or checkpoint mutation occurred.
	loaded, revision, err := cp.Load(ctx, id)
	if err != nil || revision != 1 || loaded.State.Value != 1 || enqueues != 0 {
		t.Fatalf("stale mutation: %+v %d %d %v", loaded, revision, enqueues, err)
	}
	if _, err := cp.SaveWithOutbox(
		flowy.WithExecutionLease(ctx, current),
		1,
		testSnapshot(id, 2, 2),
		enqueue,
	); err != nil {
		t.Fatal(err)
	}
	if enqueues != 1 {
		t.Fatalf("current enqueue count: %d", enqueues)
	}
}

func TestOutboxExpiryBeforeCommitRollsBack(t *testing.T) {
	// Arrange: a live handle is valid when the transaction starts.
	ctx, pool := racePool(t)
	if _, schemaErr := pool.Exec(ctx, OutboxSchemaSQL()); schemaErr != nil {
		t.Fatal(schemaErr)
	}
	id := testThreadID(t)
	cp := mustCheckpointer[intState, string](t, pool, checkpoint.JSONSerializer[intState]{})
	if _, err := cp.Save(ctx, 0, testSnapshot(id, 1, 1)); err != nil {
		t.Fatal(err)
	}
	lease, err := mustPostgresLeaseManager(t, pool).Acquire(ctx, id, "worker", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	// Act: expiry during enqueue is deterministic and part of the same transaction.
	_, saveErr := cp.SaveWithOutbox(flowy.WithExecutionLease(ctx, lease), 1, testSnapshot(id, 2, 2),
		func(ctx context.Context, handle flowy.TransactionHandle, _ uint64) error {
			tx, ok := handle.(pgx.Tx)
			if !ok {
				t.Fatal("missing native transaction")
			}
			if _, enqueueErr := tx.Exec(
				ctx,
				"INSERT INTO flowy_handoff_outbox (thread_id, snapshot_revision) VALUES ($1, $2)",
				id,
				2,
			); enqueueErr != nil {
				return enqueueErr
			}
			_, updateErr := tx.Exec(
				ctx,
				"UPDATE flowy_leases SET expires_at = clock_timestamp() - interval '1 second' WHERE thread_id = $1",
				id,
			)
			return updateErr
		})
	// Assert: even the tentative snapshot and lease update are rolled back.
	if !errors.Is(saveErr, flowy.ErrLeaseLost) {
		t.Fatalf("expired transaction committed: %v", saveErr)
	}
	loaded, revision, err := cp.Load(ctx, id)
	if err != nil || revision != 1 || loaded.State.Value != 1 {
		t.Fatalf("snapshot not rolled back: %+v %d %v", loaded, revision, err)
	}
	var expires time.Time
	if err := pool.QueryRow(ctx, "SELECT expires_at FROM flowy_leases WHERE thread_id = $1", id).
		Scan(&expires); err != nil {
		t.Fatal(err)
	}
	if !expires.Equal(lease.ExpiresAt) {
		t.Fatal("tentative lease update was committed")
	}
	var messages int
	if countErr := pool.QueryRow(ctx, "SELECT count(*) FROM flowy_handoff_outbox WHERE thread_id = $1", id).
		Scan(&messages); countErr != nil {
		t.Fatal(countErr)
	}
	if messages != 0 {
		t.Fatalf("expired transaction left %d outbox messages", messages)
	}
}
