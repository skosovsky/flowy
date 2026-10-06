//go:build integration

package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/skosovsky/flowy"
)

type discoveryFaultDB struct {
	DB

	lostAck bool
}

func (db discoveryFaultDB) BeginTx(ctx context.Context, options pgx.TxOptions) (pgx.Tx, error) {
	tx, err := db.DB.BeginTx(ctx, options)
	if err != nil {
		return nil, err
	}
	return discoveryFaultTx{Tx: tx, lostAck: db.lostAck}, nil
}

type discoveryFaultTx struct {
	pgx.Tx

	lostAck bool
}

func (tx discoveryFaultTx) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	tag, err := tx.Tx.Exec(ctx, sql, args...)
	if err == nil && !tx.lostAck && strings.Contains(sql, "SET discovery_projected=true") {
		if closeErr := tx.Conn().Close(context.WithoutCancel(ctx)); closeErr != nil {
			return tag, closeErr
		}
		return tag, errors.New("connection lost after projection publication before commit")
	}
	return tag, err
}
func (tx discoveryFaultTx) Commit(ctx context.Context) error {
	err := tx.Tx.Commit(ctx)
	if err == nil && tx.lostAck {
		return errors.New("lost projection commit acknowledgement")
	}
	return err
}

func rescheduledWaitEnvelope(
	t *testing.T,
	envelope flowy.ExecutionEnvelope,
	deadline time.Time,
) flowy.ExecutionEnvelope {
	t.Helper()
	var records map[string]flowy.DurableWaitRecord
	if err := json.Unmarshal(envelope.WaitsPayload, &records); err != nil {
		t.Fatal(err)
	}
	for identity, record := range records {
		record.Spec.Deadline = deadline
		records[identity] = record
	}
	payload, err := json.Marshal(records)
	if err != nil {
		t.Fatal(err)
	}
	envelope.WaitsPayload = payload
	return envelope
}

func TestDiscoveryProjectionAtomicRollbackLostAckAndRestart(t *testing.T) {
	for _, lostAck := range []bool{false, true} {
		t.Run(map[bool]string{false: "rollback", true: "lost-ack"}[lostAck], func(t *testing.T) {
			testDiscoveryPublicationFault(t, lostAck)
		})
	}
}

func testDiscoveryPublicationFault(t *testing.T, lostAck bool) {
	t.Helper()
	// Arrange: committed future wait, then reschedule to an earlier deadline.
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
	deadline := time.Date(2026, 10, 6, 1, 0, 0, 0, time.UTC)
	id := testThreadID(t)
	var calls atomic.Int32
	if _, err = postgresWaitRunner(
		t,
		store,
		postgresWaitSpec(deadline.Add(time.Hour)),
		&calls,
	).Start(ctx, id, intState{}); err != nil {
		t.Fatal(err)
	}
	before, err := store.LoadExecution(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	lease, err := store.AcquireExecution(ctx, id, "reschedule", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	fault := NewExecutionStore(discoveryFaultDB{DB: pool, lostAck: lostAck})
	// Act: interrupt after actual projection DML or lose acknowledgement after commit.
	_, err = fault.CommitExecution(ctx, before.Revision, lease, rescheduledWaitEnvelope(t, before, deadline))
	if err == nil {
		t.Fatal("publication fault not observed")
	}
	pool.Close()

	assertDiscoveryPublicationRecovery(t, lostAck, profile, before, &calls, lease, deadline)
}

func assertDiscoveryPublicationRecovery(t *testing.T, lostAck bool, profile flowy.WaitCapabilityProfile,
	before flowy.ExecutionEnvelope, calls *atomic.Int32, lease flowy.ExecutionLease, deadline time.Time,
) {
	t.Helper()
	restartCtx, restartPool := racePool(t)
	recovered, err := NewWaitExecutionStore(restartPool, profile)
	if err != nil {
		t.Fatal(err)
	}
	current, err := recovered.LoadExecution(restartCtx, before.ExecutionID)
	if err != nil {
		t.Fatal(err)
	}
	page, err := recovered.DiscoverDueWaits(restartCtx, deadline, DiscoveryCursor{}, 1)
	// Assert: projection and source are either both old or both newly committed.
	if err != nil || len(page.Diagnostics) != 0 || calls.Load() != 1 {
		t.Fatalf("discovery=%+v/%v calls=%d", page, err, calls.Load())
	}
	if lostAck {
		if len(page.Waits) != 1 || current.Revision != before.Revision+1 || page.Waits[0].Revision != current.Revision {
			t.Fatalf("lost-ack pair incomplete: %+v head=%+v", page, current)
		}
	} else if len(page.Waits) != 0 || current.Digest != before.Digest {
		t.Fatalf("rollback split pair: %+v head=%+v", page, current)
	}
	if err = recovered.ReleaseExecution(restartCtx, lease); err != nil {
		t.Fatal(err)
	}
}

func TestDiscoveryRescheduleBehindCursorAppearsNextCycle(t *testing.T) {
	// Arrange: b is due, a is future and sorts behind the delivered cursor after rescheduling.
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
	deadline := time.Date(2026, 10, 6, 2, 0, 0, 0, time.UTC)
	var calls atomic.Int32
	for _, suffix := range []string{"a", "b", "c"} {
		at := deadline
		if suffix == "a" {
			at = at.Add(time.Hour)
		}
		if _, err = postgresWaitRunner(
			t,
			store,
			postgresWaitSpec(at),
			&calls,
		).Start(ctx, base+suffix, intState{}); err != nil {
			t.Fatal(err)
		}
	}
	first, err := store.DiscoverDueWaits(ctx, deadline, DiscoveryCursor{}, 1)
	if err != nil {
		t.Fatal(err)
	}
	// Act: publication changes a while a polling cycle is between pages.
	head, err := store.LoadExecution(ctx, base+"a")
	if err != nil {
		t.Fatal(err)
	}
	lease, err := store.AcquireExecution(ctx, base+"a", "reschedule", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = store.CommitExecution(
		ctx,
		head.Revision,
		lease,
		rescheduledWaitEnvelope(t, head, deadline),
	); err != nil {
		t.Fatal(err)
	}
	if err = store.ReleaseExecution(ctx, lease); err != nil {
		t.Fatal(err)
	}
	last, err := store.DiscoverDueWaits(ctx, deadline, first.Cursor, 1)
	if err != nil {
		t.Fatal(err)
	}
	next, err := store.DiscoverDueWaits(ctx, deadline, DiscoveryCursor{}, 3)
	// Assert: next complete cycle includes a and repeated b/c without executing them.
	if err != nil || len(first.Waits) != 1 || first.Waits[0].Wait.ExecutionID != base+"b" || !first.More ||
		len(last.Waits) != 1 || last.Waits[0].Wait.ExecutionID != base+"c" || last.More ||
		len(next.Waits) != 3 || next.Waits[0].Wait.ExecutionID != base+"a" || next.More || calls.Load() != 3 {
		t.Fatalf(
			"reschedule lost work: first=%+v last=%+v next=%+v err=%v calls=%d",
			first,
			last,
			next,
			err,
			calls.Load(),
		)
	}
}
