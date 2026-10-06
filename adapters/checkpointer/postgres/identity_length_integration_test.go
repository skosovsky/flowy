//go:build integration

package postgres

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/skosovsky/flowy"
	pglease "github.com/skosovsky/flowy/adapters/lease/postgres"
	"github.com/skosovsky/flowy/checkpoint"
)

const oldIdentityLength = 255

func TestNativeCheckpointLongIdentitiesNeverAlias(t *testing.T) {
	// Arrange: fresh isolated native TEXT schema, two IDs with the same old-width prefix.
	ctx, pool := identityPool(t)
	prefix := strings.Repeat("x", oldIdentityLength)
	full, absent := prefix+"different-execution", prefix+"absent"
	node := strings.Repeat("узел", oldIdentityLength)
	cp := mustCheckpointer[intState, string](t, pool, checkpoint.JSONSerializer[intState]{})
	snapshot := testSnapshot(prefix, 1, 1)
	snapshot.ExecutionPointer = flowy.ExecutionPointer(node)
	if _, err := cp.Save(ctx, 0, snapshot); err != nil {
		t.Fatal(err)
	}
	manager := mustPostgresLeaseManager(t, pool)
	lease, err := manager.Acquire(ctx, prefix, strings.Repeat("owner", oldIdentityLength), time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = manager.Release(context.WithoutCancel(ctx), lease) }()
	// Act: an unrelated long-ID write must not use the leased prefix's revision.
	snapshot.ThreadID, snapshot.State.Value = full, 99
	enqueues := 0
	_, saveErr := cp.SaveWithOutbox(ctx, 1, snapshot,
		func(context.Context, flowy.TransactionHandle, uint64) error { enqueues++; return nil })
	// Assert: no prefix mutation or outbox publication; full ID can start its own revision.
	if !errors.Is(saveErr, flowy.ErrConcurrencyConflict) || enqueues != 0 {
		t.Fatalf("long ID used prefix revision: %v enqueue=%d", saveErr, enqueues)
	}
	assertExactNativeCheckpoint(ctx, t, cp, prefix, 1, 1, node)
	if _, err := cp.Save(ctx, 0, snapshot); err != nil {
		t.Fatal(err)
	}
	assertExactNativeCheckpoint(ctx, t, cp, full, 1, 99, node)
	assertAbsentNativeIdentity(ctx, t, cp, absent)
	assertExactNativeHistoryAndCleanup(ctx, t, cp, prefix, full, absent, node)
	assertLongIdentityLease(ctx, t, cp, manager, prefix, full, node)
}

func assertExactNativeCheckpoint(ctx context.Context, t *testing.T, cp *Checkpointer[intState, string],
	id string, revision uint64, value int, node string,
) {
	t.Helper()
	result, actualRevision, err := cp.Load(ctx, id)
	if err != nil || actualRevision != revision || result.ThreadID != id || result.State.Value != value ||
		string(result.ExecutionPointer) != node {
		t.Fatalf("identity shortened: id=%q result=%+v revision=%d err=%v", id, result, actualRevision, err)
	}
}

func assertAbsentNativeIdentity(ctx context.Context, t *testing.T, cp *Checkpointer[intState, string], id string) {
	t.Helper()
	if _, _, err := cp.Load(ctx, id); !errors.Is(err, flowy.ErrThreadNotFound) {
		t.Fatalf("absent long ID fell back to prefix: %v", err)
	}
	history, err := cp.GetHistory(ctx, id, 2)
	if err != nil || len(history) != 0 {
		t.Fatalf("absent long history aliased: %+v %v", history, err)
	}
	for _, cleanupErr := range []error{cp.Prune(ctx, id, 1), cp.Prune(ctx, id, 0),
		cp.Delete(ctx, id), cp.DeleteIfIdle(ctx, id)} {
		if cleanupErr != nil {
			t.Fatal(cleanupErr)
		}
	}
}

func assertExactNativeHistoryAndCleanup(ctx context.Context, t *testing.T,
	cp *Checkpointer[intState, string], prefix, full, absent, node string,
) {
	t.Helper()
	history, err := cp.GetHistory(ctx, full, 2)
	if err != nil || len(history) != 1 || history[0].ThreadID != full || history[0].State.Value != 99 {
		t.Fatalf("long history aliased: %+v %v", history, err)
	}
	snapshot := testSnapshot(full, 2, 100)
	snapshot.ExecutionPointer = flowy.ExecutionPointer(node)
	if _, saveErr := cp.Save(ctx, 1, snapshot); saveErr != nil {
		t.Fatal(saveErr)
	}
	assertExactNativeCheckpoint(ctx, t, cp, full, 2, 100, node)
	if pruneErr := cp.Prune(ctx, full, 1); pruneErr != nil {
		t.Fatal(pruneErr)
	}
	history, err = cp.GetHistory(ctx, full, 2)
	if err != nil || len(history) != 1 || history[0].Revision != 2 {
		t.Fatalf("long prune failed: %+v %v", history, err)
	}
	if err := cp.DeleteIfIdle(ctx, full); err != nil {
		t.Fatal(err)
	}
	assertAbsentNativeIdentity(ctx, t, cp, full)
	assertAbsentNativeIdentity(ctx, t, cp, absent)
	assertExactNativeCheckpoint(ctx, t, cp, prefix, 1, 1, node)
}

func assertLongIdentityLease(ctx context.Context, t *testing.T, cp *Checkpointer[intState, string],
	manager *pglease.LeaseManager, prefix, full, node string,
) {
	t.Helper()
	lease, err := manager.Acquire(ctx, full, strings.Repeat("владелец", oldIdentityLength), time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = manager.Release(context.WithoutCancel(ctx), lease) }()
	if _, err := manager.Renew(ctx, lease, time.Minute); err != nil {
		t.Fatal(err)
	}
	if err := cp.DeleteIfIdle(ctx, full); !errors.Is(err, flowy.ErrThreadLeaseBusy) {
		t.Fatalf("long active identity not protected: %v", err)
	}
	snapshot := testSnapshot(full, 1, 99)
	snapshot.ExecutionPointer = flowy.ExecutionPointer(node)
	if _, err := cp.Save(ctx, 0, snapshot); !errors.Is(err, flowy.ErrLeaseLost) {
		t.Fatalf("unleased long write accepted: %v", err)
	}
	if _, err := cp.Save(flowy.WithExecutionLease(ctx, lease), 0, snapshot); err != nil {
		t.Fatal(err)
	}
	assertExactNativeCheckpoint(ctx, t, cp, full, 1, 99, node)
	assertExactNativeCheckpoint(ctx, t, cp, prefix, 1, 1, node)
}

func identityPool(t *testing.T) (context.Context, *pgxpool.Pool) {
	t.Helper()
	ctx, base := racePool(t)
	schema := pgx.Identifier{fmt.Sprintf("flowy_identity_%d", time.Now().UnixNano())}.Sanitize()
	if _, err := base.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		t.Fatal(err)
	}
	config := base.Config()
	config.ConnConfig.RuntimeParams["search_path"] = schema
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		pool.Close()
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		// This exact schema was created above solely for this test invocation.
		if _, err := base.Exec(cleanupCtx, "DROP SCHEMA "+schema+" CASCADE"); err != nil {
			t.Errorf("owned fixture schema cleanup: %v", err)
		}
	})
	if _, err := pool.Exec(ctx, SchemaSQL()); err != nil {
		t.Fatal(err)
	}
	return ctx, pool
}
