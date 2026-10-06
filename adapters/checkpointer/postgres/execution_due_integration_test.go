//go:build integration

package postgres

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/skosovsky/flowy"
)

func TestIndexedDiscoveryCorruptionPaginationRebuildAndRetention(t *testing.T) {
	// Arrange: equal-deadline work with one corrupted authoritative aggregate.
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
	deadline := time.Date(2026, 10, 6, 0, 0, 0, 0, time.UTC)
	var calls atomic.Int32
	ids := []string{base + "a", base + "b", base + "c"}
	for _, id := range ids {
		if _, err = postgresWaitRunner(
			t,
			store,
			postgresWaitSpec(deadline),
			&calls,
		).Start(ctx, id, intState{}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err = pool.Exec(ctx, `UPDATE flowy_execution_history SET payload=jsonb_set(payload,'{digest}','"corrupt"')
 WHERE execution_id=$1 AND revision=(SELECT revision FROM flowy_executions WHERE execution_id=$1)`, ids[0]); err != nil {
		t.Fatal(err)
	}
	// Act: a corrupt first candidate advances the cursor instead of hiding siblings.
	first, err := store.DiscoverDueWaits(ctx, deadline, DiscoveryCursor{}, 1)
	if err != nil {
		t.Fatal(err)
	}
	second, err := store.DiscoverDueWaits(ctx, deadline, first.Cursor, 1)
	if err != nil {
		t.Fatal(err)
	}
	third, err := store.DiscoverDueWaits(ctx, deadline, second.Cursor, 1)
	// Assert: tied deadlines remain addressable and discovery invokes no nodes.
	if err != nil {
		t.Fatal(err)
	}
	assertIndexedDiscoveryPages(t, first, second, third, ids, calls.Load())

	if _, err = store.DiscoverDueWaits(
		ctx,
		deadline.Add(time.Second),
		first.Cursor,
		1,
	); !errors.Is(
		err,
		ErrDiscoveryCursor,
	) {
		t.Fatalf("changed cutoff accepted: %v", err)
	}
	// Arrange: simulate a pre-projection schema transition.
	if _, err = pool.Exec(
		ctx,
		`UPDATE flowy_executions SET discovery_projected=false WHERE execution_id=$1`,
		ids[0],
	); err != nil {
		t.Fatal(err)
	}
	if _, err = store.DiscoverDueWaits(
		ctx,
		deadline,
		DiscoveryCursor{},
		1,
	); !errors.Is(
		err,
		ErrDiscoveryRebuildRequired,
	) {
		t.Fatalf("implicit scan fallback: %v", err)
	}
	// Act: explicit rebuild quarantines corruption without rewriting payload.
	repair, err := store.RebuildDiscovery(ctx, base, 1)
	if err != nil || repair.Rebuilt != 1 || len(repair.Diagnostics) != 1 ||
		repair.Diagnostics[0].ExecutionID != ids[0] {
		t.Fatalf("repair: %+v %v", repair, err)
	}
	healthy, err := store.DiscoverDueWaits(ctx, deadline, DiscoveryCursor{}, 2)
	if err != nil || len(healthy.Waits) != 2 || len(healthy.Diagnostics) != 0 || healthy.More {
		t.Fatalf("healthy after quarantine: %+v %v", healthy, err)
	}
	if _, err = store.LoadExecution(ctx, ids[0]); !errors.Is(err, flowy.ErrExecutionCorrupt) {
		t.Fatalf("quarantine silently repaired source: %v", err)
	}
	assertDiscoveryUnsafeRetention(ctx, t, pool, store, ids[1])
}

func assertDiscoveryUnsafeRetention(
	ctx context.Context,
	t *testing.T,
	pool *pgxpool.Pool,
	store *ExecutionStore,
	id string,
) {
	t.Helper()
	// Active unsafe waits cannot be deleted; their derived candidates remain committed.
	envelope, err := store.LoadExecution(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	_, err = store.RetainExecution(
		ctx,
		flowy.ExecutionRetentionRequest{ExecutionID: id, Revision: envelope.Revision,
			Policy: flowy.ExecutionRetentionPolicy{Label: "unsafe-delete", KeepLast: 1, DeletePayload: true}},
	)
	if err == nil {
		t.Fatal("unsafe retention accepted")
	}
	var retained int
	if err = pool.QueryRow(ctx, `SELECT count(*) FROM flowy_due_candidates WHERE execution_id=$1`, id).
		Scan(&retained); err != nil ||
		retained != 1 {
		t.Fatalf("retention removed actionable work: %d %v", retained, err)
	}
}

func assertIndexedDiscoveryPages(t *testing.T, first, second, third WaitScanPage, ids []string, calls int32) {
	t.Helper()
	if len(first.Diagnostics) != 1 || first.Diagnostics[0].ExecutionID != ids[0] || len(first.Waits) != 0 ||
		!first.More || len(second.Waits) != 1 || second.Waits[0].Wait.ExecutionID != ids[1] || !second.More ||
		len(third.Waits) != 1 || third.Waits[0].Wait.ExecutionID != ids[2] ||
		third.More ||
		calls != 3 {
		t.Fatalf(
			"indexed pages: first=%+v second=%+v third=%+v calls=%d",
			first,
			second,
			third,
			calls,
		)
	}
}
