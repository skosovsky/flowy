//go:build integration

package postgres

import (
	"encoding/json"
	"sync/atomic"
	"testing"
	"time"

	"github.com/skosovsky/flowy"
)

func TestDiscoveryRejectsStaleOrForgedProjection(t *testing.T) {
	cases := []struct {
		name, sql string
		reason    error
	}{
		{
			"negative-revision",
			`UPDATE flowy_due_candidates SET revision=-1 WHERE execution_id=$1`,
			ErrDiscoveryProjectionMismatch,
		},
		{
			"revision",
			`UPDATE flowy_due_candidates SET revision=revision+1 WHERE execution_id=$1`,
			ErrDiscoveryStaleRevision,
		},
		{
			"identity",
			`UPDATE flowy_due_candidates SET work_identity='invented' WHERE execution_id=$1`,
			ErrDiscoveryProjectionMismatch,
		},
		{
			"deadline",
			`UPDATE flowy_due_candidates SET deadline=deadline-interval '1 hour' WHERE execution_id=$1`,
			ErrDiscoveryProjectionMismatch,
		},
		{"profile", "", ErrDiscoveryProfileMismatch},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assertDiscoveryRejectedProjection(t, tc.sql, tc.reason)
		})
	}
}

func assertDiscoveryRejectedProjection(t *testing.T, query string, reason error) {
	t.Helper()
	// Arrange: a valid armed aggregate; only the projection or persisted profile changes.
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
	id := testThreadID(t)
	deadline := time.Date(2026, 10, 6, 4, 0, 0, 0, time.UTC)
	var calls atomic.Int32
	if _, err = postgresWaitRunner(
		t,
		store,
		postgresWaitSpec(deadline),
		&calls,
	).Start(ctx, id, intState{}); err != nil {
		t.Fatal(err)
	}
	if query != "" {
		if _, err = pool.Exec(ctx, query, id); err != nil {
			t.Fatal(err)
		}
	} else {
		replaceDiscoveryFixtureProfile(t, store, id)
	}
	// Act: observations revalidate authority, never adopt the projection as work.
	page, err := store.DiscoverDueWaits(ctx, deadline, DiscoveryCursor{}, 1)
	// Assert: the healthy aggregate is not mislabeled corrupt or executed.
	if err != nil || len(page.Waits) != 0 || len(page.Diagnostics) != 1 || page.Diagnostics[0].ExecutionID != id ||
		page.Diagnostics[0].Reason != reason.Error() ||
		calls.Load() != 1 ||
		page.Cursor.AfterExecutionID != id {
		t.Fatalf("forged projection became actionable: %+v err=%v calls=%d", page, err, calls.Load())
	}
}

func replaceDiscoveryFixtureProfile(t *testing.T, store *ExecutionStore, id string) {
	t.Helper()
	ctx := t.Context()
	envelope, err := store.LoadExecution(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	profile := *envelope.RuntimeProfile
	profile.Label += "-changed"
	envelope.RuntimeProfile = &profile
	var records map[string]flowy.DurableWaitRecord
	if err = json.Unmarshal(envelope.WaitsPayload, &records); err != nil {
		t.Fatal(err)
	}
	for key, record := range records {
		record.Profile = profile
		records[key] = record
	}
	envelope.WaitsPayload, err = json.Marshal(records)
	if err != nil {
		t.Fatal(err)
	}
	sealed, err := flowy.SealExecutionEnvelope(envelope)
	if err != nil {
		t.Fatal(err)
	}
	payload, err := json.Marshal(sealed)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = store.db.Exec(
		ctx,
		`UPDATE flowy_execution_history SET payload=$1::jsonb WHERE execution_id=$2 AND revision=$3`,
		payload,
		id,
		sealed.Revision,
	); err != nil {
		t.Fatal(err)
	}
}

func TestDiscoveryTerminalCancellationAndRetentionRemoveWork(t *testing.T) {
	// Arrange: one actual armed candidate.
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
	id := testThreadID(t)
	deadline := time.Date(2026, 10, 6, 5, 0, 0, 0, time.UTC)
	var calls atomic.Int32
	runner := postgresWaitRunner(t, store, postgresWaitSpec(deadline), &calls)
	armed, err := runner.Start(ctx, id, intState{})
	if err != nil {
		t.Fatal(err)
	}
	page, err := store.DiscoverDueWaits(ctx, deadline, DiscoveryCursor{}, 1)
	if err != nil || len(page.Waits) != 1 {
		t.Fatalf("initial work missing: %+v %v", page, err)
	}
	// Act: cancellation terminal and payload deletion each publish an empty projection.
	token, err := runner.CancelWait(
		ctx,
		armed.ResumeToken,
		flowy.WaitCancellation{
			Generation: page.Waits[0].Wait.Generation,
			ID:         "cancel",
			Reason:     "host request",
			Evidence:   "host-authority",
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	empty, err := store.DiscoverDueWaits(ctx, deadline, DiscoveryCursor{}, 1)
	if err != nil || len(empty.Waits) != 0 || len(empty.Diagnostics) != 0 {
		t.Fatalf("terminal candidate survived: %+v %v", empty, err)
	}
	_, err = store.RetainExecution(
		ctx,
		flowy.ExecutionRetentionRequest{
			ExecutionID: id,
			Revision:    token.SnapshotRevision,
			Policy:      flowy.ExecutionRetentionPolicy{Label: "cleanup", DeletePayload: true},
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	var count int
	err = pool.QueryRow(ctx, `SELECT count(*) FROM flowy_due_candidates WHERE execution_id=$1`, id).Scan(&count)
	// Assert: no dangling work and no node execution during cancellation/maintenance.
	if err != nil || count != 0 || calls.Load() != 1 {
		t.Fatalf("dangling work=%d err=%v calls=%d", count, err, calls.Load())
	}
}
