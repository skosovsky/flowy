//go:build integration

package postgres

import (
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/skosovsky/flowy"
)

func TestHistoricalInspectionPersistentExactPrunedNeverLatest(t *testing.T) {
	// Arrange: source revision one precedes an armed latest revision two.
	ctx, pool := racePool(t)
	if _, err := pool.Exec(ctx, ExecutionSchemaSQL()); err != nil {
		t.Fatal(err)
	}
	store, err := NewWaitExecutionStore(pool, postgresWaitProfile())
	if err != nil {
		t.Fatal(err)
	}
	id := testThreadID(t)
	spec := postgresWaitSpec(time.Date(2026, 10, 5, 0, 0, 0, 0, time.UTC))
	var nodes atomic.Int32
	if _, err = postgresWaitRunner(t, store, spec, &nodes).Start(ctx, id, intState{}); err != nil {
		t.Fatal(err)
	}
	source, err := store.LoadCheckpoint(ctx, id, 1)
	if err != nil {
		t.Fatal(err)
	}
	latest, err := store.LoadExecution(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	pool.Close()
	restartCtx, restartPool := racePool(t)
	restarted, err := NewWaitExecutionStore(restartPool, postgresWaitProfile())
	if err != nil {
		t.Fatal(err)
	}
	lease, err := restarted.AcquireExecution(restartCtx, id, "inspection-active-owner", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	ref := flowy.HistoricalCheckpointReference{ExecutionID: id, Revision: 1, Digest: source.Digest}
	// Act: exact inspection is read-only even while another owner holds the source.
	inspected, inspectErr := flowy.InspectExecutionCheckpoint(restartCtx, restarted, ref)
	if inspectErr != nil || inspected.Digest != source.Digest || inspected.Revision != 1 || nodes.Load() != 1 {
		t.Fatalf("persistent inspection acquired/decoded/latest: %+v err=%v", inspected, inspectErr)
	}
	inspected.Progress.StatePayload[0] = 'x'
	retained, retainErr := restarted.LoadCheckpoint(restartCtx, id, 1)
	if retainErr != nil || retained.Digest != source.Digest {
		t.Fatalf("inspection mutated historical bytes: %+v err=%v", retained, retainErr)
	}
	if err = restarted.ReleaseExecution(restartCtx, lease); err != nil {
		t.Fatal(err)
	}
	// Prune only this test's addressed old checkpoint, never a shared execution.
	tag, err := restartPool.Exec(
		restartCtx,
		"DELETE FROM flowy_execution_history WHERE execution_id=$1 AND revision=$2",
		id,
		1,
	)
	if err != nil || tag.RowsAffected() != 1 {
		t.Fatalf("exact prune fixture failed: %v rows=%d", err, tag.RowsAffected())
	}
	missing, missingErr := flowy.InspectExecutionCheckpoint(restartCtx, restarted, ref)
	after, afterErr := restarted.LoadExecution(restartCtx, id)
	// Assert: absence is typed and does not substitute or damage latest.
	if !errors.Is(missingErr, flowy.ErrExecutionCheckpointUnavailable) ||
		!errors.Is(missingErr, flowy.ErrThreadNotFound) ||
		missing.ExecutionID != "" ||
		afterErr != nil ||
		after.Digest != latest.Digest ||
		after.Revision != latest.Revision ||
		nodes.Load() != 1 {
		t.Fatalf(
			"pruned inspection fell back/changed source: missing=%+v/%v latest=%+v/%v",
			missing,
			missingErr,
			after,
			afterErr,
		)
	}
}
