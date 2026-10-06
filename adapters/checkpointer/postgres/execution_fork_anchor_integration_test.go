//go:build integration

package postgres

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/skosovsky/flowy"
)

func TestForkPersistentAnchorSurvivesCreationHistoryLoss(t *testing.T) {
	// Arrange: retain a later head, remove only this fixture's initial history row.
	ctx, pool := racePool(t)
	store := NewExecutionStore(pool)
	var nodes, live atomic.Int32
	creation := createForkLineageFixture(ctx, t, pool, store, testThreadID(t), &nodes, &live)
	lease, err := store.AcquireExecution(ctx, creation.ExecutionID, "advance", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	head, err := store.CommitExecution(ctx, creation.Revision, lease, creation)
	if err != nil {
		t.Fatal(err)
	}
	if err = store.ReleaseExecution(ctx, lease); err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Exec(ctx, "DELETE FROM flowy_execution_history WHERE execution_id=$1 AND revision=$2",
		creation.ExecutionID, creation.Revision); err != nil {
		t.Fatal(err)
	}
	pool.Close()
	// Act: independent recovery uses the head anchor, not revision-one fallback.
	recoveryCtx, recoveryPool := racePool(t)
	recovered := NewExecutionStore(recoveryPool)
	loaded, loadErr := recovered.LoadExecution(recoveryCtx, head.ExecutionID)
	_, absentErr := recovered.LoadCheckpoint(recoveryCtx, head.ExecutionID, creation.Revision)
	policy := &flowy.ForkExecutionPolicy{Label: "fake", Mode: flowy.ForkFake,
		FakeActivity: func(context.Context, flowy.ActivityInvocation) ([]byte, error) { return []byte("fake"), nil }}
	result, resumeErr := pgForkRunner(t, recovered, policy, &nodes, &live).Resume(recoveryCtx,
		flowy.ResumeToken{ThreadID: head.ExecutionID, SnapshotRevision: head.Revision})
	// Assert: retained head runs only fake; exact missing history still returns absence.
	if loadErr != nil || loaded.Digest != head.Digest || loaded.Fork == nil || *loaded.Fork != *creation.Fork ||
		!errors.Is(absentErr, flowy.ErrExecutionCheckpointUnavailable) || resumeErr != nil || result == nil ||
		nodes.Load() != 1 || live.Load() != 0 {
		t.Fatalf("anchor lost with history: load=%v absent=%v resume=%v nodes/live=%d/%d",
			loadErr, absentErr, resumeErr, nodes.Load(), live.Load())
	}
}

func TestForkPersistentCorruptAnchorRejectsLoadCommitAndDiscovery(t *testing.T) {
	for name, query := range map[string]string{
		"erased": "UPDATE flowy_executions SET fork_lineage=NULL WHERE execution_id=$1",
		"scalar": "UPDATE flowy_executions SET fork_lineage='1'::jsonb WHERE execution_id=$1",
		"label":  "UPDATE flowy_executions SET fork_lineage=jsonb_set(fork_lineage,'{transform_label}','\"invented\"') WHERE execution_id=$1",
	} {
		t.Run(name, func(t *testing.T) {
			// Arrange: only the separate creation anchor is corrupted, envelope seal remains valid.
			ctx, pool := racePool(t)
			store := NewExecutionStore(pool)
			base := testThreadID(t)
			var nodes, live atomic.Int32
			before := createForkLineageFixture(ctx, t, pool, store, base, &nodes, &live)
			if _, err := pool.Exec(ctx, query, before.ExecutionID); err != nil {
				t.Fatal(err)
			}
			pool.Close()
			recoveryCtx, recoveryPool := racePool(t)
			recovered := NewExecutionStore(recoveryPool)
			lease, err := recovered.AcquireExecution(recoveryCtx, before.ExecutionID, "writer", time.Minute)
			if err != nil {
				t.Fatal(err)
			}
			// Act: every native raw boundary must refuse inconsistent origin.
			_, loadErr := recovered.LoadExecution(recoveryCtx, before.ExecutionID)
			_, exactErr := recovered.LoadCheckpoint(recoveryCtx, before.ExecutionID, before.Revision)
			_, commitErr := recovered.CommitExecution(recoveryCtx, before.Revision, lease, before)
			visited := 0
			_, scanErr := recovered.scanExecutionHeads(recoveryCtx, base+"source", 1,
				func(flowy.ExecutionEnvelope) error { visited++; return nil })
			// Assert: no callback or new head is acknowledged, and the writer can release.
			assertForkAnchorErrors(t, loadErr, exactErr, commitErr, scanErr)
			if visited != 0 || nodes.Load() != 0 || live.Load() != 0 {
				t.Fatal("corrupt anchor reached host code")
			}
			if err = recovered.ReleaseExecution(recoveryCtx, lease); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func assertForkAnchorErrors(t *testing.T, results ...error) {
	t.Helper()
	for _, err := range results {
		if !errors.Is(err, flowy.ErrExecutionCorrupt) {
			t.Fatalf("corrupt anchor accepted: %v", err)
		}
	}
}
