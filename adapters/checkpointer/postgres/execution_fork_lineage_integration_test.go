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

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/skosovsky/flowy"
)

func TestIntegrationForkPersistentRecoveryRejectsResealedLineage(t *testing.T) {
	for _, kind := range []string{"erase", "mode", "source", "label", "time"} {
		t.Run(kind, func(t *testing.T) {
			// Arrange: change stored payload and its seal, but not immutable creation metadata.
			ctx, pool := racePool(t)
			store := NewExecutionStore(pool)
			var nodes, live atomic.Int32
			before := createForkLineageFixture(ctx, t, pool, store, testThreadID(t), &nodes, &live)
			candidate := before
			switch kind {
			case "erase":
				candidate.Fork = nil
			case "mode":
				candidate.Fork.Mode, candidate.Fork.ProjectionLabel = flowy.ForkLive, "invented"
			case "source":
				candidate.Fork.Source.Digest = strings.Repeat("0", 64)
			case "label":
				candidate.Fork.TransformLabel = "invented"
			case "time":
				candidate.Fork.CreatedAt = candidate.Fork.CreatedAt.Add(time.Hour)
			}
			sealed, err := flowy.SealExecutionEnvelope(candidate)
			if err != nil {
				t.Fatal(err)
			}
			payload, err := json.Marshal(sealed)
			if err != nil {
				t.Fatal(err)
			}
			if _, err = pool.Exec(
				ctx,
				"UPDATE flowy_execution_history SET payload=$1::jsonb WHERE execution_id=$2 AND revision=$3",
				payload,
				before.ExecutionID,
				before.Revision,
			); err != nil {
				t.Fatal(err)
			}
			pool.Close()
			// Act: recover through a fresh pool, with executable fake policy present.
			recoveryCtx, recoveryPool := reopenPool(t, pool)
			recovered := NewExecutionStore(recoveryPool)
			_, loadErr := recovered.LoadExecution(recoveryCtx, before.ExecutionID)
			_, exactErr := recovered.LoadCheckpoint(recoveryCtx, before.ExecutionID, before.Revision)
			policy := &flowy.ForkExecutionPolicy{
				Label:        "fake",
				Mode:         flowy.ForkFake,
				FakeActivity: func(context.Context, flowy.ActivityInvocation) ([]byte, error) { return []byte("fake"), nil },
			}
			_, resumeErr := pgForkRunner(t, recovered, policy, &nodes, &live).Resume(recoveryCtx,
				flowy.ResumeToken{ThreadID: before.ExecutionID, SnapshotRevision: before.Revision})
			// Assert: corruption is detected before any node or activity callback.
			if !errors.Is(loadErr, flowy.ErrExecutionCorrupt) || !errors.Is(exactErr, flowy.ErrExecutionCorrupt) ||
				!errors.Is(resumeErr, flowy.ErrExecutionCorrupt) || nodes.Load() != 0 || live.Load() != 0 {
				t.Fatalf("resealed lineage accepted: load=%v exact=%v resume=%v nodes/live=%d/%d",
					loadErr, exactErr, resumeErr, nodes.Load(), live.Load())
			}
		})
	}
}

func createForkLineageFixture(ctx context.Context, t *testing.T, pool *pgxpool.Pool, store *ExecutionStore,
	base string, nodes, live *atomic.Int32,
) flowy.ExecutionEnvelope {
	t.Helper()
	if _, err := pool.Exec(ctx, ExecutionSchemaSQL()); err != nil {
		t.Fatal(err)
	}
	source, _ := seedPersistentForkSource(ctx, t, store, base+"source")
	request := flowy.ForkRequest{
		Source: flowy.HistoricalCheckpointReference{
			ExecutionID: source.ExecutionID, Revision: source.Revision, Digest: source.Digest,
		},
		TargetID: base + "target", Transform: flowy.ForkTransform{Label: "copy", Source: source.Descriptor,
			Transform: func(state flowy.MigrationState) (flowy.MigrationState, error) { return state, nil }},
	}
	if _, err := pgForkRunner(t, store, nil, nodes, live).Fork(ctx, request); err != nil {
		t.Fatal(err)
	}
	before, err := store.LoadExecution(ctx, request.TargetID)
	if err != nil {
		t.Fatal(err)
	}
	return before
}

func TestIntegrationForkPersistentLineageCannotBeRewrittenUnderValidFence(t *testing.T) {
	for name, mutate := range map[string]func(*flowy.ExecutionEnvelope){
		"erase": func(e *flowy.ExecutionEnvelope) { e.Fork = nil },
		"live mode": func(e *flowy.ExecutionEnvelope) {
			e.Fork.Mode = flowy.ForkLive
			e.Fork.ProjectionLabel = "invented"
		},
		"source digest": func(e *flowy.ExecutionEnvelope) { e.Fork.Source.Digest = strings.Repeat("0", 64) },
		"transform":     func(e *flowy.ExecutionEnvelope) { e.Fork.TransformLabel = "substituted" },
		"created time":  func(e *flowy.ExecutionEnvelope) { e.Fork.CreatedAt = e.Fork.CreatedAt.Add(time.Hour) },
	} {
		t.Run(name, func(t *testing.T) {
			// Arrange: create fork, close its pool, acquire a genuine successor lease.
			ctx, pool := racePool(t)
			store := NewExecutionStore(pool)
			base := testThreadID(t)
			var nodes, live atomic.Int32
			before := createForkLineageFixture(ctx, t, pool, store, base, &nodes, &live)
			pool.Close()
			recoveryCtx, recoveryPool := reopenPool(t, pool)
			recovered := NewExecutionStore(recoveryPool)
			candidate, err := recovered.LoadExecution(recoveryCtx, before.ExecutionID)
			if err != nil {
				t.Fatal(err)
			}
			lease, err := recovered.AcquireExecution(recoveryCtx, before.ExecutionID, "writer", time.Minute)
			if err != nil {
				t.Fatal(err)
			}
			// Act: attempt a provenance rewrite with a correct revision and live fence.
			mutate(&candidate)
			_, commitErr := recovered.CommitExecution(recoveryCtx, before.Revision, lease, candidate)
			after, loadErr := recovered.LoadExecution(recoveryCtx, before.ExecutionID)
			var historyCount int
			countErr := recoveryPool.QueryRow(
				recoveryCtx,
				"SELECT count(*) FROM flowy_execution_history WHERE execution_id=$1",
				before.ExecutionID,
			).Scan(&historyCount)
			// Assert: rejected write publishes neither a head nor an extra history row.
			if !errors.Is(commitErr, flowy.ErrExecutionCorrupt) || loadErr != nil || after.Digest != before.Digest ||
				after.Revision != before.Revision || countErr != nil || historyCount != 1 || nodes.Load() != 0 || live.Load() != 0 {
				t.Fatalf("rewrite accepted: commit=%v load=%v count=%d/%v", commitErr, loadErr, historyCount, countErr)
			}
			if err = recovered.ReleaseExecution(recoveryCtx, lease); err != nil {
				t.Fatal(err)
			}
		})
	}
}
