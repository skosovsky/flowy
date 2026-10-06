//go:build integration

package postgres

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/skosovsky/flowy"
)

func TestExecutionStoreRejectsCorruptPersistentAggregate(t *testing.T) {
	for kind, query := range map[string]string{
		"seal":                   `UPDATE flowy_execution_history SET payload=payload-'digest' WHERE execution_id=$1`,
		"identity":               `UPDATE flowy_execution_history SET payload=jsonb_set(payload,'{execution_id}','"other"') WHERE execution_id=$1`,
		"revision":               `UPDATE flowy_execution_history SET payload=jsonb_set(payload,'{revision}','2') WHERE execution_id=$1`,
		"scalar":                 `UPDATE flowy_execution_history SET payload='"invalid"'::jsonb WHERE execution_id=$1`,
		"missing latest history": `UPDATE flowy_executions SET revision=2 WHERE execution_id=$1`,
	} {
		t.Run(kind, func(t *testing.T) {
			// Arrange: each case changes only its own freshly created test execution.
			ctx, pool := racePool(t)
			if _, err := pool.Exec(ctx, ExecutionSchemaSQL()); err != nil {
				t.Fatal(err)
			}
			id := testThreadID(t)
			store := mustExecutionStore(t, pool)
			lease, err := store.AcquireExecution(ctx, id, "seed", time.Minute)
			if err != nil {
				t.Fatal(err)
			}
			if _, loadErr := store.LoadExecution(ctx, id); !errors.Is(loadErr, flowy.ErrThreadNotFound) {
				t.Fatalf("empty acquired head is not absence: %v", loadErr)
			}
			_, err = store.CommitExecution(ctx, 0, lease, flowy.ExecutionEnvelope{
				ExecutionID: id,
				Descriptor: flowy.ExecutionDescriptor{
					GraphID:       "g",
					GraphRevision: "r",
					StateCodec:    "s", EffectsCodec: "host-effects-v1",
					ExecutionContract: "e",
					ReplayPolicy:      flowy.StepReplayPolicy{Label: "safe", Mode: flowy.StepReplaySafe},
				},
				Progress: flowy.MigrationState{ExecutionPointer: "node"},
			})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := pool.Exec(ctx, query, id); err != nil {
				t.Fatal(err)
			}
			// Act: another handle observes the persisted corruption.
			restarted := mustExecutionStore(t, pool)
			_, latestErr := restarted.LoadExecution(ctx, id)
			// Assert: a lost latest history row must not be mistaken for a new run.
			if !errors.Is(latestErr, flowy.ErrExecutionCorrupt) {
				t.Fatalf("corruption accepted: %v", latestErr)
			}
			assertCorruptExecutionHistory(ctx, t, restarted, id, kind)
		})
	}
}

func assertCorruptExecutionHistory(ctx context.Context, t *testing.T, store *ExecutionStore, id, kind string) {
	t.Helper()
	_, exactErr := store.LoadCheckpoint(ctx, id, 1)
	want := uint64(1)
	if kind == "missing latest history" {
		want = 2
		if exactErr != nil {
			t.Fatalf("old history lost: %v", exactErr)
		}
	} else if !errors.Is(exactErr, flowy.ErrExecutionCorrupt) {
		t.Fatalf("corrupt exact checkpoint accepted: %v", exactErr)
	}
	var revision uint64
	if err := store.db.QueryRow(ctx, `SELECT revision FROM flowy_executions WHERE execution_id=$1`, id).
		Scan(&revision); err != nil {
		t.Fatal(err)
	}
	if revision != want {
		t.Fatal("read repaired/recreated the execution")
	}
}
