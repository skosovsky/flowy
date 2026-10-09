//go:build integration

package postgres

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/skosovsky/flowy"
	"github.com/skosovsky/flowy/checkpoint"
)

func TestIntegrationReplayPolicyPersistentMigration(t *testing.T) {
	// Arrange: all other compatibility labels match; policy change must still be explicit.
	ctx, pool := racePool(t)
	if _, err := pool.Exec(ctx, ExecutionSchemaSQL()); err != nil {
		t.Fatal(err)
	}
	id := testThreadID(t)
	store := NewExecutionStore(pool)
	old := flowy.ExecutionDescriptor{
		GraphID:           "policy-test",
		GraphRevision:     "current",
		StateCodec:        "json-state",
		ExecutionContract: "sync",
		ReplayPolicy:      flowy.StepReplayPolicy{Label: "old-safe", Mode: flowy.StepReplaySafe},
	}
	lease, err := store.AcquireExecution(ctx, id, "seed", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	source, err := store.CommitExecution(
		ctx,
		0,
		lease,
		flowy.ExecutionEnvelope{
			ExecutionID:    id,
			Descriptor:     old,
			Progress:       flowy.MigrationState{ExecutionPointer: "node", StatePayload: []byte(`{"value":41}`)},
			EffectsPayload: []byte(`[]`),
			Activation:     1,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if releaseErr := store.ReleaseExecution(ctx, lease); releaseErr != nil {
		t.Fatal(releaseErr)
	}
	var calls atomic.Int32
	builder := flowy.NewGraph[intState, flowy.NoEffect](func(_, update intState) intState { return update })
	builder.AddNode("node", func(_ context.Context, state intState) (intState, flowy.Directive, error) {
		calls.Add(1)
		state.Value++
		return state, flowy.End(), nil
	}).AllowNoOutgoingRoute("node").SetEntryPoint("node")
	graph, err := builder.Compile()
	if err != nil {
		t.Fatal(err)
	}
	target := old
	target.ReplayPolicy.Label = "current-safe"
	bind := func(migrations []flowy.ExecutionMigration) *flowy.DurableRunner[intState, flowy.NoEffect] {
		runner, bindErr := flowy.NewDurableRunner(
			graph,
			NewExecutionStore(pool),
			target,
			checkpoint.JSONSerializer[intState]{},
			checkpoint.JSONSerializer[[]flowy.NoEffect]{},
			flowy.DurableOptions{Owner: "worker", LeaseTTL: time.Minute, Migrations: migrations},
		)
		if bindErr != nil {
			t.Fatal(bindErr)
		}
		return runner
	}
	token := flowy.ResumeToken{ThreadID: id, SnapshotRevision: source.Revision}
	// Act: reject the implicit upgrade, then commit a named pure migration.
	_, rejectErr := bind(nil).Resume(ctx, token)
	if !errors.Is(rejectErr, flowy.ErrExecutionIncompatible) || calls.Load() != 0 {
		t.Fatalf("implicit policy upgrade executed: %v calls=%d", rejectErr, calls.Load())
	}
	migration := flowy.ExecutionMigration{
		ID:        "policy-change",
		Source:    old,
		Target:    target,
		Transform: func(state flowy.MigrationState) (flowy.MigrationState, error) { return state, nil },
	}
	result, err := bind([]flowy.ExecutionMigration{migration}).Resume(ctx, token)
	// Assert: exact source policy survives and target policy has its own committed lineage.
	retained, retainedErr := store.LoadCheckpoint(ctx, id, source.Revision)
	migrated, migratedErr := store.LoadCheckpoint(ctx, id, source.Revision+1)
	if err != nil || result.State.Value != 42 || calls.Load() != 1 || retainedErr != nil ||
		retained.Descriptor.ReplayPolicy != old.ReplayPolicy ||
		migratedErr != nil ||
		migrated.Descriptor.ReplayPolicy != target.ReplayPolicy ||
		migrated.Migration == nil {
		t.Fatalf(
			"policy migration lost lineage: result=%+v err=%v retained=%+v migrated=%+v",
			result,
			err,
			retained,
			migrated,
		)
	}
}
