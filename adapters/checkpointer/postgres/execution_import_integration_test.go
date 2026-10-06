//go:build integration

package postgres

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/skosovsky/flowy"
	"github.com/skosovsky/flowy/checkpoint"
)

func TestExplicitImportPersistentRestart(t *testing.T) {
	// Arrange: the original artifact is not a current execution envelope.
	ctx, pool := racePool(t)
	if _, err := pool.Exec(ctx, ExecutionSchemaSQL()); err != nil {
		t.Fatal(err)
	}
	id := testThreadID(t)
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
	descriptor := flowy.ExecutionDescriptor{
		GraphID:           "import-test",
		GraphRevision:     "current",
		StateCodec:        "json-state",
		ExecutionContract: "sync",
		ReplayPolicy:      flowy.StepReplayPolicy{Label: "test-safe-steps", Mode: flowy.StepReplaySafe},
	}
	bind := func(store flowy.ExecutionStore) *flowy.DurableRunner[intState, flowy.NoEffect] {
		runner, bindErr := flowy.NewDurableRunner(
			graph,
			store,
			descriptor,
			checkpoint.JSONSerializer[intState]{},
			checkpoint.JSONSerializer[[]flowy.NoEffect]{},
			flowy.DurableOptions{Owner: "worker", LeaseTTL: time.Minute},
		)
		if bindErr != nil {
			t.Fatal(bindErr)
		}
		return runner
	}
	payload := []byte("opaque source artifact")
	digest := sha256.Sum256(payload)
	source := flowy.LegacyExecutionSource{
		ID:       "old-checkpoint",
		Revision: 9,
		Format:   "host-defined",
		Payload:  payload,
		Digest:   hex.EncodeToString(digest[:]),
	}
	importer := flowy.ExecutionImporter{
		ID:     "host-import",
		Format: source.Format,
		Transform: func([]byte) (flowy.ImportedExecutionState, error) {
			return flowy.ImportedExecutionState{
				Progress:       flowy.MigrationState{ExecutionPointer: "node", StatePayload: []byte(`{"value":41}`)},
				EffectsPayload: []byte(`[]`),
			}, nil
		},
	}
	// Act: close the original pool after import and recover through new connections.
	token, err := bind(NewExecutionStore(pool)).Import(ctx, id, source, importer)
	if err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 0 {
		t.Fatal("import dispatched a node")
	}
	pool.Close()
	restartedPool, err := pgxpool.New(ctx, os.Getenv("FLOWY_TEST_DATABASE_URL"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(restartedPool.Close)
	store := NewExecutionStore(restartedPool)
	result, err := bind(store).Resume(ctx, token)
	// Assert: target resume executes once, while exact source provenance survives.
	if err != nil || result.State.Value != 42 || calls.Load() != 1 {
		t.Fatalf("restart failed: %+v %v calls=%d", result, err, calls.Load())
	}
	imported, err := store.LoadCheckpoint(ctx, id, token.SnapshotRevision)
	if err != nil || imported.Import == nil || !bytes.Equal(imported.Import.Source.Payload, payload) ||
		imported.Import.Source.Revision != source.Revision ||
		len(imported.JournalPayload) != 0 {
		t.Fatalf("import provenance lost: %+v %v", imported, err)
	}
}
