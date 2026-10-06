//go:build integration

package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/skosovsky/flowy"
	"github.com/skosovsky/flowy/checkpoint"
)

func TestManualActivityResolutionPersistentRestart(t *testing.T) {
	// Arrange: remote delivery is ambiguous; the operator later supplies evidence.
	ctx, pool := racePool(t)
	if _, err := pool.Exec(ctx, ExecutionSchemaSQL()); err != nil {
		t.Fatal(err)
	}
	id := testThreadID(t)
	var calls atomic.Int32
	builder := flowy.NewGraph[intState, flowy.NoEffect](func(_, update intState) intState { return update })
	builder.AddNode("node", func(ctx context.Context, state intState) (intState, flowy.Directive, error) {
		_, err := flowy.CallActivity(
			ctx,
			flowy.ActivityRequest{
				Key:            "write",
				Implementation: "stable",
				Input:          []byte("input"),
				Dispatch: func(context.Context, flowy.ActivityInvocation) ([]byte, error) {
					calls.Add(1)
					return nil, errors.New("remote outcome unknown")
				},
			},
		)
		if err != nil {
			return state, flowy.Fail("activity"), err
		}
		state.Value++
		return state, flowy.End(), nil
	}).AllowNoOutgoingRoute("node").SetEntryPoint("node")
	graph, err := builder.Compile()
	if err != nil {
		t.Fatal(err)
	}
	descriptor := flowy.ExecutionDescriptor{
		GraphID:           "manual-test",
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
	store := NewExecutionStore(pool)
	failed, startErr := bind(store).Start(ctx, id, intState{})
	if failed == nil || !errors.Is(startErr, flowy.ErrActivityUnknown) {
		t.Fatalf("unknown missing: %+v %v", failed, startErr)
	}
	source, err := store.LoadExecution(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	var journal map[string]flowy.ActivityRecord
	if decodeErr := json.Unmarshal(source.JournalPayload, &journal); decodeErr != nil {
		t.Fatal(decodeErr)
	}
	var record flowy.ActivityRecord
	for _, entry := range journal {
		record = entry
	}
	resolution := flowy.ActivityResolution{
		Identity:       record.Identity,
		InputDigest:    record.InputDigest,
		Implementation: record.Implementation,
		DecisionID:     "operator-confirmed",
		Action:         flowy.ActivityResolveComplete,
		Reason:         "confirmed downstream",
		Evidence:       "host-evidence",
		Outcome:        []byte("confirmed"),
	}
	// Act: manual commit and resumed execution each use a fresh pool.
	pool.Close()
	resolvePool, err := pgxpool.New(ctx, os.Getenv("FLOWY_TEST_DATABASE_URL"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(resolvePool.Close)
	token, err := bind(NewExecutionStore(resolvePool)).ResolveActivity(ctx, failed.ResumeToken, resolution)
	if err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 1 {
		t.Fatal("resolution dispatched")
	}
	resolvePool.Close()
	restartPool, err := pgxpool.New(ctx, os.Getenv("FLOWY_TEST_DATABASE_URL"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(restartPool.Close)
	restarted := NewExecutionStore(restartPool)
	result, err := bind(restarted).Resume(ctx, token)
	// Assert: original unknown attempt and manual evidence survive both restarts.
	if err != nil || result.State.Value != 1 || calls.Load() != 1 {
		t.Fatalf("manual recovery redispatched: %+v %v calls=%d", result, err, calls.Load())
	}
	assertStoredManualResolution(ctx, t, restarted, token, source, record.Identity, resolution)
}

func assertStoredManualResolution(
	ctx context.Context,
	t *testing.T,
	store flowy.ExecutionHistoryStore,
	token flowy.ResumeToken,
	source flowy.ExecutionEnvelope,
	identity string,
	resolution flowy.ActivityResolution,
) {
	t.Helper()
	resolved, err := store.LoadCheckpoint(ctx, token.ThreadID, token.SnapshotRevision)
	if err != nil {
		t.Fatal(err)
	}
	var journal map[string]flowy.ActivityRecord
	if decodeErr := json.Unmarshal(resolved.JournalPayload, &journal); decodeErr != nil {
		t.Fatal(decodeErr)
	}
	entry := journal[identity]
	if entry.ExecutionID != source.ExecutionID || entry.Node != source.Progress.ExecutionPointer ||
		entry.Activation != source.Activation {
		t.Fatalf("activity address lost across pools: %+v", entry)
	}
	if entry.State != flowy.ActivityCompleted || entry.Origin != flowy.ActivityManual || len(entry.Resolutions) != 1 ||
		entry.Resolutions[0].Evidence != resolution.Evidence ||
		entry.Resolutions[0].SourceRevision != source.Revision ||
		entry.Attempts[0].State != flowy.ActivityUnknown {
		t.Fatalf("manual provenance lost: %+v", entry)
	}
	retained, err := store.LoadCheckpoint(ctx, token.ThreadID, source.Revision)
	if err != nil || string(retained.JournalPayload) != string(source.JournalPayload) {
		t.Fatalf("source journal changed: %+v %v", retained, err)
	}
}
