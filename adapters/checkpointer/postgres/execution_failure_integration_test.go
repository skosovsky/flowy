//go:build integration

package postgres

import (
	"context"
	"errors"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/skosovsky/flowy"
	"github.com/skosovsky/flowy/checkpoint"
)

func TestTerminalFailurePersistentRestart(t *testing.T) {
	// Arrange: the failed terminal is persisted independently of Go error identity.
	ctx, pool := racePool(t)
	if _, err := pool.Exec(ctx, ExecutionSchemaSQL()); err != nil {
		t.Fatal(err)
	}
	id := testThreadID(t)
	var calls atomic.Int32
	builder := flowy.NewGraph[intState, flowy.NoEffect](func(_, update intState) intState { return update })
	builder.AddNode("node", func(_ context.Context, state intState) (intState, flowy.Directive, error) {
		calls.Add(1)
		return state, flowy.End(), errors.New("definitive host failure")
	}).AllowNoOutgoingRoute("node").SetEntryPoint("node")
	graph, err := builder.Compile()
	if err != nil {
		t.Fatal(err)
	}
	descriptor := flowy.ExecutionDescriptor{
		GraphID:       "failure-test",
		GraphRevision: "current",
		StateCodec:    "json-state", EffectsCodec: "host-effects-v1",
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
	// Act: close all original connections before replaying the terminal outcome.
	live, liveErr := bind(NewExecutionStore(pool)).Start(ctx, id, intState{})
	if live == nil || liveErr == nil {
		t.Fatalf("live failure missing: %+v %v", live, liveErr)
	}
	pool.Close()
	restartedPool, err := pgxpool.New(ctx, os.Getenv("FLOWY_TEST_DATABASE_URL"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(restartedPool.Close)
	store := NewExecutionStore(restartedPool)
	handle, err := bind(store).ResumeStream(ctx, live.ResumeToken)
	if err != nil {
		t.Fatal(err)
	}
	events := 0
	for event := range handle.Events() {
		if event.Type == flowy.EventFailed {
			events++
			if !errors.Is(event.Error, flowy.ErrExecutionFailed) {
				t.Fatalf("replayed event lost error: %+v", event)
			}
		}
	}
	replayed, replayErr := handle.WaitResult()
	// Assert: no node redispatch, exact failure message/reason and stable token.
	if replayed == nil || replayed.Status != flowy.RunStatusFailed || replayed.Reason != live.Reason ||
		!errors.Is(replayErr, flowy.ErrExecutionFailed) ||
		replayErr.Error() != liveErr.Error() ||
		calls.Load() != 1 ||
		events != 1 ||
		replayed.ResumeToken != live.ResumeToken {
		t.Fatalf("failure restart: %+v err=%v calls=%d events=%d", replayed, replayErr, calls.Load(), events)
	}
}
