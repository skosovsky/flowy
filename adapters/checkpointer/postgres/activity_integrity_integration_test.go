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

func TestIntegrationActivityJournalPersistentRejectionBeforeExecution(t *testing.T) {
	// Arrange: content seal is valid, but the journal is semantically invalid.
	ctx, pool := racePool(t)
	if _, err := pool.Exec(ctx, ExecutionSchemaSQL()); err != nil {
		t.Fatal(err)
	}
	id := testThreadID(t)
	store := NewExecutionStore(pool)
	lease, err := store.AcquireExecution(ctx, id, "seed", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	descriptor := flowy.ExecutionDescriptor{
		GraphID: "journal-test", GraphRevision: "current", StateCodec: "json-state", ExecutionContract: "sync",
		ReplayPolicy: flowy.StepReplayPolicy{Label: "safe", Mode: flowy.StepReplaySafe},
	}
	source, err := store.CommitExecution(ctx, 0, lease, flowy.ExecutionEnvelope{
		ExecutionID: id, Descriptor: descriptor,
		Progress:       flowy.MigrationState{ExecutionPointer: "node", StatePayload: []byte("not-decodable")},
		JournalPayload: []byte("null"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if releaseErr := store.ReleaseExecution(ctx, lease); releaseErr != nil {
		t.Fatal(releaseErr)
	}
	pool.Close()
	// A new pool/handle must reject the persistent source without trying its invalid state codec.
	restartCtx, restartedPool := reopenPool(t, pool)
	var calls atomic.Int32
	builder := flowy.NewGraph[intState, flowy.NoEffect](func(_, update intState) intState { return update })
	builder.AddNode("node", func(_ context.Context, state intState) (intState, flowy.Directive, error) {
		calls.Add(1)
		return state, flowy.End(), nil
	}).AllowNoOutgoingRoute("node").SetEntryPoint("node")
	graph, err := builder.Compile()
	if err != nil {
		t.Fatal(err)
	}
	runner, err := flowy.NewDurableRunner(graph, NewExecutionStore(restartedPool), descriptor,
		checkpoint.JSONSerializer[intState]{}, checkpoint.JSONSerializer[[]flowy.NoEffect]{},
		flowy.DurableOptions{Owner: "worker", LeaseTTL: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	token := flowy.ResumeToken{ThreadID: id, SnapshotRevision: source.Revision}
	// Act.
	_, resumeErr := runner.Resume(restartCtx, token)
	_, streamErr := runner.ResumeStream(restartCtx, token)
	// Assert: both forms return journal corruption, not a state decode error.
	if !errors.Is(resumeErr, flowy.ErrExecutionCorrupt) || !errors.Is(streamErr, flowy.ErrExecutionCorrupt) ||
		calls.Load() != 0 {
		t.Fatalf("invalid journal executed: resume=%v stream=%v calls=%d", resumeErr, streamErr, calls.Load())
	}
	latest, err := NewExecutionStore(restartedPool).LoadExecution(restartCtx, id)
	if err != nil || latest.Revision != source.Revision || latest.Digest != source.Digest {
		t.Fatalf("rejection changed source: %+v %v", latest, err)
	}
}
