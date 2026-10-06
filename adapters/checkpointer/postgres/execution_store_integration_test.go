//go:build integration

package postgres

import (
	"errors"
	"testing"
	"time"

	"github.com/skosovsky/flowy"
)

func TestExecutionStorePersistentMigrationAndFencing(t *testing.T) {
	// Arrange: separate store handles share durable state.
	ctx, pool := racePool(t)
	if _, err := pool.Exec(ctx, ExecutionSchemaSQL()); err != nil {
		t.Fatal(err)
	}
	id := testThreadID(t)
	store := mustExecutionStore(t, pool)
	old, err := store.AcquireExecution(ctx, id, "reused-owner", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	descriptor := flowy.ExecutionDescriptor{
		GraphID:       "g",
		GraphRevision: "old",
		StateCodec:    "raw", EffectsCodec: "host-effects-v1",
		ExecutionContract: "sync",
		ReplayPolicy:      flowy.StepReplayPolicy{Label: "test-safe-steps", Mode: flowy.StepReplaySafe},
	}
	source, err := store.CommitExecution(
		ctx,
		0,
		old,
		flowy.ExecutionEnvelope{
			ExecutionID:    id,
			Descriptor:     descriptor,
			Progress:       flowy.MigrationState{ExecutionPointer: "before", StatePayload: []byte("state")},
			JournalPayload: []byte("completed outcome"),
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if releaseErr := store.ReleaseExecution(ctx, old); releaseErr != nil {
		t.Fatal(releaseErr)
	}
	restarted := mustExecutionStore(t, pool)
	current, err := restarted.AcquireExecution(ctx, id, "reused-owner", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	target := descriptor
	target.GraphRevision = "new"
	migration := flowy.ExecutionMigration{
		ID:     "rename",
		Source: descriptor,
		Target: target,
		Transform: func(state flowy.MigrationState) (flowy.MigrationState, error) {
			state.ExecutionPointer = "after"
			return state, nil
		},
	}
	prepared, err := flowy.PrepareExecutionMigration(
		source,
		target,
		[]flowy.ExecutionMigration{migration},
		func(flowy.ExecutionPointer) error { return nil },
	)
	if err != nil {
		t.Fatal(err)
	}
	// Act.
	_, staleErr := store.CommitExecution(ctx, source.Revision, old, prepared)
	_, staleRenew := store.RenewExecution(ctx, old, time.Hour)
	staleRelease := store.ReleaseExecution(ctx, old)
	// Independent transactions contend after both callers reach the start barrier.
	type commitResult struct {
		envelope flowy.ExecutionEnvelope
		err      error
	}
	start := make(chan struct{})
	ready := make(chan struct{}, 2)
	results := make(chan commitResult, 2)
	for range 2 {
		go func() {
			ready <- struct{}{}
			<-start
			committed, commitErr := restarted.CommitExecution(ctx, source.Revision, current, prepared)
			results <- commitResult{envelope: committed, err: commitErr}
		}()
	}
	<-ready
	<-ready
	close(start)
	first, second := <-results, <-results
	if first.err != nil {
		first, second = second, first
	}
	result, firstErr, secondErr := first.envelope, first.err, second.err
	// Assert.
	for _, err := range []error{staleErr, staleRenew, staleRelease} {
		if !errors.Is(err, flowy.ErrLeaseLost) {
			t.Fatalf("stale owner accepted: %v", err)
		}
	}
	if firstErr != nil || !errors.Is(secondErr, flowy.ErrConcurrencyConflict) {
		t.Fatalf("commit: %v %v", firstErr, secondErr)
	}
	if result.Migration == nil || result.Migration.SourceRevision != source.Revision {
		t.Fatal("missing provenance")
	}
	history, err := restarted.LoadCheckpoint(ctx, id, source.Revision)
	if err != nil || history.Progress.ExecutionPointer != "before" ||
		string(history.JournalPayload) != "completed outcome" {
		t.Fatalf("source changed: %+v %v", history, err)
	}
	latest, err := mustExecutionStore(t, pool).LoadExecution(ctx, id)
	if err != nil || latest.Progress.ExecutionPointer != "after" || latest.Revision != result.Revision {
		t.Fatalf("restart lost progress: %+v %v", latest, err)
	}
}
