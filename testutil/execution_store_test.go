package testutil

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/skosovsky/flowy"
)

func TestExecutionStoreRejectsReusedOwnerIncarnation(t *testing.T) {
	// Arrange.
	now := time.Date(2026, 10, 4, 0, 0, 0, 0, time.UTC)
	store := NewMemoryExecutionStore(func() time.Time { return now })
	ctx := context.Background()
	old, err := store.AcquireExecution(ctx, "run", "same-owner", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	now = now.Add(2 * time.Second)
	current, err := store.AcquireExecution(ctx, "run", "same-owner", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	envelope := flowy.ExecutionEnvelope{
		ExecutionID: "run",
		Descriptor: flowy.ExecutionDescriptor{
			GraphID:       "g",
			GraphRevision: "r",
			StateCodec:    "s", EffectsCodec: "host-effects-v1",
			ExecutionContract: "e",
			ReplayPolicy:      flowy.StepReplayPolicy{Label: "test-safe-steps", Mode: flowy.StepReplaySafe},
		},
		Progress: flowy.MigrationState{ExecutionPointer: "node"},
	}
	// Act.
	_, commitErr := store.CommitExecution(ctx, 0, old, envelope)
	_, renewErr := store.RenewExecution(ctx, old, time.Hour)
	releaseErr := store.ReleaseExecution(ctx, old)
	// Assert.
	for _, err := range []error{commitErr, renewErr, releaseErr} {
		if !errors.Is(err, flowy.ErrLeaseLost) {
			t.Fatalf("stale incarnation accepted: %v", err)
		}
	}
	if current.Incarnation <= old.Incarnation {
		t.Fatal("fence reused")
	}
	if _, err := store.CommitExecution(ctx, 0, current, envelope); err != nil {
		t.Fatalf("successor changed: %v", err)
	}
}

func TestExecutionMigrationCommitOCCAndHistory(t *testing.T) {
	// Arrange.
	ctx := context.Background()
	store := NewMemoryExecutionStore(nil)
	lease, err := store.AcquireExecution(ctx, "run", "owner", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	descriptor := flowy.ExecutionDescriptor{
		GraphID:       "g",
		GraphRevision: "old",
		StateCodec:    "s", EffectsCodec: "host-effects-v1",
		ExecutionContract: "e",
		ReplayPolicy:      flowy.StepReplayPolicy{Label: "test-safe-steps", Mode: flowy.StepReplaySafe},
	}
	source, err := store.CommitExecution(
		ctx,
		0,
		lease,
		flowy.ExecutionEnvelope{
			ExecutionID: "run",
			Descriptor:  descriptor,
			Progress:    flowy.MigrationState{ExecutionPointer: "before", StatePayload: []byte("state")},
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	target := descriptor
	target.GraphRevision = "new"
	migration := flowy.ExecutionMigration{
		ID:     "move",
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
	// Act: both contenders are ready before either commits the source revision.
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
			committed, commitErr := store.CommitExecution(ctx, source.Revision, lease, prepared)
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
	if firstErr != nil || !errors.Is(secondErr, flowy.ErrConcurrencyConflict) {
		t.Fatalf("OCC lost: %v %v", firstErr, secondErr)
	}
	if result.Migration == nil || result.Migration.SourceRevision != source.Revision {
		t.Fatal("lineage missing")
	}
	old, err := store.LoadCheckpoint(ctx, "run", source.Revision)
	if err != nil || old.Progress.ExecutionPointer != "before" {
		t.Fatalf("source lost: %+v %v", old, err)
	}
	result.Progress.StatePayload[0] = 'X'
	latest, err := store.LoadExecution(ctx, "run")
	if err != nil || string(latest.Progress.StatePayload) != "state" {
		t.Fatal("caller aliases storage")
	}
}
