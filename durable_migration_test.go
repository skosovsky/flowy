package flowy_test

import (
	"bytes"
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/skosovsky/flowy"
	"github.com/skosovsky/flowy/testutil"
)

func seedRawMigration(ctx context.Context, t *testing.T, store flowy.ExecutionStore) flowy.ExecutionEnvelope {
	t.Helper()
	lease, err := store.AcquireExecution(ctx, "run", "seed", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	source, err := store.CommitExecution(ctx, 0, lease, flowy.ExecutionEnvelope{
		ExecutionID: "run", Descriptor: durableDescriptor("old"),
		Progress:       flowy.MigrationState{ExecutionPointer: "old-node", StatePayload: []byte("opaque-old-state")},
		EffectsPayload: []byte(`[]`), Activation: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if releaseErr := store.ReleaseExecution(ctx, lease); releaseErr != nil {
		t.Fatal(releaseErr)
	}
	return source
}

func TestDurableMigrationResumeUsesTargetCodec(t *testing.T) {
	t.Parallel()
	for _, stream := range []bool{false, true} {
		t.Run(map[bool]string{false: "sync", true: "stream"}[stream], func(t *testing.T) {
			t.Parallel()
			// Arrange: current codec cannot decode the opaque source state or its old cursor.
			ctx := context.Background()
			store := testutil.NewMemoryExecutionStore(nil)
			source := seedRawMigration(ctx, t, store)
			var nodes, transforms atomic.Int32
			migration := flowy.ExecutionMigration{
				ID:     "state-and-cursor",
				Source: source.Descriptor,
				Target: durableDescriptor("target"),
				Transform: func(state flowy.MigrationState) (flowy.MigrationState, error) {
					transforms.Add(1)
					state.StatePayload = []byte(`{"Value":41}`)
					state.ExecutionPointer = "node"
					return state, nil
				},
			}
			runner := importRunnerOptions(
				t,
				store,
				&nodes,
				flowy.DurableOptions{
					Owner:      "worker",
					LeaseTTL:   time.Minute,
					Migrations: []flowy.ExecutionMigration{migration},
				},
			)
			token := flowy.ResumeToken{ThreadID: "run", SnapshotRevision: source.Revision}
			// Act.
			result, err := resumeMigrated(ctx, t, runner, token, stream)
			// Assert: migration commits before execution and exact source remains readable.
			if err != nil || result.State.Value != 42 || nodes.Load() != 1 || transforms.Load() != 1 {
				t.Fatalf(
					"migration resume: %+v %v nodes=%d transforms=%d",
					result,
					err,
					nodes.Load(),
					transforms.Load(),
				)
			}
			migrated, err := store.LoadCheckpoint(ctx, "run", source.Revision+1)
			if err != nil || migrated.Migration == nil || migrated.Migration.SourceRevision != source.Revision ||
				migrated.Progress.ExecutionPointer != "node" {
				t.Fatalf("migration not committed: %+v %v", migrated, err)
			}
			retained, err := store.LoadCheckpoint(ctx, "run", source.Revision)
			if err != nil || !bytes.Equal(retained.Progress.StatePayload, source.Progress.StatePayload) ||
				retained.Progress.ExecutionPointer != source.Progress.ExecutionPointer {
				t.Fatalf("source changed: %+v %v", retained, err)
			}
		})
	}
}

func resumeMigrated(
	ctx context.Context,
	t *testing.T,
	runner *flowy.DurableRunner[durableTestState, flowy.NoEffect],
	token flowy.ResumeToken,
	stream bool,
) (*flowy.RunResult[durableTestState, flowy.NoEffect], error) {
	t.Helper()
	if !stream {
		return runner.Resume(ctx, token)
	}
	handle, err := runner.ResumeStream(ctx, token)
	if err != nil {
		return nil, err
	}
	completed := 0
	for event := range handle.Events() {
		if event.Type == flowy.EventCompleted {
			completed++
		}
	}
	if completed != 1 {
		t.Fatalf("migrated stream completion count: %d", completed)
	}
	return handle.WaitResult()
}

func TestDurableMigrationFailurePreservesSource(t *testing.T) {
	t.Parallel()
	for _, invalidPointer := range []bool{false, true} {
		t.Run(map[bool]string{false: "transform", true: "target-pointer"}[invalidPointer], func(t *testing.T) {
			t.Parallel()
			// Arrange.
			ctx := context.Background()
			store := testutil.NewMemoryExecutionStore(nil)
			source := seedRawMigration(ctx, t, store)
			var nodes atomic.Int32
			migration := flowy.ExecutionMigration{
				ID:     "invalid",
				Source: source.Descriptor,
				Target: durableDescriptor("target"),
				Transform: func(state flowy.MigrationState) (flowy.MigrationState, error) {
					state.StatePayload[0] = 'X'
					if invalidPointer {
						state.ExecutionPointer = "absent"
						return state, nil
					}
					return state, errors.New("transform failed")
				},
			}
			runner := importRunnerOptions(
				t,
				store,
				&nodes,
				flowy.DurableOptions{
					Owner:      "worker",
					LeaseTTL:   time.Minute,
					Migrations: []flowy.ExecutionMigration{migration},
				},
			)
			// Act.
			_, resumeErr := runner.Resume(ctx, flowy.ResumeToken{ThreadID: "run", SnapshotRevision: source.Revision})
			// Assert.
			latest, err := store.LoadExecution(ctx, "run")
			if !errors.Is(resumeErr, flowy.ErrMigrationInvalid) || err != nil || latest.Revision != source.Revision ||
				!bytes.Equal(latest.Progress.StatePayload, source.Progress.StatePayload) ||
				nodes.Load() != 0 {
				t.Fatalf(
					"failed migration changed source: %+v resume=%v load=%v nodes=%d",
					latest,
					resumeErr,
					err,
					nodes.Load(),
				)
			}
		})
	}
}
