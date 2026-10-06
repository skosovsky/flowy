package flowy_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/skosovsky/flowy"
	"github.com/skosovsky/flowy/testutil"
)

func TestDurableMigrationInvalidCodecPreservesSource(t *testing.T) {
	t.Parallel()
	for _, stream := range []bool{false, true} {
		for _, malformedEffects := range []bool{false, true} {
			name := map[bool]string{false: "sync", true: "stream"}[stream] + "/" +
				map[bool]string{false: "state", true: "effects"}[malformedEffects]
			t.Run(name, func(t *testing.T) {
				t.Parallel()
				// Arrange: opaque source state is incompatible with the target codec.
				ctx := context.Background()
				store := testutil.NewMemoryExecutionStore(nil)
				source := seedMigrationCodecSource(ctx, t, store, malformedEffects)
				var nodes atomic.Int32
				migration := flowy.ExecutionMigration{
					ID: "invalid-codec", Source: source.Descriptor, Target: durableDescriptor("target"),
					Transform: func(state flowy.MigrationState) (flowy.MigrationState, error) {
						state.ExecutionPointer = "node"
						state.StatePayload = []byte("invalid-state")
						if malformedEffects {
							state.StatePayload = []byte(`{"Value":41}`)
						}
						return state, nil
					},
				}
				runner := importRunnerOptions(t, store, &nodes, flowy.DurableOptions{
					Owner: "worker", LeaseTTL: time.Minute, Migrations: []flowy.ExecutionMigration{migration},
				})
				token := flowy.ResumeToken{ThreadID: "run", SnapshotRevision: source.Revision}
				// Act: both entry points must fail during preparation, not after publication.
				var resumeErr error
				if stream {
					_, resumeErr = runner.ResumeStream(ctx, token)
				} else {
					_, resumeErr = runner.Resume(ctx, token)
				}
				// Assert: no revision, descriptor, digest, node or historical target changed.
				assertInvalidMigrationCodecSource(ctx, t, store, source, resumeErr, nodes.Load())
			})
		}
	}
}

func seedMigrationCodecSource(ctx context.Context, t *testing.T, store *testutil.MemoryExecutionStore,
	malformedEffects bool,
) flowy.ExecutionEnvelope {
	t.Helper()
	source := seedRawMigration(ctx, t, store)
	if !malformedEffects {
		return source
	}
	lease, err := store.AcquireExecution(ctx, "run", "seed-effects", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	source.EffectsPayload = []byte("invalid-effects")
	source, err = store.CommitExecution(ctx, source.Revision, lease, source)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.ReleaseExecution(ctx, lease); err != nil {
		t.Fatal(err)
	}
	return source
}

func assertInvalidMigrationCodecSource(ctx context.Context, t *testing.T, store *testutil.MemoryExecutionStore,
	source flowy.ExecutionEnvelope, resumeErr error, nodes int32,
) {
	t.Helper()
	latest, loadErr := store.LoadExecution(ctx, "run")
	retained, historyErr := store.LoadCheckpoint(ctx, "run", source.Revision)
	_, targetErr := store.LoadCheckpoint(ctx, "run", source.Revision+1)
	if !errors.Is(resumeErr, flowy.ErrMigrationInvalid) || loadErr != nil || historyErr != nil ||
		latest.Revision != source.Revision || latest.Digest != source.Digest ||
		latest.Descriptor != source.Descriptor || retained.Digest != source.Digest ||
		targetErr == nil || nodes != 0 {
		t.Fatalf("invalid target published: resume=%v latest=%+v load=%v history=%v target=%v nodes=%d",
			resumeErr, latest, loadErr, historyErr, targetErr, nodes)
	}
}
