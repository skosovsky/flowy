//go:build integration

package postgres

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/skosovsky/flowy"
)

func TestMigrationInvalidCodecPersistentRestart(t *testing.T) {
	for _, stream := range []bool{false, true} {
		for _, effects := range []bool{false, true} {
			name := map[bool]string{false: "sync", true: "stream"}[stream] + "/" +
				map[bool]string{false: "state", true: "effects"}[effects]
			t.Run(name, func(t *testing.T) { assertMigrationInvalidCodecPersistentRestart(t, stream, effects) })
		}
	}
}

func assertMigrationInvalidCodecPersistentRestart(t *testing.T, stream, effects bool) {
	t.Helper()
	// Arrange: raw source has an explicitly old descriptor and incompatible bytes.
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
	envelope := flowy.ExecutionEnvelope{ExecutionID: id, Descriptor: referenceDescriptor("old"), Activation: 1,
		Progress:       flowy.ExecutionProgress{ExecutionPointer: "old-node", StatePayload: []byte("opaque-old-state")},
		EffectsPayload: []byte("[]")}
	if effects {
		envelope.EffectsPayload = []byte("invalid-effects")
	}
	source, err := store.CommitExecution(ctx, 0, lease, envelope)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.ReleaseExecution(ctx, lease); err != nil {
		t.Fatal(err)
	}
	pool.Close()
	restartCtx, restartPool := racePool(t)
	restartedStore := mustExecutionStore(t, restartPool)
	var dispatches atomic.Int32
	request := flowy.ActivityRequest{Key: "operation", Implementation: "host", Input: []byte("input"),
		Dispatch: func(context.Context, flowy.ActivityInvocation) ([]byte, error) {
			dispatches.Add(1)
			return []byte("must not run"), nil
		}}
	migration := flowy.ExecutionMigration{
		ID:     "invalid-codec",
		Source: source.Descriptor,
		Target: referenceDescriptor(
			"new",
		),
		Transform: func(state flowy.ExecutionProgress) (flowy.ExecutionProgress, error) {
			state.ExecutionPointer = "node"
			state.StatePayload = []byte("invalid-state")
			if effects {
				state.StatePayload = []byte(`{"value":1}`)
			}
			return state, nil
		},
	}
	runner := persistentReferenceRunner(t, restartedStore, referenceDescriptor("new"), "node", request,
		[]flowy.ExecutionMigration{migration})
	token := flowy.ResumeToken{ThreadID: id, SnapshotRevision: source.Revision}
	// Act: select the target codec only after explicit migration, reject before publication.
	var resumeErr error
	if stream {
		_, resumeErr = runner.ResumeStream(restartCtx, token)
	} else {
		_, resumeErr = runner.Resume(restartCtx, token)
	}
	// Assert: latest and exact source survive independently; no target history or dispatch.
	assertPersistentMigrationCodecSource(restartCtx, t, restartedStore, source, resumeErr, dispatches.Load())
}

func assertPersistentMigrationCodecSource(ctx context.Context, t *testing.T, store *ExecutionStore,
	source flowy.ExecutionEnvelope, resumeErr error, dispatches int32,
) {
	t.Helper()
	latest, loadErr := store.LoadExecution(ctx, source.ExecutionID)
	retained, historyErr := store.LoadCheckpoint(ctx, source.ExecutionID, source.Revision)
	_, targetErr := store.LoadCheckpoint(ctx, source.ExecutionID, source.Revision+1)
	if !errors.Is(resumeErr, flowy.ErrMigrationInvalid) || loadErr != nil || historyErr != nil ||
		latest.Revision != source.Revision || latest.Digest != source.Digest ||
		latest.Descriptor != source.Descriptor || retained.Digest != source.Digest ||
		!errors.Is(targetErr, flowy.ErrThreadNotFound) || dispatches != 0 {
		t.Fatalf("invalid migration published: resume=%v latest=%+v load=%v history=%v target=%v dispatches=%d",
			resumeErr, latest, loadErr, historyErr, targetErr, dispatches)
	}
}
