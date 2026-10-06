//go:build integration

package postgres

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/skosovsky/flowy"
)

func TestImportDanglingReferencesPersistentNoPublication(t *testing.T) {
	for _, children := range []bool{false, true} {
		t.Run(map[bool]string{false: "journal", true: "children"}[children], func(t *testing.T) {
			assertImportDanglingReferencesPersistent(t, children)
		})
	}
}

func assertImportDanglingReferencesPersistent(t *testing.T, children bool) {
	t.Helper()
	// Arrange: explicit valid host artifact, invalid imported runtime binding.
	ctx, pool := racePool(t)
	if _, err := pool.Exec(ctx, ExecutionSchemaSQL()); err != nil {
		t.Fatal(err)
	}
	id := testThreadID(t)
	store := mustExecutionStore(t, pool)
	var calls atomic.Int32
	request := flowy.ActivityRequest{Key: "operation", Implementation: "host", Input: []byte("input"),
		Dispatch: func(context.Context, flowy.ActivityInvocation) ([]byte, error) {
			calls.Add(1)
			return []byte("receipt"), nil
		}}
	runner := persistentReferenceRunner(t, store, referenceDescriptor("import"), "node", request, nil)
	payload := []byte("opaque legacy artifact")
	sum := sha256.Sum256(payload)
	source := flowy.LegacyExecutionSource{ID: "legacy", Revision: 1, Format: "host-format",
		Digest: hex.EncodeToString(sum[:]), Payload: payload}
	importer := flowy.ExecutionImporter{ID: "host-import", Format: source.Format,
		Transform: func([]byte) (flowy.ImportedExecutionState, error) {
			state := flowy.ImportedExecutionState{Progress: flowy.MigrationState{
				ExecutionPointer: "node", StatePayload: []byte(`{"value":1}`)}, EffectsPayload: []byte("[]")}
			if children {
				state.Progress.ChildGroupReferences = map[string]string{"group": "absent"}
			} else {
				state.Progress.JournalReferences = map[string]string{"operation": "absent"}
			}
			return state, nil
		}}
	// Act: reject before publication; inspect absence using a separately created pool.
	token, importErr := runner.Import(ctx, id, source, importer)
	pool.Close()
	restartCtx, restartPool := racePool(t)
	restartedStore := mustExecutionStore(t, restartPool)
	_, loadErr := restartedStore.LoadExecution(restartCtx, id)
	_, historyErr := restartedStore.LoadCheckpoint(restartCtx, id, 1)
	// Assert: no successful token/head/history/dispatch survived the invalid import.
	if !errors.Is(importErr, flowy.ErrExecutionImportInvalid) || token.SnapshotRevision != 0 ||
		!errors.Is(
			loadErr,
			flowy.ErrThreadNotFound,
		) || !errors.Is(historyErr, flowy.ErrThreadNotFound) || calls.Load() != 0 {
		t.Fatalf("invalid import published: token=%+v import=%v load=%v history=%v calls=%d",
			token, importErr, loadErr, historyErr, calls.Load())
	}
	invalidTransform := importer.Transform
	importer.Transform = func(payload []byte) (flowy.ImportedExecutionState, error) {
		state, err := invalidTransform(payload)
		state.Progress.JournalReferences, state.Progress.ChildGroupReferences = nil, nil
		return state, err
	}
	restarted := persistentReferenceRunner(t, restartedStore, referenceDescriptor("import"), "node", request, nil)
	validToken, err := restarted.Import(restartCtx, id, source, importer)
	if err != nil || validToken.SnapshotRevision != 1 {
		t.Fatalf("target or lease retained: token=%+v err=%v", validToken, err)
	}
	result, err := restarted.Resume(restartCtx, validToken)
	if err != nil || result == nil || result.Status != flowy.RunStatusCompleted || calls.Load() != 1 {
		t.Fatalf("valid retry not executable: result=%+v err=%v calls=%d", result, err, calls.Load())
	}
}
