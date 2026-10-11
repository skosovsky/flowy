//go:build integration

package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/skosovsky/flowy"
)

func TestLifecyclePersistentCreationReceiptRejectsResealedTarget(t *testing.T) {
	// Arrange: valid independent receipt is persisted with the initial target.
	ctx, pool := racePool(t)
	if _, err := pool.Exec(ctx, ExecutionSchemaSQL()); err != nil {
		t.Fatal(err)
	}
	store := mustExecutionStore(t, pool)
	var calls, projections atomic.Int32
	descriptor := referenceDescriptor("creation-seal")
	activity := flowy.ActivityRequest{
		Key:            "write",
		Implementation: "host",
		Input:          []byte("input"),
		Dispatch: func(context.Context, flowy.ActivityInvocation) ([]byte, error) {
			calls.Add(1)
			return []byte("receipt"), nil
		},
	}
	runner := persistentReferenceRunner(t, store, descriptor, "node", activity, nil)
	source, err := runner.Start(ctx, testThreadID(t), intState{Value: 1})
	if err != nil {
		t.Fatal(err)
	}
	token, err := runner.Rollover(
		ctx,
		source.ResumeToken,
		lifecyclePGRequest(descriptor, source.ResumeToken.ThreadID+"-next", &projections),
	)
	if err != nil {
		t.Fatal(err)
	}
	initial, err := store.LoadExecution(ctx, token.ThreadID)
	if err != nil {
		t.Fatal(err)
	}
	// Act: malicious storage changes the initial BYOT state and recomputes its seal,
	// while keeping the independent receipt and matching lineage unchanged.
	forged := flowy.CloneExecutionEnvelope(initial)
	forged.Progress.StatePayload = []byte(`{"value":999}`)
	forged, err = flowy.SealExecutionEnvelope(forged)
	if err != nil {
		t.Fatal(err)
	}
	payload, err := json.Marshal(forged)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Exec(
		ctx,
		`UPDATE flowy_execution_history SET payload=$3::jsonb WHERE execution_id=$1 AND revision=$2`,
		token.ThreadID,
		token.SnapshotRevision,
		payload,
	); err != nil {
		t.Fatal(err)
	}
	pool.Close()
	recoveryCtx, recoveryPool := racePool(t)
	recovery := mustExecutionStore(t, recoveryPool)
	recoveredRunner := persistentReferenceRunner(t, recovery, descriptor, "node", activity, nil)
	_, loadErr := recovery.LoadExecution(recoveryCtx, token.ThreadID)
	_, exactErr := recovery.LoadCheckpoint(recoveryCtx, token.ThreadID, token.SnapshotRevision)
	_, cleanupErr := recovery.RetainExecution(
		recoveryCtx,
		flowy.ExecutionRetentionRequest{
			ExecutionID: token.ThreadID,
			Revision:    token.SnapshotRevision,
			Policy:      flowy.ExecutionRetentionPolicy{Label: "keep", KeepLast: 1},
		},
	)
	_, resumeErr := recoveredRunner.Resume(recoveryCtx, token)
	// Assert: valid checksum does not replace the immutable creation receipt;
	// no runtime callback or maintenance is allowed to hide that corruption.
	if !errors.Is(loadErr, flowy.ErrExecutionCorrupt) || !errors.Is(exactErr, flowy.ErrExecutionCorrupt) ||
		!errors.Is(cleanupErr, flowy.ErrExecutionCorrupt) ||
		!errors.Is(resumeErr, flowy.ErrExecutionCorrupt) ||
		calls.Load() != 1 ||
		projections.Load() != 1 {
		t.Fatalf(
			"load=%v exact=%v cleanup=%v resume=%v calls/projections=%d/%d",
			loadErr,
			exactErr,
			cleanupErr,
			resumeErr,
			calls.Load(),
			projections.Load(),
		)
	}
	var retained uint64
	if err = recoveryPool.QueryRow(recoveryCtx, `SELECT count(*) FROM flowy_execution_history WHERE execution_id=$1`, token.ThreadID).
		Scan(&retained); err != nil ||
		retained != 1 {
		t.Fatalf("corrupt history altered: count=%d err=%v", retained, err)
	}
}
