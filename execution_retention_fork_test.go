package flowy_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/skosovsky/flowy"
	"github.com/skosovsky/flowy/testutil"
)

func TestRetentionKeepsForkAuthorityAfterSourceAndCreationPayloadLoss(t *testing.T) {
	// Arrange: completed source and explicitly simulated fork.
	ctx := context.Background()
	store := testutil.NewMemoryExecutionStore(nil)
	source := seedForkSource(t, store)
	var nodes, live atomic.Int32
	policy := &flowy.ForkExecutionPolicy{
		Label:        "fake",
		Mode:         flowy.ForkFake,
		FakeActivity: func(context.Context, flowy.ActivityInvocation) ([]byte, error) { return []byte("simulated"), nil },
	}
	runner := forkRunnerForTest(t, store, policy, &nodes, &live)
	token, err := runner.Fork(ctx, forkRequestForTest(source, "target"))
	if err != nil {
		t.Fatal(err)
	}
	creation, err := store.LoadCheckpoint(ctx, token.ThreadID, token.SnapshotRevision)
	if err != nil {
		t.Fatal(err)
	}
	completed, err := runner.Resume(ctx, token)
	if err != nil {
		t.Fatal(err)
	}
	// Act: reclaim source and target creation history while retaining target head.
	_, sourceErr := store.RetainExecution(
		ctx,
		flowy.ExecutionRetentionRequest{
			ExecutionID: source.ExecutionID,
			Revision:    source.Revision,
			Policy:      flowy.ExecutionRetentionPolicy{Label: "archive-source", DeletePayload: true},
		},
	)
	_, targetErr := store.RetainExecution(
		ctx,
		flowy.ExecutionRetentionRequest{
			ExecutionID: "target",
			Revision:    completed.ResumeToken.SnapshotRevision,
			Policy:      flowy.ExecutionRetentionPolicy{Label: "retain-head", KeepLast: 1},
		},
	)
	head, loadErr := store.LoadExecution(ctx, "target")
	_, missingSource := store.LoadCheckpoint(ctx, "source", source.Revision)
	_, missingCreation := store.LoadCheckpoint(ctx, "target", creation.Revision)
	replay, resumeErr := runner.Resume(ctx, completed.ResumeToken)
	// Assert: source availability and creation availability are independent of immutable lineage authority.
	if sourceErr != nil || targetErr != nil || loadErr != nil || head.Fork == nil || *head.Fork != *creation.Fork ||
		!errors.Is(missingSource, flowy.ErrExecutionCheckpointUnavailable) ||
		!errors.Is(missingCreation, flowy.ErrExecutionCheckpointUnavailable) ||
		resumeErr != nil ||
		replay.State != completed.State ||
		nodes.Load() != 1 ||
		live.Load() != 0 {
		t.Fatalf(
			"source=%v target=%v load=%v missing=%v/%v resume=%v nodes/live=%d/%d",
			sourceErr,
			targetErr,
			loadErr,
			missingSource,
			missingCreation,
			resumeErr,
			nodes.Load(),
			live.Load(),
		)
	}
}
