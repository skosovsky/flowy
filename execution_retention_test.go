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

func TestRetentionPreservesProtectedExactHistoryAndFenceIdentity(t *testing.T) {
	// Arrange: safe idle source plus a published continuation, with a held stale handle.
	ctx := context.Background()
	store := testutil.NewMemoryExecutionStore(nil)
	var calls, projections atomic.Int32
	runner := lifecycleRunner(t, store, 2, &calls)
	paused, err := runner.Start(ctx, "source", 0)
	if err != nil {
		t.Fatal(err)
	}
	original, err := store.LoadCheckpoint(ctx, "source", 1)
	if err != nil {
		t.Fatal(err)
	}
	target, err := runner.Rollover(ctx, paused.ResumeToken, lifecycleRolloverRequest("target", 2, &projections))
	if err != nil {
		t.Fatal(err)
	}
	head, err := store.LoadExecution(ctx, "source")
	if err != nil {
		t.Fatal(err)
	}
	lease, err := store.AcquireExecution(ctx, "source", "maintenance-contender", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	request := flowy.ExecutionRetentionRequest{
		ExecutionID: "source",
		Revision:    head.Revision,
		Policy:      flowy.ExecutionRetentionPolicy{Label: "archive-v1", KeepLast: 1, ProtectedRevisions: []uint64{1}},
	}
	// Act: busy rejects, then prune exactly and retain a usable historical anchor.
	_, busyErr := store.RetainExecution(ctx, request)
	if err = store.ReleaseExecution(ctx, lease); err != nil {
		t.Fatal(err)
	}
	receipt, err := store.RetainExecution(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	repeat, repeatErr := store.RetainExecution(ctx, request)
	kept, keptErr := flowy.InspectExecutionCheckpoint(
		ctx,
		store,
		flowy.HistoricalCheckpointReference{ExecutionID: "source", Revision: 1, Digest: original.Digest},
	)
	_, prunedErr := store.LoadCheckpoint(ctx, "source", 2)
	// Assert: current head/protected revision remain exact; deleted revisions never become latest.
	if !errors.Is(busyErr, flowy.ErrThreadLeaseBusy) || receipt.DeletedRevisions == 0 || receipt.DeletedBytes <= 0 ||
		repeatErr != nil ||
		repeat.DeletedRevisions != 0 ||
		keptErr != nil ||
		kept.Digest != original.Digest ||
		!errors.Is(prunedErr, flowy.ErrExecutionCheckpointUnavailable) {
		t.Fatalf(
			"busy=%v receipt=%+v repeat=%+v/%v kept=%v pruned=%v",
			busyErr,
			receipt,
			repeat,
			repeatErr,
			keptErr,
			prunedErr,
		)
	}
	// Act: delete the completed source payload; replay still uses independent receipt metadata.
	request.Policy = flowy.ExecutionRetentionPolicy{Label: "delete-payload-v1", DeletePayload: true}
	_, err = store.RetainExecution(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	replay, replayErr := runner.Rollover(ctx, paused.ResumeToken, lifecycleRolloverRequest("target", 2, &projections))
	_, unavailableErr := store.LoadExecution(ctx, "source")
	_, recreateErr := runner.Start(ctx, "source", 0)
	successor, err := store.AcquireExecution(ctx, "source", "successor", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	_, staleErr := store.CommitExecution(ctx, head.Revision, lease, head)
	_, reuseErr := store.CommitExecution(ctx, 0, successor, head)
	if err = store.ReleaseExecution(ctx, successor); err != nil {
		t.Fatal(err)
	}
	resumed, resumeErr := runner.Resume(ctx, target)
	// Assert: ID never becomes fresh; an old lease cannot write after payload deletion.
	if replayErr != nil || replay != target || projections.Load() != 1 ||
		!errors.Is(unavailableErr, flowy.ErrExecutionCheckpointUnavailable) ||
		!errors.Is(recreateErr, flowy.ErrExecutionCheckpointUnavailable) ||
		successor.Incarnation <= lease.Incarnation ||
		!errors.Is(staleErr, flowy.ErrLeaseLost) ||
		!errors.Is(reuseErr, flowy.ErrConcurrencyConflict) ||
		resumeErr != nil ||
		resumed.State != 2 ||
		calls.Load() != 4 {
		t.Fatalf(
			"replay=%v unavailable=%v recreate=%v stale=%v reuse=%v resume=%v fence=%d/%d",
			replayErr,
			unavailableErr,
			recreateErr,
			staleErr,
			reuseErr,
			resumeErr,
			lease.Incarnation,
			successor.Incarnation,
		)
	}
}

func TestLifecycleUnresolvedWorkCannotBePrunedOrTransferred(t *testing.T) {
	// Arrange: abandoned external child cannot become safe from lease release.
	ctx := context.Background()
	store := testutil.NewMemoryExecutionStore(nil)
	dispatch := func(context.Context, flowy.ChildInvocation) (flowy.ChildResult, error) {
		return flowy.ChildResult{State: flowy.ChildUnknown}, nil
	}
	runner := task24ChildJoinRunner(t, store, dispatch)
	_, err := runner.Start(ctx, "run", durableTestState{})
	if !errors.Is(err, flowy.ErrChildrenUnresolved) {
		t.Fatal(err)
	}
	source, err := store.LoadExecution(ctx, "run")
	if err != nil {
		t.Fatal(err)
	}
	var projections atomic.Int32
	req := lifecycleRolloverRequest("new", 1, &projections)
	req.SourceDescriptor = source.Descriptor
	// Act.
	_, rolloverErr := runner.Rollover(ctx, flowy.ResumeToken{ThreadID: "run", SnapshotRevision: source.Revision}, req)
	_, pruneErr := store.RetainExecution(
		ctx,
		flowy.ExecutionRetentionRequest{
			ExecutionID: "run",
			Revision:    source.Revision,
			Policy:      flowy.ExecutionRetentionPolicy{Label: "prune", KeepLast: 1},
		},
	)
	after, err := store.LoadExecution(ctx, "run")
	// Assert: rejected before BYOT projection/publication, journal is unchanged.
	if err != nil || !errors.Is(rolloverErr, flowy.ErrExecutionLifecycleUnsafe) ||
		!errors.Is(pruneErr, flowy.ErrExecutionLifecycleUnsafe) ||
		source.Digest != after.Digest ||
		projections.Load() != 0 {
		t.Fatalf("rollover=%v prune=%v load=%v", rolloverErr, pruneErr, err)
	}
}
