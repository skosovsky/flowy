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

func TestLifecyclePersistentConcurrentResumeForkInspectionAndCleanup(t *testing.T) {
	// Arrange: independent pools contend while source projection holds its durable lease.
	ctx, pool := racePool(t)
	if _, err := pool.Exec(ctx, ExecutionSchemaSQL()); err != nil {
		t.Fatal(err)
	}
	otherCtx, otherPool := racePool(t)
	base, other := mustExecutionStore(t, pool), mustExecutionStore(t, otherPool)
	var calls, projections, fakeCalls atomic.Int32
	descriptor := referenceDescriptor("concurrency")
	activity := flowy.ActivityRequest{
		Key:            "write",
		Implementation: "host",
		Input:          []byte("input"),
		Dispatch: func(context.Context, flowy.ActivityInvocation) ([]byte, error) {
			calls.Add(1)
			return []byte("receipt"), nil
		},
	}
	runner := persistentReferenceRunner(t, base, descriptor, "node", activity, nil)
	source, err := runner.Start(ctx, testThreadID(t), intState{Value: 1})
	if err != nil {
		t.Fatal(err)
	}
	before, err := base.LoadExecution(ctx, source.ResumeToken.ThreadID)
	if err != nil {
		t.Fatal(err)
	}
	policy := &flowy.ForkExecutionPolicy{
		Label: "fake",
		Mode:  flowy.ForkFake,
		FakeActivity: func(context.Context, flowy.ActivityInvocation) ([]byte, error) {
			fakeCalls.Add(1)
			return []byte("fake"), nil
		},
	}
	contender := persistentReferenceRunnerOptions(
		t,
		other,
		descriptor,
		"node",
		activity,
		flowy.DurableOptions{Owner: "worker", LeaseTTL: time.Minute, ForkPolicy: policy},
	)
	entered, release := make(chan struct{}), make(chan struct{})
	request := lifecyclePGRequest(descriptor, before.ExecutionID+"-next", &projections)
	original := request.Project
	request.Project = func(p flowy.RolloverPayload) (flowy.RolloverPayload, error) {
		close(entered)
		<-release
		return original(p)
	}
	type transferResult struct {
		token flowy.ResumeToken
		err   error
	}
	done := make(chan transferResult, 1)
	go func() {
		token, transferErr := runner.Rollover(ctx, source.ResumeToken, request)
		done <- transferResult{token: token, err: transferErr}
	}()
	<-entered
	// Act: mutating contenders cannot enter, historical read/fake fork can.
	_, resumeErr := contender.Resume(otherCtx, source.ResumeToken)
	_, cleanupErr := other.RetainExecution(
		otherCtx,
		flowy.ExecutionRetentionRequest{
			ExecutionID: before.ExecutionID,
			Revision:    before.Revision,
			Policy:      flowy.ExecutionRetentionPolicy{Label: "keep", KeepLast: 1},
		},
	)
	exact, inspectErr := other.LoadCheckpoint(otherCtx, before.ExecutionID, before.Revision)
	fork, forkErr := contender.Fork(
		otherCtx,
		flowy.ForkRequest{
			Source: flowy.HistoricalCheckpointReference{
				ExecutionID: before.ExecutionID,
				Revision:    before.Revision,
				Digest:      before.Digest,
			},
			TargetID: before.ExecutionID + "-fork",
			Transform: flowy.ForkTransform{
				Label:     "identity",
				Source:    descriptor,
				Transform: func(p flowy.ExecutionProgress) (flowy.ExecutionProgress, error) { return p, nil },
			},
		},
	)
	close(release)
	transferred := <-done
	// Assert: one continuation, historical fork separately authorized to simulate.
	if !errors.Is(resumeErr, flowy.ErrThreadLeaseBusy) || !errors.Is(cleanupErr, flowy.ErrThreadLeaseBusy) ||
		inspectErr != nil ||
		exact.Digest != before.Digest ||
		forkErr != nil ||
		transferred.err != nil ||
		calls.Load() != 1 ||
		projections.Load() != 1 {
		t.Fatalf(
			"resume=%v cleanup=%v inspect=%v fork=%v transfer=%v calls/projections=%d/%d",
			resumeErr,
			cleanupErr,
			inspectErr,
			forkErr,
			transferred.err,
			calls.Load(),
			projections.Load(),
		)
	}
	forkCreation, err := other.LoadCheckpoint(otherCtx, fork.ThreadID, fork.SnapshotRevision)
	if err != nil {
		t.Fatal(err)
	}
	simulated, err := contender.Resume(otherCtx, fork)
	if err != nil {
		t.Fatal(err)
	}
	head, err := other.LoadExecution(otherCtx, before.ExecutionID)
	if err != nil {
		t.Fatal(err)
	}
	_, err = other.RetainExecution(
		otherCtx,
		flowy.ExecutionRetentionRequest{
			ExecutionID: before.ExecutionID,
			Revision:    head.Revision,
			Policy:      flowy.ExecutionRetentionPolicy{Label: "archive", DeletePayload: true},
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	_, err = other.RetainExecution(
		otherCtx,
		flowy.ExecutionRetentionRequest{
			ExecutionID: fork.ThreadID,
			Revision:    simulated.ResumeToken.SnapshotRevision,
			Policy:      flowy.ExecutionRetentionPolicy{Label: "head", KeepLast: 1},
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	pool.Close()
	otherPool.Close()
	finalCtx, finalPool := racePool(t)
	finalStore := mustExecutionStore(t, finalPool)
	retained, retainedErr := finalStore.LoadExecution(finalCtx, fork.ThreadID)
	_, missingSource := finalStore.LoadCheckpoint(finalCtx, before.ExecutionID, before.Revision)
	_, missingCreation := finalStore.LoadCheckpoint(finalCtx, fork.ThreadID, forkCreation.Revision)
	finalRunner := persistentReferenceRunnerOptions(
		t,
		finalStore,
		descriptor,
		"node",
		activity,
		flowy.DurableOptions{Owner: "worker", LeaseTTL: time.Minute, ForkPolicy: policy},
	)
	cached, cachedErr := finalRunner.Resume(finalCtx, simulated.ResumeToken)
	continued, realErr := finalRunner.Resume(finalCtx, transferred.token)
	if retainedErr != nil || retained.Fork == nil || *retained.Fork != *forkCreation.Fork ||
		!errors.Is(missingSource, flowy.ErrExecutionCheckpointUnavailable) ||
		!errors.Is(missingCreation, flowy.ErrExecutionCheckpointUnavailable) ||
		cachedErr != nil ||
		cached.Status != flowy.RunStatusCompleted ||
		realErr != nil ||
		continued.Status != flowy.RunStatusCompleted ||
		fakeCalls.Load() != 1 ||
		calls.Load() != 2 {
		t.Fatalf(
			"fork=%v source/creation=%v/%v cached=%v real=%v fake/live=%d/%d",
			retainedErr,
			missingSource,
			missingCreation,
			cachedErr,
			realErr,
			fakeCalls.Load(),
			calls.Load(),
		)
	}
}
