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

func TestRolloverRejectedRequestsNeverPublishPartialTarget(t *testing.T) {
	cases := []struct {
		name      string
		change    func(*flowy.RolloverRequest, *flowy.ResumeToken)
		expected  error
		projected int32
	}{
		{
			name:     "stale revision",
			change:   func(_ *flowy.RolloverRequest, token *flowy.ResumeToken) { token.SnapshotRevision-- },
			expected: flowy.ErrConcurrencyConflict,
		},
		{
			name:     "wrong source descriptor",
			change:   func(r *flowy.RolloverRequest, _ *flowy.ResumeToken) { r.SourceDescriptor.GraphRevision = "foreign" },
			expected: flowy.ErrExecutionIncompatible,
		},
		{
			name:     "record limit",
			change:   func(r *flowy.RolloverRequest, _ *flowy.ResumeToken) { r.Policy.MaxRecords = 1 },
			expected: flowy.ErrExecutionLifecycleLimit,
		},
		{
			name:     "whole source byte limit",
			change:   func(r *flowy.RolloverRequest, _ *flowy.ResumeToken) { r.Policy.MaxAggregateBytes = 1 },
			expected: flowy.ErrExecutionLifecycleLimit,
		},
		{
			name:      "whole sealed target byte limit",
			change:    func(r *flowy.RolloverRequest, _ *flowy.ResumeToken) { r.Policy.MaxTargetBytes = 1 },
			expected:  flowy.ErrExecutionLifecycleLimit,
			projected: 1,
		},
		{name: "invalid state codec", change: func(r *flowy.RolloverRequest, _ *flowy.ResumeToken) {
			r.Project = wrapRolloverProjection(r.Project, func(p flowy.RolloverPayload) (flowy.RolloverPayload, error) {
				p.Progress.StatePayload = []byte("broken-json")
				return p, nil
			})
		}, expected: flowy.ErrExecutionIncompatible, projected: 1},
		{name: "invalid effects codec", change: func(r *flowy.RolloverRequest, _ *flowy.ResumeToken) {
			r.Project = wrapRolloverProjection(r.Project, func(p flowy.RolloverPayload) (flowy.RolloverPayload, error) {
				p.EffectsPayload = []byte("broken-json")
				return p, nil
			})
		}, expected: flowy.ErrExecutionIncompatible, projected: 1},
		{name: "projection refusal", change: func(r *flowy.RolloverRequest, _ *flowy.ResumeToken) {
			r.Project = wrapRolloverProjection(r.Project, func(p flowy.RolloverPayload) (flowy.RolloverPayload, error) {
				p.Progress.StatePayload[0] = '9'
				return p, errors.New("host refusal")
			})
		}, expected: flowy.ErrExecutionLifecycleUnsafe, projected: 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// Arrange: a valid boundary, before-image and detached host projection.
			ctx := context.Background()
			store := testutil.NewMemoryExecutionStore(nil)
			var calls, projections atomic.Int32
			runner := lifecycleRunner(t, store, 2, &calls)
			result, err := runner.Start(ctx, "source", 0)
			if err != nil {
				t.Fatal(err)
			}
			before, err := store.LoadExecution(ctx, "source")
			if err != nil {
				t.Fatal(err)
			}
			token := result.ResumeToken
			request := lifecycleRolloverRequest("target", 2, &projections)
			tc.change(&request, &token)
			// Act.
			_, rejected := runner.Rollover(ctx, token, request)
			after, loadErr := store.LoadExecution(ctx, "source")
			_, targetErr := store.LoadExecution(ctx, "target")
			receipt, receiptErr := store.LoadRollover(ctx, "source")
			// Assert: no transfer, target, mutable source change or extra dispatch.
			if !errors.Is(rejected, tc.expected) || loadErr != nil || before.Digest != after.Digest ||
				before.Revision != after.Revision ||
				!errors.Is(targetErr, flowy.ErrThreadNotFound) ||
				receiptErr != nil ||
				receipt != nil ||
				calls.Load() != 2 ||
				projections.Load() != tc.projected {
				t.Fatalf(
					"reject=%v source=%v target=%v receipt=%+v/%v calls=%d projections=%d",
					rejected,
					loadErr,
					targetErr,
					receipt,
					receiptErr,
					calls.Load(),
					projections.Load(),
				)
			}
		})
	}
}

func wrapRolloverProjection(
	original func(flowy.RolloverPayload) (flowy.RolloverPayload, error),
	transform func(flowy.RolloverPayload) (flowy.RolloverPayload, error),
) func(flowy.RolloverPayload) (flowy.RolloverPayload, error) {
	return func(p flowy.RolloverPayload) (flowy.RolloverPayload, error) {
		projected, err := original(p)
		if err != nil {
			return projected, err
		}
		return transform(projected)
	}
}

func TestRolloverSerializesResumeAndCleanupWhileInspectionAndForkStayHistorical(t *testing.T) {
	// Arrange: hold an explicit pure projection inside the source lease boundary.
	ctx := context.Background()
	store := testutil.NewMemoryExecutionStore(nil)
	var calls, projections atomic.Int32
	runner := lifecycleRunner(t, store, 2, &calls)
	source, err := runner.Start(ctx, "source", 0)
	if err != nil {
		t.Fatal(err)
	}
	before, err := store.LoadExecution(ctx, "source")
	if err != nil {
		t.Fatal(err)
	}
	entered, release := make(chan struct{}), make(chan struct{})
	request := lifecycleRolloverRequest("target", 2, &projections)
	request.Project = wrapRolloverProjection(
		request.Project,
		func(p flowy.RolloverPayload) (flowy.RolloverPayload, error) { close(entered); <-release; return p, nil },
	)
	type rolloverResult struct {
		token flowy.ResumeToken
		err   error
	}
	done := make(chan rolloverResult, 1)
	go func() {
		token, rolloverErr := runner.Rollover(ctx, source.ResumeToken, request)
		done <- rolloverResult{token: token, err: rolloverErr}
	}()
	<-entered
	// Act: source mutation is excluded; exact reads and a separately labelled fake fork remain historical.
	_, resumeErr := runner.Resume(ctx, source.ResumeToken)
	_, cleanupErr := store.RetainExecution(
		ctx,
		flowy.ExecutionRetentionRequest{
			ExecutionID: "source",
			Revision:    before.Revision,
			Policy:      flowy.ExecutionRetentionPolicy{Label: "keep", KeepLast: 1},
		},
	)
	_, competingErr := runner.Rollover(ctx, source.ResumeToken, lifecycleRolloverRequest("other", 2, &projections))
	exact, inspectErr := store.LoadCheckpoint(ctx, "source", before.Revision)
	fork, forkErr := runner.Fork(
		ctx,
		flowy.ForkRequest{
			Source: flowy.HistoricalCheckpointReference{
				ExecutionID: "source",
				Revision:    before.Revision,
				Digest:      before.Digest,
			},
			TargetID: "historical-fork",
			Transform: flowy.ForkTransform{
				Label:     "identity",
				Source:    before.Descriptor,
				Transform: func(p flowy.MigrationState) (flowy.MigrationState, error) { return p, nil },
			},
		},
	)
	close(release)
	transferred := <-done
	// Assert: one authorized continuation and no unintended dispatch; fork remains separate fake authority.
	if !errors.Is(resumeErr, flowy.ErrThreadLeaseBusy) || !errors.Is(cleanupErr, flowy.ErrThreadLeaseBusy) ||
		!errors.Is(competingErr, flowy.ErrThreadLeaseBusy) ||
		inspectErr != nil ||
		exact.Digest != before.Digest ||
		forkErr != nil ||
		transferred.err != nil ||
		transferred.token.ThreadID != "target" ||
		calls.Load() != 2 ||
		projections.Load() != 1 {
		t.Fatalf(
			"resume=%v cleanup=%v competing=%v inspection=%v fork=%v transfer=%v calls/projections=%d/%d",
			resumeErr,
			cleanupErr,
			competingErr,
			inspectErr,
			forkErr,
			transferred.err,
			calls.Load(),
			projections.Load(),
		)
	}
	head, err := store.LoadExecution(ctx, "source")
	if err != nil {
		t.Fatal(err)
	}
	_, err = store.RetainExecution(
		ctx,
		flowy.ExecutionRetentionRequest{
			ExecutionID: "source",
			Revision:    head.Revision,
			Policy:      flowy.ExecutionRetentionPolicy{Label: "archive", DeletePayload: true},
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	retainedFork, err := store.LoadExecution(ctx, fork.ThreadID)
	if err != nil || retainedFork.Fork == nil || retainedFork.Fork.Source.Digest != before.Digest {
		t.Fatalf("retained fork=%+v/%v", retainedFork, err)
	}
}

func TestRolloverInitialIntentAndFailedSourceRejectBeforeProjection(t *testing.T) {
	for _, failed := range []bool{false, true} {
		t.Run(map[bool]string{false: "initial intent", true: "failed terminal"}[failed], func(t *testing.T) {
			// Arrange: valid metadata describes a boundary that cannot grant continuation transfer.
			ctx := context.Background()
			store := testutil.NewMemoryExecutionStore(nil)
			var calls, projections atomic.Int32
			runner := lifecycleRunner(t, store, 1, &calls)
			lease, err := store.AcquireExecution(ctx, "source", "seed", time.Minute)
			if err != nil {
				t.Fatal(err)
			}
			envelope := flowy.ExecutionEnvelope{
				ExecutionID:    "source",
				Descriptor:     durableDescriptor("lifecycle"),
				Activation:     1,
				Progress:       flowy.MigrationState{ExecutionPointer: "work", StatePayload: []byte("0")},
				EffectsPayload: []byte("[]"),
			}
			if failed {
				envelope.RunMeta.StepCount = 1
				envelope.Terminal = &flowy.ExecutionTerminal{
					Status:  flowy.RunStatusFailed,
					Reason:  "failed",
					Failure: &flowy.ExecutionFailure{Message: "failed"},
				}
			}
			source, err := store.CommitExecution(ctx, 0, lease, envelope)
			if err != nil {
				t.Fatal(err)
			}
			if err = store.ReleaseExecution(ctx, lease); err != nil {
				t.Fatal(err)
			}
			// Act.
			_, rejected := runner.Rollover(
				ctx,
				flowy.ResumeToken{ThreadID: "source", SnapshotRevision: source.Revision},
				lifecycleRolloverRequest("target", 1, &projections),
			)
			after, loadErr := store.LoadExecution(ctx, "source")
			// Assert: failed/intention-only sources reject before any host callback.
			if !errors.Is(rejected, flowy.ErrExecutionLifecycleUnsafe) || loadErr != nil ||
				after.Digest != source.Digest ||
				calls.Load() != 0 ||
				projections.Load() != 0 {
				t.Fatalf(
					"reject=%v load=%v calls/projections=%d/%d",
					rejected,
					loadErr,
					calls.Load(),
					projections.Load(),
				)
			}
		})
	}
}
