//go:build integration

package postgres

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/skosovsky/flowy"
)

type lifecyclePoolFaultStore struct {
	*ExecutionStore

	pool    *pgxpool.Pool
	lostAck bool
}

func (s lifecyclePoolFaultStore) CommitRollover(
	ctx context.Context,
	lease flowy.ExecutionLease,
	source flowy.HistoricalCheckpointReference,
	target flowy.ExecutionEnvelope,
) (flowy.RolloverReceipt, error) {
	if !s.lostAck {
		s.pool.Close()
	}
	receipt, err := s.ExecutionStore.CommitRollover(ctx, lease, source, target)
	if err == nil && s.lostAck {
		s.pool.Close()
		return flowy.RolloverReceipt{}, errors.New("lost rollover acknowledgement")
	}
	return receipt, err
}

func lifecyclePGRequest(
	descriptor flowy.ExecutionDescriptor,
	target string,
	projections *atomic.Int32,
) flowy.RolloverRequest {
	return flowy.RolloverRequest{
		SourceDescriptor: descriptor,
		TargetID:         target,
		DecisionID:       "continue",
		ProjectionLabel:  "host-pure",
		Policy: flowy.RolloverPolicy{
			Label:             "bounded",
			MaxRecords:        1,
			MaxAggregateBytes: 65536,
			MaxTargetBytes:    16384,
		},
		Project: func(p flowy.RolloverPayload) (flowy.RolloverPayload, error) { projections.Add(1); return p, nil },
	}
}

//nolint:gocognit // Keep before-commit and lost-ack assertions in one complete rollover and cleanup crash matrix.
func TestLifecyclePersistentRolloverAndCleanupRestart(t *testing.T) {
	for _, lostAck := range []bool{false, true} {
		t.Run(map[bool]string{false: "before commit", true: "lost ack"}[lostAck], func(t *testing.T) {
			// Arrange: actual original pool is interrupted, source already completed a bounded cycle.
			ctx, pool := racePool(t)
			if _, err := pool.Exec(ctx, ExecutionSchemaSQL()); err != nil {
				t.Fatal(err)
			}
			sourceID := testThreadID(t)
			targetID := sourceID + "-next"
			var calls, projections atomic.Int32
			activity := flowy.ActivityRequest{
				Key:            "write",
				Implementation: "host",
				Input:          []byte("input"),
				Dispatch: func(context.Context, flowy.ActivityInvocation) ([]byte, error) {
					calls.Add(1)
					return []byte("receipt"), nil
				},
			}
			descriptor := referenceDescriptor("lifecycle")
			base := NewExecutionStore(pool)
			source, err := persistentReferenceRunner(
				t,
				base,
				descriptor,
				"node",
				activity,
				nil,
			).Start(ctx, sourceID, intState{Value: 1})
			if err != nil {
				t.Fatal(err)
			}
			request := lifecyclePGRequest(descriptor, targetID, &projections)
			faulty := lifecyclePoolFaultStore{ExecutionStore: base, pool: pool, lostAck: lostAck}
			runner := persistentReferenceRunner(t, faulty, descriptor, "node", activity, nil)
			// Act: neither an outage nor an unknown response grants a second continuation.
			_, rolloverErr := runner.Rollover(ctx, source.ResumeToken, request)
			if rolloverErr == nil {
				t.Fatal("actual pool fault missing")
			}
			recoverCtx, recoverPool := racePool(t)
			recovery := NewExecutionStore(recoverPool)
			restart := persistentReferenceRunner(t, recovery, descriptor, "node", activity, nil)
			committedRevision := source.ResumeToken.SnapshotRevision
			if lostAck {
				committedRevision++
			}
			assertActivityCrashLeaseExpiry(recoverCtx, t, recoverPool, restart, source.ResumeToken, committedRevision)
			target, err := restart.Rollover(recoverCtx, source.ResumeToken, request)
			if err != nil {
				t.Fatal(err)
			}
			repeated, replayErr := restart.Rollover(recoverCtx, source.ResumeToken, request)
			current, loadErr := recovery.LoadExecution(recoverCtx, sourceID)
			_, sourceErr := restart.Resume(
				recoverCtx,
				flowy.ResumeToken{ThreadID: sourceID, SnapshotRevision: current.Revision},
			)
			// Assert: one transferred source and exactly one target, original work count remains one.
			expectedProjections := int32(2)
			if lostAck {
				expectedProjections = 1
			}
			if replayErr != nil || repeated != target || loadErr != nil ||
				!errors.Is(sourceErr, flowy.ErrExecutionTransferred) ||
				calls.Load() != 1 ||
				projections.Load() != expectedProjections {
				t.Fatalf(
					"replay=%v load=%v source=%v calls=%d projections=%d",
					replayErr,
					loadErr,
					sourceErr,
					calls.Load(),
					projections.Load(),
				)
			}
			receipt, err := recovery.RetainExecution(
				recoverCtx,
				flowy.ExecutionRetentionRequest{
					ExecutionID: sourceID,
					Revision:    current.Revision,
					Policy:      flowy.ExecutionRetentionPolicy{Label: "reclaim", DeletePayload: true},
				},
			)
			if err != nil || receipt.DeletedRevisions == 0 {
				t.Fatalf("cleanup=%+v/%v", receipt, err)
			}
			recoverPool.Close()
			finalCtx, finalPool := racePool(t)
			finalStore := NewExecutionStore(finalPool)
			finalRunner := persistentReferenceRunner(t, finalStore, descriptor, "node", activity, nil)
			replay, replayErr := finalRunner.Rollover(finalCtx, source.ResumeToken, request)
			_, missingErr := finalStore.LoadExecution(finalCtx, sourceID)
			_, historicalErr := finalStore.LoadCheckpoint(finalCtx, sourceID, source.ResumeToken.SnapshotRevision)
			completed, runErr := finalRunner.Resume(finalCtx, target)
			// Assert: payload reclamation retains receipt authority/target creation anchor across another independent pool.
			if replayErr != nil || replay != target ||
				!errors.Is(missingErr, flowy.ErrExecutionCheckpointUnavailable) ||
				!errors.Is(historicalErr, flowy.ErrExecutionCheckpointUnavailable) ||
				runErr != nil ||
				completed.Status != flowy.RunStatusCompleted ||
				calls.Load() != 2 ||
				projections.Load() != expectedProjections {
				t.Fatalf(
					"replay=%v missing=%v history=%v run=%v calls=%d projections=%d",
					replayErr,
					missingErr,
					historicalErr,
					runErr,
					calls.Load(),
					projections.Load(),
				)
			}
		})
	}
}
