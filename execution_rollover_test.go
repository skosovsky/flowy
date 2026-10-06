package flowy_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/skosovsky/flowy"
	"github.com/skosovsky/flowy/checkpoint"
	"github.com/skosovsky/flowy/testutil"
)

func lifecycleRunner(
	tb testing.TB,
	store flowy.ExecutionStore,
	records int,
	calls *atomic.Int32,
) *flowy.DurableRunner[int, flowy.NoEffect] {
	tb.Helper()
	b := flowy.NewGraph[int, flowy.NoEffect](func(_, u int) int { return u })
	b.AddNode("work", func(ctx context.Context, s int) (int, flowy.Directive, error) {
		for i := range records {
			_, err := flowy.CallActivity(
				ctx,
				flowy.ActivityRequest{
					Key:            fmt.Sprintf("write-%d", i),
					Implementation: "host-write",
					Input:          []byte("input"),
					Dispatch: func(context.Context, flowy.ActivityInvocation) ([]byte, error) {
						calls.Add(1)
						return []byte("receipt"), nil
					},
				},
			)
			if err != nil {
				return s, flowy.End(), err
			}
		}
		return s + 1, flowy.Completed(), nil
	}).AddNode("boundary", func(_ context.Context, s int) (int, flowy.Directive, error) {
		return s, flowy.Suspend("host rollover boundary"), nil
	}).AddEdge("work", "boundary").AllowNoOutgoingRoute("boundary").SetEntryPoint("work")
	graph, err := b.Compile()
	if err != nil {
		tb.Fatal(err)
	}
	runner, err := flowy.NewDurableRunner(
		graph,
		store,
		durableDescriptor("lifecycle"),
		checkpoint.JSONSerializer[int]{},
		checkpoint.JSONSerializer[[]flowy.NoEffect]{},
		flowy.DurableOptions{Owner: "host", LeaseTTL: time.Minute},
	)
	if err != nil {
		tb.Fatal(err)
	}
	return runner
}

func lifecycleRolloverRequest(target string, records int, projections *atomic.Int32) flowy.RolloverRequest {
	return flowy.RolloverRequest{
		SourceDescriptor: durableDescriptor("lifecycle"),
		TargetID:         target,
		DecisionID:       "cycle-continuation",
		ProjectionLabel:  "host-bounded-state-v1",
		Policy: flowy.RolloverPolicy{
			Label:             "bounded-cycle-v1",
			MaxRecords:        records,
			MaxAggregateBytes: 65536,
			MaxTargetBytes:    16384,
		},
		Project: func(p flowy.RolloverPayload) (flowy.RolloverPayload, error) {
			projections.Add(1)
			p.Progress.ExecutionPointer = "work"
			return p, nil
		},
	}
}

func TestExecutionRolloverHasOneContinuationAcrossCycles(t *testing.T) {
	// Arrange: each cycle has the same logical keys in a fresh execution namespace.
	ctx := context.Background()
	store := testutil.NewMemoryExecutionStore(nil)
	var calls, projections atomic.Int32
	runner := lifecycleRunner(t, store, 3, &calls)
	first, err := runner.Start(ctx, "cycle-0", 0)
	if err != nil || first.Status != flowy.RunStatusSuspended {
		t.Fatalf("result=%+v err=%v", first, err)
	}
	for cycle := range 3 {
		source := first.ResumeToken
		request := lifecycleRolloverRequest(fmt.Sprintf("cycle-%d", cycle+1), 3, &projections)
		// Act: publication and identical replay must create exactly one target.
		target, rolloverErr := runner.Rollover(ctx, source, request)
		replay, replayErr := runner.Rollover(ctx, source, request)
		changed := request
		changed.TargetID = "another-target"
		_, conflictErr := runner.Rollover(ctx, source, changed)
		head, loadErr := store.LoadExecution(ctx, source.ThreadID)
		_, sourceErr := runner.Resume(
			ctx,
			flowy.ResumeToken{ThreadID: source.ThreadID, SnapshotRevision: head.Revision},
		)
		first, err = runner.Resume(ctx, target)
		// Assert: source is no longer executable; repeated receipts never dispatch.
		if rolloverErr != nil || replayErr != nil || target != replay ||
			!errors.Is(conflictErr, flowy.ErrExecutionRolloverConflict) ||
			!errors.Is(sourceErr, flowy.ErrExecutionTransferred) ||
			loadErr != nil ||
			err != nil ||
			first.State != cycle+2 ||
			calls.Load() != int32(3*(cycle+2)) ||
			projections.Load() != int32(cycle+1) ||
			head.Terminal == nil ||
			head.Terminal.Status != flowy.RunStatusTransferred {
			t.Fatalf(
				"rollover=%v replay=%v conflict=%v source=%v resume=%v head=%+v calls=%d projections=%d",
				rolloverErr,
				replayErr,
				conflictErr,
				sourceErr,
				err,
				head,
				calls.Load(),
				projections.Load(),
			)
		}
		historical, historyErr := store.LoadCheckpoint(ctx, source.ThreadID, source.SnapshotRevision)
		if historyErr != nil || historical.Transfer != nil || historical.Terminal != nil {
			t.Fatalf("history=%+v err=%v", historical, historyErr)
		}
		targetHead, loadErr := store.LoadExecution(ctx, target.ThreadID)
		var journal map[string]flowy.ActivityRecord
		if loadErr != nil || json.Unmarshal(targetHead.JournalPayload, &journal) != nil || len(journal) != 3 ||
			targetHead.Rollover == nil {
			t.Fatalf("target=%+v load=%v", targetHead, loadErr)
		}
	}
}
