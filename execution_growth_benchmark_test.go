package flowy_test

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/skosovsky/flowy"
	"github.com/skosovsky/flowy/checkpoint"
	"github.com/skosovsky/flowy/testutil"
)

type executionGrowthStore struct {
	flowy.ExecutionStore

	commits        int
	aggregateBytes int
	historyBytes   int
}

func (s *executionGrowthStore) CommitExecution(
	ctx context.Context,
	rev uint64,
	lease flowy.ExecutionLease,
	e flowy.ExecutionEnvelope,
) (flowy.ExecutionEnvelope, error) {
	committed, err := s.ExecutionStore.CommitExecution(ctx, rev, lease, e)
	if err != nil {
		return committed, err
	}
	encoded, err := json.Marshal(committed)
	if err != nil {
		return committed, err
	}
	s.commits++
	s.historyBytes += len(encoded)
	s.aggregateBytes = max(s.aggregateBytes, len(encoded))
	return committed, nil
}

func growthRunner(
	tb testing.TB,
	store flowy.ExecutionStore,
	records int,
	fanout bool,
) *flowy.DurableRunner[int, flowy.NoEffect] {
	tb.Helper()
	payload := make([]byte, 512)
	builder := flowy.NewGraph[int, flowy.NoEffect](func(_, u int) int { return u })
	builder.AddNode("growth", func(ctx context.Context, s int) (int, flowy.Directive, error) {
		if fanout {
			children := make([]flowy.ChildSpec, records)
			for i := range children {
				children[i] = flowy.ChildSpec{ID: fmt.Sprintf("child-%04d", i), Input: payload}
			}
			group, err := flowy.RunChildren(
				ctx,
				flowy.ChildGroupPlan{
					Key:            "group",
					Label:          "bytes",
					MergeLabel:     "count",
					BudgetLabel:    "none",
					CancelLabel:    "host",
					MaxConcurrency: 1,
					FailurePolicy:  flowy.ChildCollectErrors,
					Children:       children,
				},
				nil,
				func(context.Context, flowy.ChildInvocation) (flowy.ChildResult, error) {
					return flowy.ChildResult{State: flowy.ChildCompleted, Payload: payload}, nil
				},
			)
			if err == nil {
				_, err = flowy.JoinChildren(
					ctx,
					group,
					func(context.Context, []flowy.ChildRecord) ([]byte, error) { return nil, nil },
				)
			}
			return s + records, flowy.End(), err
		}
		for i := range records {
			_, err := flowy.CallActivity(
				ctx,
				flowy.ActivityRequest{
					Key:            fmt.Sprintf("activity-%04d", i),
					Implementation: "bytes",
					Input:          payload,
					Dispatch:       func(context.Context, flowy.ActivityInvocation) ([]byte, error) { return payload, nil },
				},
			)
			if err != nil {
				return s, flowy.End(), err
			}
		}
		return s + records, flowy.End(), nil
	}).SetEntryPoint("growth").AllowNoOutgoingRoute("growth")
	graph, err := builder.Compile()
	if err != nil {
		tb.Fatal(err)
	}
	runner, err := flowy.NewDurableRunner(
		graph,
		store,
		durableDescriptor("growth"),
		checkpoint.JSONSerializer[int]{},
		checkpoint.JSONSerializer[[]flowy.NoEffect]{},
		flowy.DurableOptions{Owner: "measure", LeaseTTL: time.Minute},
	)
	if err != nil {
		tb.Fatal(err)
	}
	return runner
}

func BenchmarkExecutionGrowth(b *testing.B) {
	for _, fanout := range []bool{false, true} {
		for _, records := range []int{16, 64, 256} {
			b.Run(fmt.Sprintf("fanout=%t/records=%d", fanout, records), func(b *testing.B) {
				b.ReportAllocs()
				for range b.N {
					store := &executionGrowthStore{ExecutionStore: testutil.NewMemoryExecutionStore(nil)}
					runner := growthRunner(b, store, records, fanout)
					result, err := runner.Start(context.Background(), "growth", 0)
					if err != nil || result.State != records {
						b.Fatalf("run=%+v err=%v", result, err)
					}
					b.ReportMetric(float64(store.commits), "commits/op")
					b.ReportMetric(float64(store.aggregateBytes), "aggregate-B/op")
					b.ReportMetric(float64(store.historyBytes), "history-B/op")
				}
			})
		}
	}
}

func (s *executionGrowthStore) LoadRollover(ctx context.Context, id string) (*flowy.RolloverReceipt, error) {
	return s.ExecutionStore.(flowy.ExecutionRolloverStore).LoadRollover(ctx, id)
}

func (s *executionGrowthStore) CommitRollover(
	ctx context.Context,
	lease flowy.ExecutionLease,
	source flowy.HistoricalCheckpointReference,
	target flowy.ExecutionEnvelope,
) (flowy.RolloverReceipt, error) {
	receipt, err := s.ExecutionStore.(flowy.ExecutionRolloverStore).CommitRollover(ctx, lease, source, target)
	if err != nil {
		return receipt, err
	}
	for _, id := range []string{source.ExecutionID, target.ExecutionID} {
		e, loadErr := s.LoadExecution(ctx, id)
		if loadErr != nil {
			return receipt, loadErr
		}
		encoded, encodeErr := json.Marshal(e)
		if encodeErr != nil {
			return receipt, encodeErr
		}
		s.commits++
		s.historyBytes += len(encoded)
		s.aggregateBytes = max(s.aggregateBytes, len(encoded))
	}
	return receipt, nil
}

// BenchmarkExecutionGrowthRollover repeats the identical 512-byte activities or
// children workload in 16-record cycles. One current full checkpoint remains;
// transferred payloads are reclaimed, while permanent metadata is measured.
func BenchmarkExecutionGrowthRollover(b *testing.B) {
	const cycleRecords = 16
	for _, fanout := range []bool{false, true} {
		for _, records := range []int{16, 64, 256} {
			b.Run(fmt.Sprintf("fanout=%t/records=%d", fanout, records), func(b *testing.B) {
				b.ReportAllocs()
				for range b.N {
					benchmarkGrowthCycles(b, records, fanout, cycleRecords)
				}
			})
		}
	}
}

func benchmarkGrowthCycles(b *testing.B, records int, fanout bool, cycleRecords int) {
	b.Helper()
	ctx := context.Background()
	memory := testutil.NewMemoryExecutionStore(nil)
	store := &executionGrowthStore{ExecutionStore: memory}
	runner := growthRunner(b, store, cycleRecords, fanout)
	result, err := runner.Start(ctx, "growth-0000", 0)
	if err != nil {
		b.Fatal(err)
	}
	for cycle := 1; cycle < records/cycleRecords; cycle++ {
		sourceID := result.ResumeToken.ThreadID
		request := flowy.RolloverRequest{
			SourceDescriptor: durableDescriptor("growth"), TargetID: fmt.Sprintf("growth-%04d", cycle),
			DecisionID: "next", ProjectionLabel: "identity", Policy: flowy.RolloverPolicy{
				Label: "16-record-cycle", MaxRecords: cycleRecords, MaxAggregateBytes: 131072, MaxTargetBytes: 8192,
			}, Project: func(p flowy.RolloverPayload) (flowy.RolloverPayload, error) { return p, nil },
		}
		token, rolloverErr := runner.Rollover(ctx, result.ResumeToken, request)
		if rolloverErr != nil {
			b.Fatal(rolloverErr)
		}
		transferred, loadErr := memory.LoadExecution(ctx, sourceID)
		if loadErr != nil {
			b.Fatal(loadErr)
		}
		_, cleanupErr := memory.RetainExecution(ctx, flowy.ExecutionRetentionRequest{
			ExecutionID: sourceID, Revision: transferred.Revision,
			Policy: flowy.ExecutionRetentionPolicy{Label: "one-cycle", DeletePayload: true},
		})
		if cleanupErr != nil {
			b.Fatal(cleanupErr)
		}
		result, err = runner.Resume(ctx, token)
		if err != nil {
			b.Fatal(err)
		}
	}
	_, err = memory.RetainExecution(ctx, flowy.ExecutionRetentionRequest{
		ExecutionID: result.ResumeToken.ThreadID, Revision: result.ResumeToken.SnapshotRevision,
		Policy: flowy.ExecutionRetentionPolicy{Label: "one-cycle", KeepLast: 1},
	})
	if err != nil || result.State != records {
		b.Fatalf("result=%+v err=%v", result, err)
	}
	usage, err := memory.StorageUsage()
	if err != nil {
		b.Fatal(err)
	}
	if usage.Revisions != 1 || usage.PayloadBytes > 131072 {
		b.Fatalf("unbounded retained payload: %+v", usage)
	}
	b.ReportMetric(float64(store.commits), "commits/op")
	b.ReportMetric(float64(store.aggregateBytes), "aggregate-B/op")
	b.ReportMetric(float64(store.historyBytes), "written-history-B/op")
	b.ReportMetric(float64(usage.PayloadBytes), "retained-payload-B/op")
	b.ReportMetric(float64(usage.MetadataBytes), "metadata-B/op")
}
