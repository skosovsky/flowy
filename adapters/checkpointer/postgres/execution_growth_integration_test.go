//go:build integration

package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/skosovsky/flowy"
	"github.com/skosovsky/flowy/checkpoint"
)

type pgGrowthStore struct {
	*ExecutionStore

	commits int
	written int
	peak    int
}

func (s *pgGrowthStore) measure(e flowy.ExecutionEnvelope) error {
	encoded, err := json.Marshal(e)
	if err != nil {
		return err
	}
	s.commits++
	s.written += len(encoded)
	s.peak = max(s.peak, len(encoded))
	return nil
}

func (s *pgGrowthStore) CommitExecution(
	ctx context.Context,
	revision uint64,
	lease flowy.ExecutionLease,
	e flowy.ExecutionEnvelope,
) (flowy.ExecutionEnvelope, error) {
	committed, err := s.ExecutionStore.CommitExecution(ctx, revision, lease, e)
	if err == nil {
		err = s.measure(committed)
	}
	return committed, err
}

func (s *pgGrowthStore) CommitRollover(
	ctx context.Context,
	lease flowy.ExecutionLease,
	source flowy.HistoricalCheckpointReference,
	target flowy.ExecutionEnvelope,
) (flowy.RolloverReceipt, error) {
	receipt, err := s.ExecutionStore.CommitRollover(ctx, lease, source, target)
	if err != nil {
		return receipt, err
	}
	for _, id := range []string{source.ExecutionID, target.ExecutionID} {
		e, loadErr := s.LoadExecution(ctx, id)
		if loadErr != nil {
			return receipt, loadErr
		}
		if err = s.measure(e); err != nil {
			return receipt, err
		}
	}
	return receipt, nil
}

func pgGrowthRunner(
	b *testing.B,
	store flowy.ExecutionStore,
	records int,
	fanout bool,
) *flowy.DurableRunner[int, flowy.NoEffect] {
	b.Helper()
	payload := make([]byte, 512)
	builder := flowy.NewGraph[int, flowy.NoEffect](func(_, u int) int { return u })
	builder.AddNode("growth", func(ctx context.Context, state int) (int, flowy.Directive, error) {
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
			return state + records, flowy.End(), err
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
				return state, flowy.End(), err
			}
		}
		return state + records, flowy.End(), nil
	}).SetEntryPoint("growth").AllowNoOutgoingRoute("growth")
	graph, err := builder.Compile()
	if err != nil {
		b.Fatal(err)
	}
	runner, err := flowy.NewDurableRunner(
		graph,
		store,
		referenceDescriptor("growth"),
		checkpoint.JSONSerializer[int]{},
		checkpoint.JSONSerializer[[]flowy.NoEffect]{},
		flowy.DurableOptions{Owner: "measure", LeaseTTL: time.Minute},
	)
	if err != nil {
		b.Fatal(err)
	}
	return runner
}

func BenchmarkPostgresExecutionGrowth(b *testing.B) {
	dsn := os.Getenv("FLOWY_TEST_DATABASE_URL")
	if dsn == "" {
		b.Skip("FLOWY_TEST_DATABASE_URL missing; backend not measured")
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		b.Fatal(err)
	}
	defer pool.Close()
	if _, err = pool.Exec(ctx, ExecutionSchemaSQL()); err != nil {
		b.Fatal(err)
	}
	for _, rollover := range []bool{false, true} {
		for _, fanout := range []bool{false, true} {
			for _, records := range []int{16, 64, 256} {
				b.Run(fmt.Sprintf("rollover=%t/fanout=%t/records=%d", rollover, fanout, records), func(b *testing.B) {
					b.ReportAllocs()
					for iteration := range b.N {
						pgGrowthWorkload(ctx, b, pool, rollover, fanout, records, iteration)
					}
				})
			}
		}
	}
}

func pgGrowthWorkload(
	ctx context.Context,
	b *testing.B,
	pool *pgxpool.Pool,
	rollover, fanout bool,
	records, iteration int,
) {
	b.Helper()
	cycleRecords := records
	if rollover {
		cycleRecords = 16
	}
	store := &pgGrowthStore{ExecutionStore: NewExecutionStore(pool)}
	runner := pgGrowthRunner(b, store, cycleRecords, fanout)
	baseID := fmt.Sprintf("bench-%d-%d", time.Now().UnixNano(), iteration)
	ids := []string{baseID}
	result, err := runner.Start(ctx, baseID, 0)
	if err != nil {
		b.Fatal(err)
	}
	for cycle := 1; cycle < records/cycleRecords; cycle++ {
		sourceID := result.ResumeToken.ThreadID
		targetID := fmt.Sprintf("%s-%04d", baseID, cycle)
		ids = append(ids, targetID)
		request := flowy.RolloverRequest{
			SourceDescriptor: referenceDescriptor("growth"),
			TargetID:         targetID,
			DecisionID:       "next",
			ProjectionLabel:  "identity",
			Policy: flowy.RolloverPolicy{
				Label:             "16-record-cycle",
				MaxRecords:        cycleRecords,
				MaxAggregateBytes: 131072,
				MaxTargetBytes:    8192,
			},
			Project: func(p flowy.RolloverPayload) (flowy.RolloverPayload, error) { return p, nil },
		}
		token, rolloverErr := runner.Rollover(ctx, result.ResumeToken, request)
		if rolloverErr != nil {
			b.Fatal(rolloverErr)
		}
		source, loadErr := store.LoadExecution(ctx, sourceID)
		if loadErr != nil {
			b.Fatal(loadErr)
		}
		_, cleanupErr := store.RetainExecution(
			ctx,
			flowy.ExecutionRetentionRequest{
				ExecutionID: sourceID,
				Revision:    source.Revision,
				Policy:      flowy.ExecutionRetentionPolicy{Label: "one-cycle", DeletePayload: true},
			},
		)
		if cleanupErr != nil {
			b.Fatal(cleanupErr)
		}
		result, err = runner.Resume(ctx, token)
		if err != nil {
			b.Fatal(err)
		}
	}
	if result.State != records {
		b.Fatalf("state=%d expected=%d", result.State, records)
	}
	if rollover {
		_, err = store.RetainExecution(
			ctx,
			flowy.ExecutionRetentionRequest{
				ExecutionID: result.ResumeToken.ThreadID,
				Revision:    result.ResumeToken.SnapshotRevision,
				Policy:      flowy.ExecutionRetentionPolicy{Label: "one-cycle", KeepLast: 1},
			},
		)
		if err != nil {
			b.Fatal(err)
		}
	}
	var payloadBytes, metadataBytes, revisions int64
	if err = pool.QueryRow(ctx, `SELECT COALESCE(sum(octet_length(payload::text)),0),count(*) FROM flowy_execution_history WHERE execution_id=ANY($1::text[])`, ids).
		Scan(&payloadBytes, &revisions); err != nil {
		b.Fatal(err)
	}
	if err = pool.QueryRow(ctx, `SELECT COALESCE(sum(octet_length(to_jsonb(e)::text)),0) FROM flowy_executions e WHERE execution_id=ANY($1::text[])`, ids).
		Scan(&metadataBytes); err != nil {
		b.Fatal(err)
	}
	if rollover && (revisions != 1 || payloadBytes > 131072) {
		b.Fatalf("unbounded retained data: %d bytes %d revisions", payloadBytes, revisions)
	}
	b.ReportMetric(float64(store.commits), "commits/op")
	b.ReportMetric(float64(store.peak), "aggregate-JSON-B/op")
	b.ReportMetric(float64(store.written), "written-JSON-B/op")
	b.ReportMetric(float64(payloadBytes), "retained-SQL-B/op")
	b.ReportMetric(float64(metadataBytes), "metadata-SQL-B/op")
	b.ReportMetric(float64(revisions), "retained-revisions/op")
}
