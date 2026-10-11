//go:build integration

package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/skosovsky/flowy"
	"github.com/skosovsky/flowy/checkpoint"
)

type discoveryReadProbe struct {
	DB

	heads atomic.Int64
}

func (db *discoveryReadProbe) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	if strings.Contains(sql, "h.payload") {
		db.heads.Add(1)
	}
	return db.DB.QueryRow(ctx, sql, args...)
}

func discoveryPerformancePool(tb testing.TB) (context.Context, *pgxpool.Pool) {
	tb.Helper()
	dsn := os.Getenv("FLOWY_TEST_DATABASE_URL")
	if dsn == "" {
		tb.Skip("real PostgreSQL not configured; performance gate unverified")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	tb.Cleanup(cancel)
	config, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		tb.Fatal(err)
	}
	config.MaxConns = 1
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(pool.Close)
	if _, err = pool.Exec(ctx, ExecutionSchemaSQL()); err != nil {
		tb.Fatal(err)
	}
	return ctx, pool
}

func insertDiscoveryTerminalArchive(ctx context.Context, tb testing.TB, pool *pgxpool.Pool, prefix string, count int) {
	tb.Helper()
	heads := make([][]any, 0, count)
	history := make([][]any, 0, count)
	for i := range count {
		var envelope flowy.ExecutionEnvelope
		envelope.ExecutionID = fmt.Sprintf("%sarchive-%06d", prefix, i)
		envelope.Revision = 1
		envelope.Descriptor = referenceDescriptor("archive")
		envelope.Progress.ExecutionPointer = "finished"
		envelope.Progress.StatePayload = []byte(`"` + strings.Repeat("s", 1024) + `"`)
		envelope.EffectsPayload = []byte("[]")
		envelope.Terminal = &flowy.ExecutionTerminal{Status: flowy.RunStatusCompleted, Reason: "end", Failure: nil}
		sealed, err := flowy.SealExecutionEnvelope(envelope)
		if err != nil {
			tb.Fatal(err)
		}
		payload, err := json.Marshal(sealed)
		if err != nil {
			tb.Fatal(err)
		}
		heads = append(heads, []any{sealed.ExecutionID, int64(1), true})
		history = append(history, []any{sealed.ExecutionID, int64(1), payload})
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		tb.Fatal(err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if _, err = tx.CopyFrom(
		ctx,
		pgx.Identifier{"flowy_executions"},
		[]string{"execution_id", "revision", "discovery_projected"},
		pgx.CopyFromRows(heads),
	); err != nil {
		tb.Fatal(err)
	}
	if _, err = tx.CopyFrom(
		ctx,
		pgx.Identifier{"flowy_execution_history"},
		[]string{"execution_id", "revision", "payload"},
		pgx.CopyFromRows(history),
	); err != nil {
		tb.Fatal(err)
	}
	if err = tx.Commit(ctx); err != nil {
		tb.Fatal(err)
	}
}

func prepareDiscoveryPerformance(
	ctx context.Context,
	tb testing.TB,
	pool *pgxpool.Pool,
	archive int,
) (*ExecutionStore, *discoveryReadProbe, time.Time) {
	tb.Helper()
	prefix := fmt.Sprintf("discovery-perf-%d-", time.Now().UnixNano())
	registerDiscoveryFixtureCleanup(tb, pool, prefix)
	insertDiscoveryTerminalArchive(ctx, tb, pool, prefix, archive)
	profile := postgresWaitProfile()
	profile.Label = prefix
	probe := &discoveryReadProbe{DB: pool}
	store, err := NewWaitExecutionStore(probe, profile)
	if err != nil {
		tb.Fatal(err)
	}
	deadline := time.Date(2026, 10, 6, 3, 0, 0, 0, time.UTC)
	graph := flowy.NewGraph[int, flowy.NoEffect](func(_, next int) int { return next })
	graph.AddNode("wait", func(_ context.Context, state int) (int, flowy.Directive, error) {
		spec := postgresWaitSpec(deadline)
		if state != 0 {
			spec.Deadline = spec.Deadline.Add(24 * time.Hour)
		}
		return state, flowy.Await(spec), nil
	}).AllowNoOutgoingRoute("wait").SetEntryPoint("wait")
	for _, id := range []string{"accepted", "timed-out"} {
		graph.AddNode(id, func(_ context.Context, state int) (int, flowy.Directive, error) { return state, flowy.End(), nil }).
			AllowNoOutgoingRoute(id)
	}
	compiled, err := graph.Compile()
	if err != nil {
		tb.Fatal(err)
	}
	runner, err := flowy.NewDurableRunner(
		compiled,
		store,
		referenceDescriptor("perf"),
		checkpoint.JSONSerializer[int]{},
		checkpoint.JSONSerializer[[]flowy.NoEffect]{},
		flowy.DurableOptions{Owner: "perf", LeaseTTL: time.Minute, WaitProfile: &profile},
	)
	if err != nil {
		tb.Fatal(err)
	}
	if _, err = runner.Start(ctx, prefix+"due", 0); err != nil {
		tb.Fatal(err)
	}
	// Real armed but future work gives the planner a meaningful candidate population.
	for i := range 256 {
		if _, err = runner.Start(ctx, fmt.Sprintf("%sfuture-%04d", prefix, i), 1); err != nil {
			tb.Fatal(err)
		}
	}
	if _, err = pool.Exec(ctx, `ANALYZE flowy_executions; ANALYZE flowy_due_candidates`); err != nil {
		tb.Fatal(err)
	}
	probe.heads.Store(0)
	return store, probe, deadline
}

func TestDiscoveryTerminalArchiveAndSingleConnection(t *testing.T) {
	// Arrange: a real sealed terminal archive and a one-connection pool.
	ctx, pool := discoveryPerformancePool(t)
	store, probe, deadline := prepareDiscoveryPerformance(ctx, t, pool, 20000)
	// Act: ordinary indexed polling never nests open rows with another query.
	started := time.Now()
	page, err := store.DiscoverDueWaits(ctx, deadline, DiscoveryCursor{}, 1)
	// Assert: archive size does not increase authoritative payload reads.
	if err != nil || len(page.Waits) != 1 || page.More || len(page.Diagnostics) != 0 || probe.heads.Load() != 1 {
		t.Fatalf("archive discovery: %+v err=%v payload reads=%d", page, err, probe.heads.Load())
	}
	t.Logf("terminal_archive=20000 due=1 decoded_heads=%d polling=%s", probe.heads.Load(), time.Since(started))
	assertDiscoveryExplainPlans(ctx, t, pool, store, deadline, page.Cursor)

	// Database outage remains an operation error instead of empty successful work.
	_, closedPool := discoveryPerformancePool(t)
	closedPool.Close()
	offline, err := NewWaitExecutionStore(closedPool, store.WaitCapabilities())
	if err != nil {
		t.Fatal(err)
	}
	if _, err = offline.DiscoverDueWaits(ctx, deadline, DiscoveryCursor{}, 1); err == nil {
		t.Fatal("closed database masked as empty page")
	}
}

func BenchmarkIndexedDiscoveryTerminalArchive(b *testing.B) {
	for _, archive := range []int{0, 20000} {
		b.Run(fmt.Sprintf("terminal-%d", archive), func(b *testing.B) {
			ctx, pool := discoveryPerformancePool(b)
			store, probe, deadline := prepareDiscoveryPerformance(ctx, b, pool, archive)
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				page, err := store.DiscoverDueWaits(ctx, deadline, DiscoveryCursor{}, 1)
				if err != nil || len(page.Waits) != 1 {
					b.Fatalf("poll: %+v %v", page, err)
				}
			}
			b.StopTimer()
			b.ReportMetric(float64(probe.heads.Load())/float64(b.N), "heads/poll")
			b.ReportMetric(float64(archive), "terminal-heads")
		})
	}
}

func registerDiscoveryFixtureCleanup(tb testing.TB, pool *pgxpool.Pool, prefix string) {
	tb.Helper()
	tb.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer cancel()
		tx, err := pool.Begin(ctx)
		if err != nil {
			tb.Error(err)
			return
		}
		defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
		for _, table := range []string{"flowy_due_candidates", "flowy_execution_history", "flowy_executions"} {
			// Table identifiers are closed test-owned constants; values remain parameters.
			if _, err = tx.Exec(ctx, "DELETE FROM "+table+" WHERE starts_with(execution_id,$1)", prefix); err != nil {
				tb.Error(err)
				return
			}
		}
		if err = tx.Commit(ctx); err != nil {
			tb.Error(err)
		}
	})
}

func assertDiscoveryExplainPlans(
	ctx context.Context,
	t *testing.T,
	pool *pgxpool.Pool,
	store *ExecutionStore,
	deadline time.Time,
	next DiscoveryCursor,
) {
	t.Helper()
	cursor, err := store.discoveryPosition(deadline, DiscoveryCursor{}, 1, "wait")
	if err != nil {
		t.Fatal(err)
	}
	for _, position := range []DiscoveryCursor{cursor, next} {
		query, args := dueSelection(position, 1)
		var plan []byte
		if err = pool.QueryRow(ctx, "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) "+query, args...).Scan(&plan); err != nil {
			t.Fatal(err)
		}
		t.Logf("due-plan cursor=%+v: %s", position, plan)
		if !strings.Contains(string(plan), "flowy_due_order") {
			t.Fatal("due selection lacks index")
		}
		if strings.Contains(string(plan), "flowy_execution_history") ||
			strings.Contains(string(plan), "flowy_executions") {
			t.Fatal("due selection touches archive")
		}
		if !position.AfterDeadline.IsZero() && !discoveryPlanTupleBound(plan) {
			t.Fatal("next cursor is not an index bound")
		}
	}
	var plan []byte
	if err = pool.QueryRow(ctx, `EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT EXISTS(SELECT 1 FROM flowy_executions WHERE revision>0 AND NOT payload_deleted AND NOT discovery_projected)`).
		Scan(&plan); err != nil {
		t.Fatal(err)
	}
	t.Logf("readiness-plan: %s", plan)
	if !strings.Contains(string(plan), "flowy_discovery_unready") {
		t.Fatal("readiness lacks partial index")
	}
}

type discoveryPlanNode struct {
	IndexName string              `json:"Index Name"`
	IndexCond string              `json:"Index Cond"`
	Plans     []discoveryPlanNode `json:"Plans"`
}

func discoveryPlanTupleBound(payload []byte) bool {
	var plans []struct {
		Plan discoveryPlanNode `json:"Plan"`
	}
	if json.Unmarshal(payload, &plans) != nil || len(plans) != 1 {
		return false
	}
	return discoveryNodeTupleBound(plans[0].Plan)
}
func discoveryNodeTupleBound(node discoveryPlanNode) bool {
	if node.IndexName == "flowy_due_order" && strings.Contains(node.IndexCond, "ROW(deadline") {
		return true
	}
	return slices.ContainsFunc(node.Plans, discoveryNodeTupleBound)
}
