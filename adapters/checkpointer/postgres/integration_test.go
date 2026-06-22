//go:build integration

package postgres

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	pglease "github.com/skosovsky/flowy/adapters/lease/postgres"

	"github.com/skosovsky/flowy"
	"github.com/skosovsky/flowy/checkpoint"
)

type intState struct {
	Value int `json:"value"`
}

func testThreadID(t *testing.T) string {
	t.Helper()
	return t.Name() + "-" + time.Now().UTC().Format("20060102150405.000000000")
}

func testSnapshot(threadID string, revision uint64, value int) flowy.Snapshot[intState, string] {
	return flowy.Snapshot[intState, string]{
		ThreadID:         threadID,
		Revision:         revision,
		ExecutionPointer: "n1",
		State:            intState{Value: value},
	}
}

// E2E: paired postgres lease adapter blocks DeleteIfIdle until Release.
func TestE2ELeaseAcquireBlocksDeleteUntilRelease(t *testing.T) {
	dsn := os.Getenv("FLOWY_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("FLOWY_TEST_DATABASE_URL not set")
	}

	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer pool.Close()

	if _, err := pool.Exec(ctx, SchemaSQL()); err != nil {
		t.Fatalf("schema: %v", err)
	}

	cp := NewCheckpointer[intState, string](pool, checkpoint.JSONSerializer[intState]{})
	leaseMgr := pglease.NewLeaseManager(pool)
	threadID := testThreadID(t)

	if _, err := cp.Save(ctx, 0, testSnapshot(threadID, 1, 1)); err != nil {
		t.Fatalf("save: %v", err)
	}
	if err := leaseMgr.Acquire(ctx, threadID, "worker", time.Minute); err != nil {
		t.Fatalf("acquire: %v", err)
	}
	if err := cp.DeleteIfIdle(ctx, threadID); !errors.Is(err, flowy.ErrThreadLeaseBusy) {
		t.Fatalf("expected ErrThreadLeaseBusy, got %v", err)
	}
	if err := leaseMgr.Release(ctx, threadID, "worker"); err != nil {
		t.Fatalf("release: %v", err)
	}
	if err := cp.DeleteIfIdle(ctx, threadID); err != nil {
		t.Fatalf("delete after release: %v", err)
	}
}

func TestOCCConcurrencyConflict(t *testing.T) {
	dsn := os.Getenv("FLOWY_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("FLOWY_TEST_DATABASE_URL not set")
	}

	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer pool.Close()

	if _, err := pool.Exec(ctx, SchemaSQL()); err != nil {
		t.Fatalf("schema: %v", err)
	}

	cp := NewCheckpointer[intState, string](pool, checkpoint.JSONSerializer[intState]{})
	threadID := testThreadID(t)
	if _, err := cp.Save(ctx, 0, testSnapshot(threadID, 1, 1)); err != nil {
		t.Fatalf("initial save: %v", err)
	}
	_, err = cp.Save(ctx, 0, testSnapshot(threadID, 2, 2))
	if !errors.Is(err, flowy.ErrConcurrencyConflict) {
		t.Fatalf("expected ErrConcurrencyConflict, got %v", err)
	}
}

func TestSaveWithOutboxRollbackOnEnqueueFail(t *testing.T) {
	dsn := os.Getenv("FLOWY_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("FLOWY_TEST_DATABASE_URL not set")
	}

	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer pool.Close()

	if _, err := pool.Exec(ctx, SchemaSQL()); err != nil {
		t.Fatalf("schema: %v", err)
	}

	cp := NewCheckpointer[intState, string](pool, checkpoint.JSONSerializer[intState]{})
	threadID := testThreadID(t)
	snap := testSnapshot(threadID, 1, 1)
	snap.RunMeta = flowy.RunMetadata{HandoffStatus: flowy.HandoffStatusEnqueued}
	_, err = cp.SaveWithOutbox(ctx, 0, snap, func(
		context.Context,
		flowy.TransactionHandle,
		uint64,
	) error {
		return errors.New("enqueue failed")
	})
	if err == nil {
		t.Fatal("expected enqueue failure")
	}
	_, _, loadErr := cp.Load(ctx, threadID)
	if !errors.Is(loadErr, flowy.ErrThreadNotFound) {
		t.Fatalf("expected no checkpoint after rollback, got %v", loadErr)
	}
}

func TestSaveWithOutboxSuccess(t *testing.T) {
	dsn := os.Getenv("FLOWY_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("FLOWY_TEST_DATABASE_URL not set")
	}

	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer pool.Close()

	if _, err := pool.Exec(ctx, SchemaSQL()); err != nil {
		t.Fatalf("schema: %v", err)
	}
	if _, err := pool.Exec(ctx, OutboxSchemaSQL()); err != nil {
		t.Fatalf("outbox schema: %v", err)
	}

	cp := NewCheckpointer[intState, string](pool, checkpoint.JSONSerializer[intState]{})
	threadID := testThreadID(t)
	snap := testSnapshot(threadID, 1, 42)
	snap.RunMeta = flowy.RunMetadata{HandoffStatus: flowy.HandoffStatusEnqueued}
	var sawTx bool
	rev, err := cp.SaveWithOutbox(ctx, 0, snap, func(
		enqueueCtx context.Context,
		txHandle flowy.TransactionHandle,
		savedRevision uint64,
	) error {
		tx, ok := PgxTxFromHandle(txHandle)
		if !ok {
			return errors.New("expected pgx.Tx transaction handle")
		}
		if savedRevision != 1 {
			return fmt.Errorf("unexpected saved revision: %d", savedRevision)
		}
		sawTx = true
		_, execErr := tx.Exec(enqueueCtx,
			`INSERT INTO flowy_handoff_outbox (thread_id, snapshot_revision) VALUES ($1, $2)`,
			threadID, savedRevision,
		)
		return execErr
	})
	if err != nil {
		t.Fatalf("SaveWithOutbox: %v", err)
	}
	if !sawTx {
		t.Fatal("enqueue callback did not receive pgx.Tx")
	}
	if rev != 1 {
		t.Fatalf("expected revision 1, got %d", rev)
	}
	loaded, loadedRev, err := cp.Load(ctx, threadID)
	if err != nil {
		t.Fatalf("load checkpoint: %v", err)
	}
	if loadedRev != 1 || loaded.State.Value != 42 {
		t.Fatalf("unexpected checkpoint: rev=%d state=%+v", loadedRev, loaded.State)
	}
	var outboxCount int
	if err := pool.QueryRow(ctx,
		`SELECT COUNT(*) FROM flowy_handoff_outbox WHERE thread_id = $1 AND snapshot_revision = $2`,
		threadID, int64(1),
	).Scan(&outboxCount); err != nil {
		t.Fatalf("count outbox: %v", err)
	}
	if outboxCount != 1 {
		t.Fatalf("expected one outbox row, got %d", outboxCount)
	}
}

func TestSaveWithOutboxOCCConflict(t *testing.T) {
	dsn := os.Getenv("FLOWY_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("FLOWY_TEST_DATABASE_URL not set")
	}

	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer pool.Close()

	if _, err := pool.Exec(ctx, SchemaSQL()); err != nil {
		t.Fatalf("schema: %v", err)
	}
	if _, err := pool.Exec(ctx, OutboxSchemaSQL()); err != nil {
		t.Fatalf("outbox schema: %v", err)
	}

	cp := NewCheckpointer[intState, string](pool, checkpoint.JSONSerializer[intState]{})
	threadID := testThreadID(t)
	if _, err := cp.Save(ctx, 0, testSnapshot(threadID, 1, 1)); err != nil {
		t.Fatalf("initial save: %v", err)
	}
	snap := testSnapshot(threadID, 2, 2)
	snap.RunMeta = flowy.RunMetadata{HandoffStatus: flowy.HandoffStatusEnqueued}
	_, err = cp.SaveWithOutbox(ctx, 0, snap, func(
		context.Context,
		flowy.TransactionHandle,
		uint64,
	) error {
		return nil
	})
	if !errors.Is(err, flowy.ErrConcurrencyConflict) {
		t.Fatalf("expected ErrConcurrencyConflict, got %v", err)
	}
	loaded, loadedRev, loadErr := cp.Load(ctx, threadID)
	if loadErr != nil {
		t.Fatalf("load after OCC conflict: %v", loadErr)
	}
	if loadedRev != 1 || loaded.State.Value != 1 {
		t.Fatalf("checkpoint mutated on OCC conflict: rev=%d state=%+v", loadedRev, loaded.State)
	}
}

func TestSaveWithOutboxRollbackOnOutboxInsertFail(t *testing.T) {
	dsn := os.Getenv("FLOWY_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("FLOWY_TEST_DATABASE_URL not set")
	}

	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer pool.Close()

	if _, err := pool.Exec(ctx, SchemaSQL()); err != nil {
		t.Fatalf("schema: %v", err)
	}
	if _, err := pool.Exec(ctx, OutboxSchemaSQL()); err != nil {
		t.Fatalf("outbox schema: %v", err)
	}

	cp := NewCheckpointer[intState, string](pool, checkpoint.JSONSerializer[intState]{})
	threadID := testThreadID(t)
	snap := testSnapshot(threadID, 1, 1)
	snap.RunMeta = flowy.RunMetadata{HandoffStatus: flowy.HandoffStatusEnqueued}
	_, err = cp.SaveWithOutbox(ctx, 0, snap, func(
		enqueueCtx context.Context,
		txHandle flowy.TransactionHandle,
		_ uint64,
	) error {
		tx, ok := PgxTxFromHandle(txHandle)
		if !ok {
			return errors.New("missing pgx.Tx")
		}
		_, execErr := tx.Exec(enqueueCtx,
			`INSERT INTO flowy_handoff_outbox_nonexistent (thread_id) VALUES ($1)`,
			threadID,
		)
		return execErr
	})
	if err == nil {
		t.Fatal("expected outbox insert failure")
	}
	_, _, loadErr := cp.Load(ctx, threadID)
	if !errors.Is(loadErr, flowy.ErrThreadNotFound) {
		t.Fatalf("expected no checkpoint after outbox rollback, got %v", loadErr)
	}
	var outboxCount int
	_ = pool.QueryRow(ctx, `SELECT COUNT(*) FROM flowy_handoff_outbox WHERE thread_id = $1`, threadID).Scan(&outboxCount)
	if outboxCount != 0 {
		t.Fatalf("expected no outbox rows after rollback, got %d", outboxCount)
	}
}

type runnerHandoffState struct {
	N int `json:"n"`
}

type stubHandoffOutbox struct {
	calls   []flowy.HandoffIntent
	txCalls []flowy.HandoffIntent
	err     error
}

func (s *stubHandoffOutbox) EnqueueIntent(_ context.Context, intent flowy.HandoffIntent) error {
	s.calls = append(s.calls, intent)
	return s.err
}

func (s *stubHandoffOutbox) EnqueueIntentTx(
	ctx context.Context,
	tx flowy.TransactionHandle,
	intent flowy.HandoffIntent,
) error {
	s.txCalls = append(s.txCalls, intent)
	if s.err != nil {
		return s.err
	}
	pgxTx, ok := tx.(pgx.Tx)
	if !ok {
		return errors.New("expected pgx.Tx transaction handle")
	}
	_, err := pgxTx.Exec(ctx,
		`INSERT INTO flowy_handoff_outbox (thread_id, snapshot_revision) VALUES ($1, $2)`,
		intent.ThreadID, int64(intent.SnapshotRevision),
	)
	return err
}

func pgRunnerPool(t *testing.T) (*pgxpool.Pool, *Checkpointer[runnerHandoffState, flowy.NoEffect]) {
	t.Helper()
	dsn := os.Getenv("FLOWY_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("FLOWY_TEST_DATABASE_URL not set")
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { pool.Close() })
	if _, err := pool.Exec(ctx, SchemaSQL()); err != nil {
		t.Fatalf("schema: %v", err)
	}
	if _, err := pool.Exec(ctx, OutboxSchemaSQL()); err != nil {
		t.Fatalf("outbox schema: %v", err)
	}
	cp := NewCheckpointer[runnerHandoffState, flowy.NoEffect](pool, checkpoint.JSONSerializer[runnerHandoffState]{})
	return pool, cp
}

func pgHandoffGraph(t *testing.T) *flowy.Graph[runnerHandoffState, flowy.NoEffect] {
	t.Helper()
	b := flowy.NewGraph[runnerHandoffState, flowy.NoEffect](func(s, u runnerHandoffState) runnerHandoffState { return u })
	b.AddNode("work", func(_ context.Context, s runnerHandoffState) (runnerHandoffState, flowy.Directive, error) {
		return s, flowy.Handoff("bg"), nil
	})
	b.AllowNoOutgoingRoute("work")
	b.SetEntryPoint("work")
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	return g
}

func TestRunnerHandoffPostgresTransactionalSuccess(t *testing.T) {
	pool, cp := pgRunnerPool(t)
	outbox := &stubHandoffOutbox{}
	g := pgHandoffGraph(t)
	threadID := testThreadID(t)

	res, err := g.NewRunner(cp).Start(context.Background(), threadID, runnerHandoffState{},
		flowy.WithHandoffOutbox[runnerHandoffState, flowy.NoEffect](outbox),
	)
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	if res.Status != flowy.RunStatusHandoff {
		t.Fatalf("expected handoff, got %s", res.Status)
	}
	snap, rev, loadErr := cp.Load(context.Background(), threadID)
	if loadErr != nil {
		t.Fatalf("load: %v", loadErr)
	}
	if snap.RunMeta.HandoffStatus != flowy.HandoffStatusEnqueued {
		t.Fatalf("expected enqueued, got %q", snap.RunMeta.HandoffStatus)
	}
	if len(outbox.txCalls) != 1 {
		t.Fatalf("expected one transactional enqueue, got %d", len(outbox.txCalls))
	}
	if len(outbox.calls) != 0 {
		t.Fatalf("expected no non-transactional enqueue, got %d", len(outbox.calls))
	}
	if outbox.txCalls[0].SnapshotRevision != rev {
		t.Fatalf("outbox intent rev %d != snapshot rev %d", outbox.txCalls[0].SnapshotRevision, rev)
	}
	var outboxRows int
	if err := pool.QueryRow(context.Background(),
		`SELECT COUNT(*) FROM flowy_handoff_outbox WHERE thread_id = $1 AND snapshot_revision = $2`,
		threadID, int64(rev),
	).Scan(&outboxRows); err != nil {
		t.Fatalf("count outbox: %v", err)
	}
	if outboxRows != 1 {
		t.Fatalf("expected one transactional outbox row, got %d", outboxRows)
	}
	if res.RunMeta.HandoffStatus != snap.RunMeta.HandoffStatus {
		t.Fatalf("result handoff status %q != snapshot %q", res.RunMeta.HandoffStatus, snap.RunMeta.HandoffStatus)
	}
	if !res.RunMeta.HandoffPendingAt.IsZero() {
		t.Fatalf("expected HandoffPendingAt cleared after TX enqueued, got %v", res.RunMeta.HandoffPendingAt)
	}
}

func TestRunnerHandoffPostgresTransactionalEnqueueFail(t *testing.T) {
	_, cp := pgRunnerPool(t)
	outbox := &stubHandoffOutbox{err: errors.New("broker down")}
	g := pgHandoffGraph(t)
	threadID := testThreadID(t)

	res, err := g.NewRunner(cp).Start(context.Background(), threadID, runnerHandoffState{},
		flowy.WithHandoffOutbox[runnerHandoffState, flowy.NoEffect](outbox),
	)
	if err == nil {
		t.Fatal("expected transactional handoff failure")
	}
	if res == nil || res.Reason != flowy.ReasonHandoffSaveFailed {
		t.Fatalf("expected reason %q, got %+v", flowy.ReasonHandoffSaveFailed, res)
	}
	if res.RunMeta.HandoffStatus != flowy.HandoffStatusNone {
		t.Fatalf("expected none handoff status after TX rollback, got %q", res.RunMeta.HandoffStatus)
	}
	if len(outbox.txCalls) != 1 {
		t.Fatalf("expected one transactional enqueue attempt, got %d", len(outbox.txCalls))
	}
	if len(outbox.calls) != 0 {
		t.Fatalf("expected no non-transactional enqueue, got %d", len(outbox.calls))
	}
	_, _, loadErr := cp.Load(context.Background(), threadID)
	if !errors.Is(loadErr, flowy.ErrThreadNotFound) {
		t.Fatalf("expected missing checkpoint after rollback, got %v", loadErr)
	}
}

func TestRecoverStaleHandoffPostgresOrphaned(t *testing.T) {
	_, cp := pgRunnerPool(t)
	outbox := &stubHandoffOutbox{}
	now := time.Now().UTC().Add(-time.Hour)
	threadID := testThreadID(t)
	if _, err := cp.Save(context.Background(), 0, flowy.Snapshot[runnerHandoffState, flowy.NoEffect]{
		ThreadID:         threadID,
		ExecutionPointer: "work",
		State:            runnerHandoffState{},
		RunMeta: flowy.RunMetadata{
			HandoffStatus:    flowy.HandoffStatusOrphaned,
			HandoffPendingAt: now,
		},
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	g := pgHandoffGraph(t)
	runner := g.NewRunnerWithOptions(cp, []flowy.RunnerOption[runnerHandoffState, flowy.NoEffect]{
		flowy.WithRunnerHandoffOutbox[runnerHandoffState, flowy.NoEffect](outbox),
	})
	if _, recoverErr := runner.RecoverStaleHandoff(context.Background(), threadID); recoverErr != nil {
		t.Fatalf("recover: %v", recoverErr)
	}
	if len(outbox.calls) != 1 {
		t.Fatalf("expected one enqueue, got %d", len(outbox.calls))
	}
	snap, _, err := cp.Load(context.Background(), threadID)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if snap.RunMeta.HandoffStatus != flowy.HandoffStatusEnqueued {
		t.Fatalf("expected enqueued, got %q", snap.RunMeta.HandoffStatus)
	}
}

func TestRecoverStaleHandoffPostgresStalePending(t *testing.T) {
	_, cp := pgRunnerPool(t)
	outbox := &stubHandoffOutbox{}
	staleAt := time.Now().UTC().Add(-10 * time.Minute)
	threadID := testThreadID(t)
	if _, err := cp.Save(context.Background(), 0, flowy.Snapshot[runnerHandoffState, flowy.NoEffect]{
		ThreadID:         threadID,
		ExecutionPointer: "work",
		State:            runnerHandoffState{},
		RunMeta: flowy.RunMetadata{
			HandoffStatus:    flowy.HandoffStatusPending,
			HandoffPendingAt: staleAt,
		},
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	g := pgHandoffGraph(t)
	runner := g.NewRunnerWithOptions(cp, []flowy.RunnerOption[runnerHandoffState, flowy.NoEffect]{
		flowy.WithRunnerHandoffOutbox[runnerHandoffState, flowy.NoEffect](outbox),
		flowy.WithHandoffStaleAfter[runnerHandoffState, flowy.NoEffect](time.Minute),
	})
	if _, recoverErr := runner.RecoverStaleHandoff(context.Background(), threadID); recoverErr != nil {
		t.Fatalf("recover: %v", recoverErr)
	}
	if len(outbox.calls) != 1 {
		t.Fatalf("expected one enqueue after stale pending, got %d", len(outbox.calls))
	}
	snap, rev, err := cp.Load(context.Background(), threadID)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if snap.RunMeta.HandoffStatus != flowy.HandoffStatusEnqueued {
		t.Fatalf("expected enqueued, got %q", snap.RunMeta.HandoffStatus)
	}
	if outbox.calls[0].SnapshotRevision != rev ||
		outbox.calls[0].CommittedSnapshotRevision != rev ||
		outbox.calls[0].ResumeToken.SnapshotRevision != rev {
		t.Fatalf("enqueue intent revision should match committed snapshot revision %d, got %+v",
			rev, outbox.calls[0])
	}
}

func TestRecoverStaleHandoffPostgresFreshPendingRejected(t *testing.T) {
	_, cp := pgRunnerPool(t)
	threadID := testThreadID(t)
	if _, err := cp.Save(context.Background(), 0, flowy.Snapshot[runnerHandoffState, flowy.NoEffect]{
		ThreadID:         threadID,
		ExecutionPointer: "work",
		State:            runnerHandoffState{},
		RunMeta: flowy.RunMetadata{
			HandoffStatus:    flowy.HandoffStatusPending,
			HandoffPendingAt: time.Now().UTC(),
		},
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	g := pgHandoffGraph(t)
	runner := g.NewRunnerWithOptions(cp, []flowy.RunnerOption[runnerHandoffState, flowy.NoEffect]{
		flowy.WithRunnerHandoffOutbox[runnerHandoffState, flowy.NoEffect](&stubHandoffOutbox{}),
		flowy.WithHandoffStaleAfter[runnerHandoffState, flowy.NoEffect](5 * time.Minute),
	})
	_, err := runner.RecoverStaleHandoff(context.Background(), threadID)
	if !errors.Is(err, flowy.ErrHandoffPending) {
		t.Fatalf("expected ErrHandoffPending, got %v", err)
	}
}

func TestRecoverStaleHandoffPostgresWithoutOutbox(t *testing.T) {
	_, cp := pgRunnerPool(t)
	threadID := testThreadID(t)
	if _, err := cp.Save(context.Background(), 0, flowy.Snapshot[runnerHandoffState, flowy.NoEffect]{
		ThreadID:         threadID,
		ExecutionPointer: "work",
		State:            runnerHandoffState{},
		RunMeta: flowy.RunMetadata{
			HandoffStatus: flowy.HandoffStatusOrphaned,
		},
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	g := pgHandoffGraph(t)
	runner := g.NewRunner(cp)
	_, err := runner.RecoverStaleHandoff(context.Background(), threadID)
	if !errors.Is(err, flowy.ErrHandoffOutboxRequired) {
		t.Fatalf("expected ErrHandoffOutboxRequired, got %v", err)
	}
}

func TestRecoverStaleHandoffPostgresAlreadyEnqueuedRejected(t *testing.T) {
	_, cp := pgRunnerPool(t)
	threadID := testThreadID(t)
	if _, err := cp.Save(context.Background(), 0, flowy.Snapshot[runnerHandoffState, flowy.NoEffect]{
		ThreadID:         threadID,
		ExecutionPointer: "work",
		State:            runnerHandoffState{},
		RunMeta: flowy.RunMetadata{
			HandoffStatus: flowy.HandoffStatusEnqueued,
		},
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	g := pgHandoffGraph(t)
	runner := g.NewRunnerWithOptions(cp, []flowy.RunnerOption[runnerHandoffState, flowy.NoEffect]{
		flowy.WithRunnerHandoffOutbox[runnerHandoffState, flowy.NoEffect](&stubHandoffOutbox{}),
	})
	_, err := runner.RecoverStaleHandoff(context.Background(), threadID)
	if !errors.Is(err, flowy.ErrHandoffAlreadyEnqueued) {
		t.Fatalf("expected ErrHandoffAlreadyEnqueued, got %v", err)
	}
}

func TestRecoverStaleHandoffPostgresNoneStatusRejected(t *testing.T) {
	_, cp := pgRunnerPool(t)
	threadID := testThreadID(t)
	if _, err := cp.Save(context.Background(), 0, flowy.Snapshot[runnerHandoffState, flowy.NoEffect]{
		ThreadID:         threadID,
		ExecutionPointer: "work",
		State:            runnerHandoffState{},
		RunMeta:          flowy.RunMetadata{HandoffStatus: flowy.HandoffStatusNone},
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	g := pgHandoffGraph(t)
	runner := g.NewRunnerWithOptions(cp, []flowy.RunnerOption[runnerHandoffState, flowy.NoEffect]{
		flowy.WithRunnerHandoffOutbox[runnerHandoffState, flowy.NoEffect](&stubHandoffOutbox{}),
	})
	_, err := runner.RecoverStaleHandoff(context.Background(), threadID)
	if !errors.Is(err, flowy.ErrHandoffNotRecoverable) {
		t.Fatalf("expected ErrHandoffNotRecoverable, got %v", err)
	}
}
