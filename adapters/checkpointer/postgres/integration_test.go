//go:build integration

package postgres

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	pglease "github.com/skosovsky/flowy/adapters/lease/postgres"

	"github.com/skosovsky/flowy"
	"github.com/skosovsky/flowy/checkpoint"
)

type intState struct {
	Value int `json:"value"`
}

func testSnapshot(revision int, value int) flowy.Snapshot[intState, string] {
	return flowy.Snapshot[intState, string]{
		ThreadID:         "t1",
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

	if err := cp.Save(ctx, testSnapshot(1, 1)); err != nil {
		t.Fatalf("save: %v", err)
	}
	if err := leaseMgr.Acquire(ctx, "t1", "worker", time.Minute); err != nil {
		t.Fatalf("acquire: %v", err)
	}
	if err := cp.DeleteIfIdle(ctx, "t1"); !errors.Is(err, flowy.ErrThreadLeaseBusy) {
		t.Fatalf("expected ErrThreadLeaseBusy, got %v", err)
	}
	if err := leaseMgr.Release(ctx, "t1", "worker"); err != nil {
		t.Fatalf("release: %v", err)
	}
	if err := cp.DeleteIfIdle(ctx, "t1"); err != nil {
		t.Fatalf("delete after release: %v", err)
	}
}
