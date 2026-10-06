//go:build integration

package postgres

import (
	"context"
	"errors"
	"math"
	"testing"
	"time"

	"github.com/skosovsky/flowy"
	"github.com/skosovsky/flowy/checkpoint"
)

func TestLiveHistoricalAddressAndFenceBoundaries(t *testing.T) {
	// Arrange: a retained published head whose payloads are gone.
	ctx, pool := racePool(t)
	if _, err := pool.Exec(ctx, ExecutionSchemaSQL()); err != nil {
		t.Fatal(err)
	}
	id := testThreadID(t)
	if _, err := pool.Exec(ctx, `INSERT INTO flowy_executions(execution_id,revision,fence,payload_deleted)
		VALUES($1,2,$2,true)`, id, int64(math.MaxInt64)); err != nil {
		t.Fatal(err)
	}
	store := mustExecutionStore(t, pool)
	for _, test := range []struct {
		id       string
		revision uint64
		want     error
	}{
		{id: id, revision: 0, want: flowy.ErrInvalidSnapshot},
		{id: id + "absent", revision: 1, want: flowy.ErrThreadNotFound},
		{id: id, revision: 1, want: flowy.ErrExecutionCheckpointUnavailable},
		{id: id, revision: 2, want: flowy.ErrExecutionCheckpointUnavailable},
		{id: id, revision: 3, want: flowy.ErrThreadNotFound},
		{id: id, revision: uint64(math.MaxInt64) + 1, want: flowy.ErrExecutionCapability},
	} {
		// Act.
		_, err := store.LoadCheckpoint(ctx, test.id, test.revision)
		// Assert.
		if !errors.Is(err, test.want) {
			t.Fatalf("id=%s revision=%d error=%v want=%v", test.id, test.revision, err, test.want)
		}
	}
	_, latestErr := store.LoadExecution(ctx, id)
	_, acquireErr := store.AcquireExecution(ctx, id, "worker", time.Minute)
	var fence, revision int64
	var owner *string
	err := pool.QueryRow(ctx, "SELECT fence,revision,lease_owner FROM flowy_executions WHERE execution_id=$1", id).
		Scan(&fence, &revision, &owner)
	if !errors.Is(latestErr, flowy.ErrExecutionCheckpointUnavailable) ||
		!errors.Is(acquireErr, flowy.ErrExecutionCapability) || err != nil ||
		fence != math.MaxInt64 || revision != 2 || owner != nil {
		t.Fatalf("latest=%v acquire=%v query=%v fence=%d revision=%d owner=%v",
			latestErr, acquireErr, err, fence, revision, owner)
	}
	// Standalone leases preserve the same retained signed counter on overflow.
	if _, seedErr := pool.Exec(ctx, "INSERT INTO flowy_lease_fences(thread_id,incarnation) VALUES($1,$2)",
		id, int64(math.MaxInt64)); seedErr != nil {
		t.Fatal(seedErr)
	}
	manager := mustPostgresLeaseManager(t, pool)
	_, acquireErr = manager.Acquire(ctx, id, "worker", time.Minute)
	var leases int
	err = pool.QueryRow(ctx, "SELECT incarnation FROM flowy_lease_fences WHERE thread_id=$1", id).Scan(&fence)
	countErr := pool.QueryRow(ctx, "SELECT count(*) FROM flowy_leases WHERE thread_id=$1", id).Scan(&leases)
	if !errors.Is(acquireErr, flowy.ErrExecutionCapability) || err != nil || countErr != nil ||
		fence != math.MaxInt64 || leases != 0 {
		t.Fatalf("lease acquire=%v query=%v count=%v fence=%d leases=%d", acquireErr, err, countErr, fence, leases)
	}
}

type jsonDomainCodec struct{ payload []byte }

func (c jsonDomainCodec) Marshal(intState) ([]byte, error) { return c.payload, nil }
func (jsonDomainCodec) Unmarshal(data []byte) (intState, error) {
	return (checkpoint.JSONSerializer[intState]{}).Unmarshal(data)
}

func TestLiveJSONBDomainRejectionPreservesHeadAndOutbox(t *testing.T) {
	ctx, pool := racePool(t)
	if _, err := pool.Exec(ctx, OutboxSchemaSQL()); err != nil {
		t.Fatal(err)
	}
	for _, payload := range []string{`{"Value":"\u0000"}`, `{"Value":1e1000000}`, `not JSON`} {
		t.Run(payload, func(t *testing.T) {
			// Arrange: the codec's domain can be stricter at the backend boundary.
			id := testThreadID(t)
			healthy := mustCheckpointer[intState, string](t, pool, checkpoint.JSONSerializer[intState]{})
			snapshot := testSnapshot(id, 0, 42)
			if _, err := healthy.Save(ctx, 0, snapshot); err != nil {
				t.Fatal(err)
			}
			cp := mustCheckpointer[intState, string](t, pool, jsonDomainCodec{payload: []byte(payload)})
			enqueues := 0
			// Act.
			_, saveErr := cp.SaveWithOutbox(ctx, 1, snapshot,
				func(context.Context, flowy.TransactionHandle, uint64) error { enqueues++; return nil })
			// Assert.
			stored, revision, loadErr := healthy.Load(ctx, id)
			history, historyErr := healthy.GetHistory(ctx, id, 0)
			if saveErr == nil || enqueues != 0 || loadErr != nil || historyErr != nil ||
				revision != 1 || stored.State.Value != 42 || len(history) != 1 {
				t.Fatalf("save=%v enqueues=%d load=%v history=%v revision=%d stored=%+v historylen=%d",
					saveErr, enqueues, loadErr, historyErr, revision, stored, len(history))
			}
		})
	}
}
