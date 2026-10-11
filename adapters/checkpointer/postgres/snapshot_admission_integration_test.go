//go:build integration

package postgres

import (
	"context"
	"errors"
	"testing"

	"github.com/skosovsky/flowy"
	"github.com/skosovsky/flowy/checkpoint"
)

func TestLiveSnapshotAdmissionKeepsHeadHistoryAndOutbox(t *testing.T) {
	ctx, pool := racePool(t)
	if _, err := pool.Exec(ctx, OutboxSchemaSQL()); err != nil {
		t.Fatal(err)
	}
	cp := mustCheckpointer[intState, string](t, pool, checkpoint.JSONSerializer[intState]{})
	for _, malformed := range []string{"empty id", "empty pointer", "invalid id", "invalid pointer"} {
		t.Run(malformed, func(t *testing.T) {
			// Arrange: one healthy head/history before the attempted transactional handoff.
			id := testThreadID(t)
			snapshot := testSnapshot(id, 0, 42)
			if _, err := cp.Save(ctx, 0, snapshot); err != nil {
				t.Fatal(err)
			}
			switch malformed {
			case "empty id":
				snapshot.ThreadID = ""
			case "empty pointer":
				snapshot.ExecutionPointer = ""
			case "invalid id":
				snapshot.ThreadID += "\xff"
			case "invalid pointer":
				snapshot.ExecutionPointer += "\xff"
			}
			enqueues := 0
			// Act.
			_, saveErr := cp.SaveWithOutbox(
				ctx,
				1,
				snapshot,
				func(context.Context, flowy.TransactionHandle, uint64) error { enqueues++; return nil },
			)
			// Assert: a rejecting metadata boundary cannot poison the healthy stored head.
			healthy, revision, loadErr := cp.Load(ctx, id)
			history, historyErr := cp.GetHistory(ctx, id, 0)
			var outboxCount int
			countErr := pool.QueryRow(ctx, "SELECT count(*) FROM flowy_handoff_outbox WHERE thread_id=$1", id).
				Scan(&outboxCount)
			if !errors.Is(saveErr, flowy.ErrSnapshotEnvelopeInvalid) || enqueues != 0 || loadErr != nil ||
				historyErr != nil ||
				countErr != nil ||
				revision != 1 ||
				healthy.State.Value != 42 ||
				len(history) != 1 ||
				outboxCount != 0 {
				t.Fatalf(
					"save=%v enqueue=%d head=%+v revision=%d history=%d outbox=%d errors=%v/%v/%v",
					saveErr,
					enqueues,
					healthy,
					revision,
					len(history),
					outboxCount,
					loadErr,
					historyErr,
					countErr,
				)
			}
		})
	}
}
