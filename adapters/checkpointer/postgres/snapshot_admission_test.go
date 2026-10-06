package postgres

import (
	"context"
	"errors"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/skosovsky/flowy"
	"github.com/skosovsky/flowy/checkpoint"
)

type admissionDB struct {
	DB

	begins int
}

func (db *admissionDB) BeginTx(context.Context, pgx.TxOptions) (pgx.Tx, error) {
	db.begins++
	return nil, errors.New("unexpected transaction")
}
func TestSnapshotAdmissionRejectsBeforeTransactionAndOutbox(t *testing.T) {
	t.Parallel()
	for name, snapshot := range map[string]flowy.Snapshot[int, flowy.NoEffect]{
		"empty thread":    {ThreadID: "", ExecutionPointer: "node"},
		"empty pointer":   {ThreadID: "run", ExecutionPointer: ""},
		"invalid thread":  {ThreadID: "run\xff", ExecutionPointer: "node"},
		"invalid pointer": {ThreadID: "run", ExecutionPointer: "node\xff"},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			// Arrange: malformed metadata must not reach any SQL or enqueue callback.
			db := &admissionDB{DB: nil, begins: 0}
			cp := mustCheckpointer[int, flowy.NoEffect](t, db, checkpoint.JSONSerializer[int]{})
			enqueues := 0
			// Act.
			_, err := cp.SaveWithOutbox(
				context.Background(),
				1,
				snapshot,
				func(context.Context, flowy.TransactionHandle, uint64) error { enqueues++; return nil },
			)
			// Assert.
			if !errors.Is(err, flowy.ErrSnapshotEnvelopeInvalid) || db.begins != 0 || enqueues != 0 {
				t.Fatalf("err=%v begins=%d enqueues=%d", err, db.begins, enqueues)
			}
		})
	}
}
