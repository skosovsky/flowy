//go:build integration

package postgres

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/skosovsky/flowy"
)

type retentionFaultDB struct {
	DB
	lostAck bool
}

func (db retentionFaultDB) BeginTx(ctx context.Context, options pgx.TxOptions) (pgx.Tx, error) {
	tx, err := db.DB.BeginTx(ctx, options)
	if err != nil {
		return nil, err
	}
	return retentionFaultTx{Tx: tx, lostAck: db.lostAck}, nil
}

type retentionFaultTx struct {
	pgx.Tx
	lostAck bool
}

func (tx retentionFaultTx) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	tag, err := tx.Tx.Exec(ctx, sql, args...)
	if err == nil && !tx.lostAck && strings.Contains(sql, "SET payload_deleted=true") {
		// The history DELETE and tombstone UPDATE have executed, but no commit.
		// Closing this actual database connection forces rollback of both changes.
		if closeErr := tx.Conn().Close(context.WithoutCancel(ctx)); closeErr != nil {
			return tag, closeErr
		}
		return tag, errors.New("connection lost before retention commit")
	}
	return tag, err
}

func (tx retentionFaultTx) Commit(ctx context.Context) error {
	err := tx.Tx.Commit(ctx)
	if err == nil && tx.lostAck {
		return errors.New("lost retention acknowledgement")
	}
	return err
}

func TestRetentionPersistentInterruptedTransactionAndLostAck(t *testing.T) {
	for _, lostAck := range []bool{false, true} {
		t.Run(
			map[bool]string{false: "connection interruption after deletion", true: "lost commit acknowledgement"}[lostAck],
			func(t *testing.T) {
				// Arrange: one completed execution with committed external outcome.
				ctx, pool := racePool(t)
				if _, err := pool.Exec(ctx, ExecutionSchemaSQL()); err != nil {
					t.Fatal(err)
				}
				id := testThreadID(t)
				store := NewExecutionStore(pool)
				var calls atomic.Int32
				activity := flowy.ActivityRequest{
					Key:            "write",
					Implementation: "host",
					Input:          []byte("input"),
					Dispatch: func(context.Context, flowy.ActivityInvocation) ([]byte, error) {
						calls.Add(1)
						return []byte("receipt"), nil
					},
				}
				result, err := persistentReferenceRunner(
					t,
					store,
					referenceDescriptor("cleanup"),
					"node",
					activity,
					nil,
				).Start(ctx, id, intState{Value: 1})
				if err != nil {
					t.Fatal(err)
				}
				before, err := store.LoadExecution(ctx, id)
				if err != nil {
					t.Fatal(err)
				}
				request := flowy.ExecutionRetentionRequest{
					ExecutionID: id,
					Revision:    result.ResumeToken.SnapshotRevision,
					Policy:      flowy.ExecutionRetentionPolicy{Label: "archive", DeletePayload: true},
				}
				fault := NewExecutionStore(retentionFaultDB{DB: pool, lostAck: lostAck})
				// Act: lose connection before commit or lose acknowledgement after atomic commit.
				_, failed := fault.RetainExecution(ctx, request)
				if failed == nil {
					t.Fatal("fault not observed")
				}
				pool.Close()
				recoveryCtx, recoveryPool := racePool(t)
				recovery := NewExecutionStore(recoveryPool)
				recovered, loadErr := recovery.LoadExecution(recoveryCtx, id)
				// Assert: before-commit failure leaves every original payload unchanged;
				// after-commit failure leaves a complete tombstone, never a partial history.
				if lostAck {
					if !errors.Is(loadErr, flowy.ErrExecutionCheckpointUnavailable) {
						t.Fatalf("committed deletion absent: %v", loadErr)
					}
				} else if loadErr != nil || recovered.Digest != before.Digest || recovered.Revision != before.Revision {
					t.Fatalf("partial cleanup=%+v/%v", recovered, loadErr)
				}
				receipt, err := recovery.RetainExecution(recoveryCtx, request)
				repeat, repeatErr := recovery.RetainExecution(recoveryCtx, request)
				_, unavailable := recovery.LoadCheckpoint(recoveryCtx, id, before.Revision)
				expectedDeleted := receipt.DeletedRevisions > 0
				if lostAck {
					expectedDeleted = receipt.DeletedRevisions == 0
				}
				if err != nil || repeatErr != nil || !expectedDeleted || repeat.DeletedRevisions != 0 ||
					!errors.Is(unavailable, flowy.ErrExecutionCheckpointUnavailable) ||
					calls.Load() != 1 {
					t.Fatalf(
						"retry=%+v/%v repeat=%+v/%v unavailable=%v calls=%d",
						receipt,
						err,
						repeat,
						repeatErr,
						unavailable,
						calls.Load(),
					)
				}
				var revision uint64
				var fence uint64
				if err = recoveryPool.QueryRow(recoveryCtx, `SELECT revision,fence FROM flowy_executions WHERE execution_id=$1`, id).
					Scan(&revision, &fence); err != nil ||
					revision != before.Revision ||
					fence == 0 {
					t.Fatalf("identity lost: revision=%d fence=%d err=%v", revision, fence, err)
				}
			},
		)
	}
}
