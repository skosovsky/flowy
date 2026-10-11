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

type rolloverPartialFaultDB struct{ DB }

func (db rolloverPartialFaultDB) BeginTx(ctx context.Context, options pgx.TxOptions) (pgx.Tx, error) {
	tx, err := db.DB.BeginTx(ctx, options)
	if err != nil {
		return nil, err
	}
	return rolloverPartialFaultTx{Tx: tx}, nil
}

type rolloverPartialFaultTx struct{ pgx.Tx }

func (tx rolloverPartialFaultTx) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	tag, err := tx.Tx.Exec(ctx, sql, args...)
	if err == nil && strings.Contains(sql, "rollover_outgoing=") {
		// Target head and both history rows have been inserted, source updated.
		// Interrupt the actual connection before COMMIT: none may become authority.
		if closeErr := tx.Conn().Close(context.WithoutCancel(ctx)); closeErr != nil {
			return tag, closeErr
		}
		return tag, errors.New("connection lost inside rollover publication")
	}
	return tag, err
}

func TestRolloverPersistentInterruptedPairPublication(t *testing.T) {
	// Arrange: a completed bounded source with a known external effect.
	ctx, pool := racePool(t)
	if _, err := pool.Exec(ctx, ExecutionSchemaSQL()); err != nil {
		t.Fatal(err)
	}
	id := testThreadID(t)
	var calls, projections atomic.Int32
	activity := flowy.ActivityRequest{
		Key:            "write",
		Implementation: "host",
		Input:          []byte("input"),
		Dispatch: func(context.Context, flowy.ActivityInvocation) ([]byte, error) {
			calls.Add(1)
			return []byte("receipt"), nil
		},
	}
	descriptor := referenceDescriptor("pair-fault")
	base := mustExecutionStore(t, pool)
	source, err := persistentReferenceRunner(
		t,
		base,
		descriptor,
		"node",
		activity,
		nil,
	).Start(ctx, id, intState{Value: 1})
	if err != nil {
		t.Fatal(err)
	}
	before, err := base.LoadExecution(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	request := lifecyclePGRequest(descriptor, id+"-next", &projections)
	faulty := persistentReferenceRunner(
		t,
		mustExecutionStore(t, rolloverPartialFaultDB{DB: pool}),
		descriptor,
		"node",
		activity,
		nil,
	)
	// Act: actual connection failure after every DML phase, before atomic COMMIT.
	_, failed := faulty.Rollover(ctx, source.ResumeToken, request)
	if failed == nil {
		t.Fatal("transaction interruption missing")
	}
	pool.Close()
	recoveryCtx, recoveryPool := racePool(t)
	recovery := mustExecutionStore(t, recoveryPool)
	after, loadErr := recovery.LoadExecution(recoveryCtx, id)
	receipt, receiptErr := recovery.LoadRollover(recoveryCtx, id)
	_, targetErr := recovery.LoadExecution(recoveryCtx, request.TargetID)
	// Assert: rollback retains original authority and removes every unpublished target artifact.
	if loadErr != nil || after.Digest != before.Digest || after.Revision != before.Revision || receiptErr != nil ||
		receipt != nil ||
		!errors.Is(targetErr, flowy.ErrThreadNotFound) ||
		projections.Load() != 1 ||
		calls.Load() != 1 {
		t.Fatalf(
			"source=%v receipt=%+v/%v target=%v calls/projections=%d/%d",
			loadErr,
			receipt,
			receiptErr,
			targetErr,
			calls.Load(),
			projections.Load(),
		)
	}
	var targetHeads, targetRows int
	if err = recoveryPool.QueryRow(recoveryCtx, `SELECT (SELECT count(*) FROM flowy_executions WHERE execution_id=$1),(SELECT count(*) FROM flowy_execution_history WHERE execution_id=$1)`, request.TargetID).
		Scan(&targetHeads, &targetRows); err != nil ||
		targetHeads != 0 ||
		targetRows != 0 {
		t.Fatalf("partial target=%d/%d err=%v", targetHeads, targetRows, err)
	}
	runner := persistentReferenceRunner(t, recovery, descriptor, "node", activity, nil)
	target, err := runner.Rollover(recoveryCtx, source.ResumeToken, request)
	if err != nil {
		t.Fatal(err)
	}
	_, replayErr := runner.Rollover(recoveryCtx, source.ResumeToken, request)
	result, resumeErr := runner.Resume(recoveryCtx, target)
	if replayErr != nil || resumeErr != nil || result.Status != flowy.RunStatusCompleted || calls.Load() != 2 ||
		projections.Load() != 2 {
		t.Fatalf("replay=%v resume=%v calls/projections=%d/%d", replayErr, resumeErr, calls.Load(), projections.Load())
	}
}
