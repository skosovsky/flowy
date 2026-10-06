//go:build integration

package postgres

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/skosovsky/flowy"
)

func pgWaitCancellation(delivery flowy.WaitDelivery) flowy.WaitCancellation {
	return flowy.WaitCancellation{Generation: delivery.Generation, ID: "cancel", Reason: "operator requested",
		Evidence: "host-evidence"}
}

func TestWaitCancellationPersistentLateDeliveryAndResumeCannotRevive(t *testing.T) {
	// Arrange: cancellation is committed on one pool, late delivery on its replacement.
	ctx, pool := racePool(t)
	if _, err := pool.Exec(ctx, ExecutionSchemaSQL()); err != nil {
		t.Fatal(err)
	}
	store, err := NewWaitExecutionStore(pool, postgresWaitProfile())
	if err != nil {
		t.Fatal(err)
	}
	var calls, matches, applies atomic.Int32
	id := testThreadID(t)
	spec := postgresWaitSpec(time.Date(2026, 10, 5, 0, 0, 0, 0, time.UTC))
	clock := waitAcceptanceClock{now: spec.Deadline}
	runner := postgresWaitRunner(t, store, spec, &calls, clock)
	armed, err := runner.Start(ctx, id, intState{})
	if err != nil {
		t.Fatal(err)
	}
	delivery := pgWaitDelivery(ctx, t, store, id)
	request := pgWaitCancellation(delivery)
	canceled, err := runner.CancelWait(ctx, armed.ResumeToken, request)
	if err != nil {
		t.Fatal(err)
	}
	pool.Close()
	restartCtx, restartPool := racePool(t)
	restarted, err := NewWaitExecutionStore(restartPool, postgresWaitProfile())
	if err != nil {
		t.Fatal(err)
	}
	recovered := postgresWaitRunner(t, restarted, spec, &calls, clock)
	contract := pgWaitContract(spec, &matches, &applies)
	contract.Match, contract.Apply = nil, nil
	// Act.
	replayed, replayErr := recovered.CancelWait(restartCtx, armed.ResumeToken, request)
	late, lateErr := recovered.DeliverWait(restartCtx, id, delivery, contract)
	source, loadErr := restarted.LoadExecution(restartCtx, id)
	// Assert: replay writes no replacement cancellation, and late rejection clears nothing.
	if replayErr != nil || replayed != canceled || lateErr != nil ||
		late.Decision.Status != flowy.WaitRejectedCanceled ||
		loadErr != nil ||
		source.Terminal == nil ||
		source.Terminal.Reason != "durable_wait_canceled" ||
		source.Progress.ExecutionPointer != "waiting" ||
		source.Activation != 1 ||
		calls.Load() != 1 ||
		matches.Load() != 0 ||
		applies.Load() != 0 ||
		source.Revision != 4 {
		t.Fatalf("persistent cancellation revived: replay=%+v/%v late=%+v/%v source=%+v load=%v",
			replayed, replayErr, late, lateErr, source, loadErr)
	}
	restartPool.Close()
	finalCtx, finalPool := racePool(t)
	finalStore, err := NewWaitExecutionStore(finalPool, postgresWaitProfile())
	if err != nil {
		t.Fatal(err)
	}
	result, resumeErr := postgresWaitRunner(t, finalStore, spec, &calls, clock).Resume(finalCtx, late.ResumeToken)
	if !errors.Is(resumeErr, flowy.ErrExecutionFailed) || result == nil || result.Status != flowy.RunStatusFailed ||
		result.Reason != "durable_wait_canceled" || calls.Load() != 1 {
		t.Fatalf("canceled persistent Resume executed node: %+v err=%v calls=%d", result, resumeErr, calls.Load())
	}
}

type waitCancelFaultStore struct {
	*ExecutionStore

	fail atomic.Bool
}

func (s *waitCancelFaultStore) CommitExecution(ctx context.Context, revision uint64,
	lease flowy.ExecutionLease, envelope flowy.ExecutionEnvelope,
) (flowy.ExecutionEnvelope, error) {
	if envelope.Terminal != nil && envelope.Terminal.Reason == "durable_wait_canceled" && s.fail.Swap(false) {
		return flowy.ExecutionEnvelope{}, errors.New("injected cancellation commit failure")
	}
	return s.ExecutionStore.CommitExecution(ctx, revision, lease, envelope)
}

func TestWaitCancellationPersistentCommitFaultRetainsArmedSource(t *testing.T) {
	// Arrange.
	ctx, pool := racePool(t)
	if _, err := pool.Exec(ctx, ExecutionSchemaSQL()); err != nil {
		t.Fatal(err)
	}
	store, err := NewWaitExecutionStore(pool, postgresWaitProfile())
	if err != nil {
		t.Fatal(err)
	}
	fault := &waitCancelFaultStore{ExecutionStore: store}
	fault.fail.Store(true)
	var calls atomic.Int32
	spec := postgresWaitSpec(time.Date(2026, 10, 5, 0, 0, 0, 0, time.UTC))
	id := testThreadID(t)
	runner := postgresWaitRunner(t, fault, spec, &calls)
	armed, err := runner.Start(ctx, id, intState{})
	if err != nil {
		t.Fatal(err)
	}
	request := pgWaitCancellation(pgWaitDelivery(ctx, t, store, id))
	before, err := store.LoadExecution(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	// Act.
	failed, failedErr := runner.CancelWait(ctx, armed.ResumeToken, request)
	after, loadErr := store.LoadExecution(ctx, id)
	// Assert: no successful token, replacement history or partial terminal on fault.
	if failedErr == nil || failed.SnapshotRevision != 0 || loadErr != nil || after.Digest != before.Digest ||
		after.Revision != before.Revision || after.Terminal != nil {
		t.Fatalf("cancel fault leaked: token=%+v err=%v after=%+v load=%v", failed, failedErr, after, loadErr)
	}
	pool.Close()
	restartCtx, restartPool := racePool(t)
	restarted, err := NewWaitExecutionStore(restartPool, postgresWaitProfile())
	if err != nil {
		t.Fatal(err)
	}
	canceled, cancelErr := postgresWaitRunner(
		t,
		restarted,
		spec,
		&calls,
	).CancelWait(restartCtx, armed.ResumeToken, request)
	if cancelErr != nil || canceled.SnapshotRevision != 3 || calls.Load() != 1 {
		t.Fatalf("persistent cancel retry failed: %+v err=%v calls=%d", canceled, cancelErr, calls.Load())
	}
}
