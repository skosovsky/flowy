//go:build integration

package postgres

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/skosovsky/flowy"
)

func TestWaitStaleMatcherCannotOverwriteTimerWinnerOnLiveOldConnection(t *testing.T) {
	// Arrange: old matcher ignores cancellation while a separate pool takes its expired lease.
	ctx, oldPool := racePool(t)
	if _, err := oldPool.Exec(ctx, ExecutionSchemaSQL()); err != nil {
		t.Fatal(err)
	}
	oldStore, err := NewWaitExecutionStore(oldPool, postgresWaitProfile())
	if err != nil {
		t.Fatal(err)
	}
	newCtx, newPool := racePool(t)
	newStore, err := NewWaitExecutionStore(newPool, postgresWaitProfile())
	if err != nil {
		t.Fatal(err)
	}
	id := testThreadID(t)
	spec := postgresWaitSpec(time.Date(2026, 10, 5, 0, 0, 0, 0, time.UTC))
	clock := waitAcceptanceClock{now: spec.Deadline}
	var nodes, matches, oldApplies, newMatches, newApplies atomic.Int32
	oldRunner := postgresWaitRunner(t, oldStore, spec, &nodes, clock)
	if _, err = oldRunner.Start(ctx, id, intState{}); err != nil {
		t.Fatal(err)
	}
	event := pgWaitDelivery(ctx, t, oldStore, id)
	entered := make(chan context.Context, 1)
	proceed := make(chan struct{})
	defer func() {
		select {
		case <-proceed:
		default:
			close(proceed)
		}
	}()
	contract := pgWaitContract(spec, &matches, &oldApplies)
	contract.Match = func(callbackCtx context.Context, _ []byte) (bool, error) {
		matches.Add(1)
		entered <- callbackCtx
		<-proceed
		return true, nil
	}
	oldDone := make(chan error, 1)
	go func() { _, deliveryErr := oldRunner.DeliverWait(ctx, id, event, contract); oldDone <- deliveryErr }()
	var callbackCtx context.Context
	select {
	case callbackCtx = <-entered:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if _, err = newPool.Exec(newCtx, `UPDATE flowy_executions
SET lease_expiry=clock_timestamp()-interval '1 second' WHERE execution_id=@execution_id`,
		pgx.NamedArgs{executionIDArgument: id}); err != nil {
		t.Fatal(err)
	}
	timer := event
	timer.ID, timer.Kind, timer.Payload = "timer", flowy.WaitTimer, nil
	// Act: new incarnation publishes its timer winner before the old callback returns.
	winner, err := postgresWaitRunner(t, newStore, spec, &nodes, clock).DeliverWait(newCtx, id, timer,
		pgWaitContract(spec, &newMatches, &newApplies))
	if err != nil {
		t.Fatal(err)
	}
	before, err := newStore.LoadExecution(newCtx, id)
	if err != nil {
		t.Fatal(err)
	}
	if callbackCtx.Err() != nil {
		t.Fatalf("fixture lost context before testing storage fencing: %v", callbackCtx.Err())
	}
	close(proceed)
	var rejected error
	select {
	case rejected = <-oldDone:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	after, loadErr := newStore.LoadExecution(newCtx, id)
	// Assert: live old connection reached storage, but its incarnation could publish nothing.
	if !errors.Is(rejected, flowy.ErrLeaseLost) || loadErr != nil || after.Digest != before.Digest ||
		after.Revision != winner.ResumeToken.SnapshotRevision || after.Progress.ExecutionPointer != "timed-out" ||
		matches.Load() != 1 || oldApplies.Load() != 1 || newMatches.Load() != 0 || newApplies.Load() != 1 || nodes.Load() != 1 {
		t.Fatalf("stale matcher overwrote winner: rejected=%v after=%+v load=%v old=%d new=%d",
			rejected, after, loadErr, oldApplies.Load(), newApplies.Load())
	}
	if pingErr := oldPool.Ping(ctx); pingErr != nil {
		t.Fatalf("old connection was closed rather than fenced: %v", pingErr)
	}
	oldPool.Close()
	newPool.Close()
	assertStaleWaitLoserPersistentRedelivery(t, id, event, spec, &nodes)
}

func assertStaleWaitLoserPersistentRedelivery(t *testing.T, id string, event flowy.WaitDelivery,
	spec flowy.DurableWaitSpec, nodes *atomic.Int32,
) {
	t.Helper()
	ctx, pool := racePool(t)
	store, err := NewWaitExecutionStore(pool, postgresWaitProfile())
	if err != nil {
		t.Fatal(err)
	}
	var matches, applies atomic.Int32
	runner := postgresWaitRunner(t, store, spec, nodes, waitAcceptanceClock{now: spec.Deadline})
	contract := pgWaitContract(spec, &matches, &applies)
	contract.Match, contract.Apply = nil, nil
	// Act: retry the unacknowledged old delivery against the durable timer winner.
	loser, loserErr := runner.DeliverWait(ctx, id, event, contract)
	duplicate, duplicateErr := runner.DeliverWait(ctx, id, event, contract)
	envelope, loadErr := store.LoadExecution(ctx, id)
	if loadErr != nil {
		t.Fatal(loadErr)
	}
	waits, inspectErr := flowy.InspectExecutionWaits(envelope)
	// Assert: the rejected stale write never acknowledged a loser; redelivery does.
	if loserErr != nil || duplicateErr != nil || loser.Decision.Status != flowy.WaitLost || loser.Replay ||
		!duplicate.Replay || duplicate.Decision != loser.Decision || duplicate.ResumeToken != loser.ResumeToken ||
		inspectErr != nil || len(waits) != 1 || waits[0].WinnerID != "timer" || len(waits[0].Decisions) != 2 ||
		matches.Load() != 0 || applies.Load() != 0 || nodes.Load() != 1 {
		t.Fatalf("stale delivery lost durable loser: loser=%+v/%v duplicate=%+v/%v waits=%+v inspect=%v",
			loser, loserErr, duplicate, duplicateErr, waits, inspectErr)
	}
	result, resumeErr := runner.Resume(ctx, duplicate.ResumeToken)
	if resumeErr != nil || result.State.Value != 11 || nodes.Load() != 2 || result.Status != flowy.RunStatusCompleted {
		t.Fatalf("timer continuation lost/repeated: %+v err=%v nodes=%d", result, resumeErr, nodes.Load())
	}
}
