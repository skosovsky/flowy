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

type waitArmBarrierStore struct {
	*ExecutionStore

	entered chan struct{}
	proceed chan struct{}
	blocked atomic.Bool
}

func (s *waitArmBarrierStore) CommitExecution(ctx context.Context, revision uint64,
	lease flowy.ExecutionLease, envelope flowy.ExecutionEnvelope,
) (flowy.ExecutionEnvelope, error) {
	if len(envelope.WaitsPayload) != 0 && s.blocked.CompareAndSwap(false, true) {
		close(s.entered)
		select {
		case <-s.proceed:
		case <-ctx.Done():
			return flowy.ExecutionEnvelope{}, context.Cause(ctx)
		}
	}
	return s.ExecutionStore.CommitExecution(ctx, revision, lease, envelope)
}

func TestIntegrationWaitEarlyDeliveryPersistentNotArmedAndCallerRedelivery(t *testing.T) {
	// Arrange: an event is not durably acknowledged before its arm boundary exists.
	ctx, pool := racePool(t)
	if _, err := pool.Exec(ctx, ExecutionSchemaSQL()); err != nil {
		t.Fatal(err)
	}
	base, err := NewWaitExecutionStore(pool, postgresWaitProfile())
	if err != nil {
		t.Fatal(err)
	}
	store := &waitArmBarrierStore{ExecutionStore: base, entered: make(chan struct{}), proceed: make(chan struct{})}
	defer func() {
		select {
		case <-store.proceed:
		default:
			close(store.proceed)
		}
	}()
	id := testThreadID(t)
	spec := postgresWaitSpec(time.Date(2026, 10, 5, 0, 0, 0, 0, time.UTC))
	clock := waitAcceptanceClock{now: spec.Deadline.Add(-time.Minute)}
	var nodes, matches, applies atomic.Int32
	runner := postgresWaitRunner(t, store, spec, &nodes, clock)
	event := flowy.WaitDelivery{Generation: "not-yet-armed", ID: "early-event", Kind: flowy.WaitEvent,
		CorrelationID: spec.CorrelationID, ExpectedRevision: 1, Payload: []byte("approved")}
	contract := pgWaitContract(spec, &matches, &applies)
	assertEarlyWaitNotAccepted(ctx, t, runner, id, event, contract)
	var heads int
	if err = pool.QueryRow(ctx, "SELECT count(*) FROM flowy_executions WHERE execution_id=$1", id).
		Scan(&heads); err != nil {
		t.Fatal(err)
	}
	if heads != 0 {
		t.Fatalf("early delivery created %d execution heads", heads)
	}
	started := make(chan error, 1)
	go func() { _, startErr := runner.Start(ctx, id, intState{}); started <- startErr }()
	select {
	case <-store.entered:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	before, err := base.LoadExecution(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	// Act: initial head exists and lease is held, but arm has not committed.
	assertEarlyWaitNotAccepted(ctx, t, runner, id, event, contract)
	after, err := base.LoadExecution(ctx, id)
	if err != nil || before.Digest != after.Digest || before.Revision != after.Revision || matches.Load() != 0 ||
		applies.Load() != 0 {
		t.Fatalf("early delivery mutated/unnecessarily matched: %+v err=%v", after, err)
	}
	close(store.proceed)
	select {
	case err = <-started:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if err != nil {
		t.Fatal(err)
	}
	armed := pgWaitDelivery(ctx, t, base, id)
	event.Generation, event.ExpectedRevision = armed.Generation, armed.ExpectedRevision
	pool.Close()
	assertEarlyWaitRedeliveryAfterRestart(
		t,
		pool.Config().ConnString(),
		id,
		event,
		spec,
		clock,
		&nodes,
		&matches,
		&applies,
	)
}

func assertEarlyWaitNotAccepted(ctx context.Context, t *testing.T,
	runner *flowy.DurableRunner[intState, flowy.NoEffect], id string, event flowy.WaitDelivery,
	contract flowy.WaitDeliveryContract[intState],
) {
	t.Helper()
	result, err := runner.DeliverWait(ctx, id, event, contract)
	if !errors.Is(err, flowy.ErrWaitNotArmed) || result != (flowy.WaitDeliveryResult{}) {
		t.Fatalf("early delivery acknowledged/acquired worker: %+v err=%v", result, err)
	}
}

func assertEarlyWaitRedeliveryAfterRestart(t *testing.T, dsn string, id string, event flowy.WaitDelivery,
	spec flowy.DurableWaitSpec, clock waitAcceptanceClock, nodes, matches, applies *atomic.Int32,
) {
	t.Helper()
	ctx, pool := openRacePool(t, dsn)
	store, err := NewWaitExecutionStore(pool, postgresWaitProfile())
	if err != nil {
		t.Fatal(err)
	}
	runner := postgresWaitRunner(t, store, spec, nodes, clock)
	contract := pgWaitContract(spec, matches, applies)
	accepted, acceptErr := runner.DeliverWait(ctx, id, event, contract)
	duplicate, duplicateErr := runner.DeliverWait(ctx, id, event, contract)
	// Assert: caller redelivery after arm is durable; duplicate never repeats callbacks.
	if acceptErr != nil || duplicateErr != nil || accepted.Decision.Status != flowy.WaitAccepted || accepted.Replay ||
		!duplicate.Replay || duplicate.Decision != accepted.Decision || matches.Load() != 1 || applies.Load() != 1 || nodes.Load() != 1 {
		t.Fatalf(
			"early redelivery lost/duplicated: accepted=%+v/%v duplicate=%+v/%v",
			accepted,
			acceptErr,
			duplicate,
			duplicateErr,
		)
	}
}
