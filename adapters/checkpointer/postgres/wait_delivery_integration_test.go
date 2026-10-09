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

type waitAcceptanceClock struct{ now time.Time }

func (c waitAcceptanceClock) Now() time.Time { return c.now }

func pgWaitDelivery(ctx context.Context, t *testing.T, store *ExecutionStore, id string) flowy.WaitDelivery {
	t.Helper()
	envelope, err := store.LoadExecution(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	waits, err := flowy.InspectExecutionWaits(envelope)
	if err != nil || len(waits) != 1 {
		t.Fatalf("wait missing: %+v err=%v", waits, err)
	}
	return flowy.WaitDelivery{Generation: waits[0].Generation, ID: "event", Kind: flowy.WaitEvent,
		CorrelationID: waits[0].Spec.CorrelationID, ExpectedRevision: envelope.Revision, Payload: []byte("approved")}
}

func pgWaitContract(spec flowy.DurableWaitSpec, matches, applies *atomic.Int32) flowy.WaitDeliveryContract[intState] {
	return flowy.WaitDeliveryContract[intState]{MatcherLabel: spec.MatcherLabel, PayloadCodec: spec.PayloadCodec,
		ContinuationLabel: spec.ContinuationLabel,
		Match: func(_ context.Context, payload []byte) (bool, error) {
			matches.Add(1)
			return string(payload) == "approved", nil
		},
		Apply: func(_ context.Context, state intState, _ flowy.WaitDelivery) (intState, error) {
			applies.Add(1)
			state.Value += 10
			return state, nil
		}}
}

func TestIntegrationWaitAcceptancePersistentCrashBeforeContinuationAndLoserReplay(t *testing.T) {
	// Arrange: accepted event is committed, but the worker disappears before Resume.
	ctx, pool := racePool(t)
	if _, err := pool.Exec(ctx, ExecutionSchemaSQL()); err != nil {
		t.Fatal(err)
	}
	store, err := NewWaitExecutionStore(pool, postgresWaitProfile())
	if err != nil {
		t.Fatal(err)
	}
	id := testThreadID(t)
	spec := postgresWaitSpec(time.Date(2026, 10, 5, 0, 0, 0, 0, time.UTC))
	clock := waitAcceptanceClock{now: spec.Deadline}
	var calls, matches, applies atomic.Int32
	runner := postgresWaitRunner(t, store, spec, &calls, clock)
	if _, err = runner.Start(ctx, id, intState{}); err != nil {
		t.Fatal(err)
	}
	delivery := pgWaitDelivery(ctx, t, store, id)
	contract := pgWaitContract(spec, &matches, &applies)
	accepted, err := runner.DeliverWait(ctx, id, delivery, contract)
	if err != nil {
		t.Fatal(err)
	}
	pool.Close()
	restartCtx, restartPool := reopenPool(t, pool)
	restarted, err := NewWaitExecutionStore(restartPool, postgresWaitProfile())
	if err != nil {
		t.Fatal(err)
	}
	recoveredRunner := postgresWaitRunner(t, restarted, spec, &calls, clock)
	contract.Match, contract.Apply = nil, nil
	// Act: duplicate event and old competing timer observe the same persisted winner.
	duplicate, duplicateErr := recoveredRunner.DeliverWait(restartCtx, id, delivery, contract)
	timer := delivery
	timer.ID, timer.Kind, timer.Payload = "timer", flowy.WaitTimer, nil
	loser, loserErr := recoveredRunner.DeliverWait(restartCtx, id, timer, contract)
	// Assert: no host callback/node repeated, and only one selected continuation exists.
	if duplicateErr != nil || loserErr != nil || !duplicate.Replay || duplicate.Decision != accepted.Decision ||
		loser.Decision.Status != flowy.WaitLost || calls.Load() != 1 || matches.Load() != 1 || applies.Load() != 1 {
		t.Fatalf("persistent acceptance lost: duplicate=%+v/%v loser=%+v/%v calls=%d matches=%d applies=%d",
			duplicate, duplicateErr, loser, loserErr, calls.Load(), matches.Load(), applies.Load())
	}
	envelope, loadErr := restarted.LoadExecution(restartCtx, id)
	if loadErr != nil {
		t.Fatal(loadErr)
	}
	waits, inspectErr := flowy.InspectExecutionWaits(envelope)
	if inspectErr != nil || len(waits) != 1 || waits[0].WinnerID != "event" || len(waits[0].Decisions) != 2 ||
		envelope.Progress.ExecutionPointer != "accepted" || envelope.Activation != 2 || envelope.Terminal != nil {
		t.Fatalf("winner/loser/continuation not atomic: %+v waits=%+v err=%v", envelope, waits, inspectErr)
	}
	result, resumeErr := recoveredRunner.Resume(restartCtx, loser.ResumeToken)
	if resumeErr != nil || result.State.Value != 11 || result.Status != flowy.RunStatusCompleted || calls.Load() != 2 {
		t.Fatalf("persistent continuation duplicated/lost: %+v err=%v calls=%d", result, resumeErr, calls.Load())
	}
}

type waitAcceptanceFaultStore struct {
	*ExecutionStore

	fail atomic.Bool
}

func (s *waitAcceptanceFaultStore) CommitExecution(ctx context.Context, revision uint64,
	lease flowy.ExecutionLease, envelope flowy.ExecutionEnvelope,
) (flowy.ExecutionEnvelope, error) {
	if envelope.Progress.ExecutionPointer == "accepted" && envelope.Terminal == nil && s.fail.Swap(false) {
		return flowy.ExecutionEnvelope{}, errors.New("injected acceptance commit failure")
	}
	return s.ExecutionStore.CommitExecution(ctx, revision, lease, envelope)
}

func TestIntegrationWaitAcceptancePersistentCommitFailureRetainsSource(t *testing.T) {
	// Arrange: fail the whole acceptance write, never just the state or journal side.
	ctx, pool := racePool(t)
	if _, err := pool.Exec(ctx, ExecutionSchemaSQL()); err != nil {
		t.Fatal(err)
	}
	store, err := NewWaitExecutionStore(pool, postgresWaitProfile())
	if err != nil {
		t.Fatal(err)
	}
	fault := &waitAcceptanceFaultStore{ExecutionStore: store}
	fault.fail.Store(true)
	var calls, matches, applies atomic.Int32
	spec := postgresWaitSpec(time.Date(2026, 10, 5, 0, 0, 0, 0, time.UTC))
	id := testThreadID(t)
	runner := postgresWaitRunner(t, fault, spec, &calls)
	if _, err = runner.Start(ctx, id, intState{}); err != nil {
		t.Fatal(err)
	}
	delivery := pgWaitDelivery(ctx, t, store, id)
	source, err := store.LoadExecution(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	// Act.
	failed, failedErr := runner.DeliverWait(ctx, id, delivery, pgWaitContract(spec, &matches, &applies))
	after, afterErr := store.LoadExecution(ctx, id)
	// Assert: the failed acknowledgement has no decision/token and source remains exact.
	if failedErr == nil || failed.Decision.ID != "" || failed.ResumeToken.SnapshotRevision != 0 ||
		afterErr != nil || after.Digest != source.Digest || after.Revision != source.Revision || after.Activation != 1 {
		t.Fatalf(
			"failed acceptance partially published: %+v err=%v after=%+v load=%v",
			failed,
			failedErr,
			after,
			afterErr,
		)
	}
	pool.Close()
	restartCtx, restartPool := reopenPool(t, pool)
	restarted, err := NewWaitExecutionStore(restartPool, postgresWaitProfile())
	if err != nil {
		t.Fatal(err)
	}
	retried, retryErr := postgresWaitRunner(t, restarted, spec, &calls).DeliverWait(restartCtx, id, delivery,
		pgWaitContract(spec, &matches, &applies))
	if retryErr != nil || retried.Decision.Status != flowy.WaitAccepted || retried.ResumeToken.SnapshotRevision != 3 ||
		calls.Load() != 1 || matches.Load() != 2 || applies.Load() != 2 {
		t.Fatalf("persistent pure retry failed: %+v err=%v", retried, retryErr)
	}
}
