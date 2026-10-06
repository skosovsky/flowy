package flowy_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/skosovsky/flowy"
	"github.com/skosovsky/flowy/checkpoint"
)

type waitDeliveryClock struct{ now time.Time }

func (c waitDeliveryClock) Now() time.Time { return c.now }

func deliveryRunner(t *testing.T, store *waitRegistrationStore, calls *atomic.Int32,
	codec flowy.StateSerializer[durableTestState],
) *flowy.DurableRunner[durableTestState, flowy.NoEffect] {
	t.Helper()
	runner, err := waitRunnerForTest(t, store, &store.profile, calls, codec,
		waitDeliveryClock{now: waitSpecForTest().Deadline})
	if err != nil {
		t.Fatal(err)
	}
	return runner
}

func deliveryForArmedWait(t *testing.T, store *waitRegistrationStore) flowy.WaitDelivery {
	t.Helper()
	envelope, err := store.LoadExecution(context.Background(), "run")
	if err != nil {
		t.Fatal(err)
	}
	records, err := flowy.InspectExecutionWaits(envelope)
	if err != nil || len(records) != 1 {
		t.Fatalf("armed record missing: %+v err=%v", records, err)
	}
	return flowy.WaitDelivery{Generation: records[0].Generation, ID: "event", Kind: flowy.WaitEvent,
		CorrelationID: records[0].Spec.CorrelationID, ExpectedRevision: envelope.Revision, Payload: []byte("approved")}
}

func deliveryContract(matches, applies *atomic.Int32) flowy.WaitDeliveryContract[durableTestState] {
	spec := waitSpecForTest()
	return flowy.WaitDeliveryContract[durableTestState]{MatcherLabel: spec.MatcherLabel,
		PayloadCodec: spec.PayloadCodec, ContinuationLabel: spec.ContinuationLabel,
		Match: func(_ context.Context, payload []byte) (bool, error) {
			matches.Add(1)
			matched := string(payload) == "approved"
			payload[0] = 'X'
			return matched, nil
		},
		Apply: func(_ context.Context, state durableTestState, delivery flowy.WaitDelivery) (durableTestState, error) {
			applies.Add(1)
			if delivery.Kind == flowy.WaitEvent && string(delivery.Payload) != "approved" {
				return state, errors.New("matcher mutated continuation payload")
			}
			state.Value += 10
			return state, nil
		}}
}

func TestWaitDeliveryAtomicContinuationAndCodecFreeDuplicateLoser(t *testing.T) {
	t.Parallel()
	// Arrange.
	ctx := context.Background()
	store := newWaitRegistrationStore()
	var calls, matches, applies, decodes atomic.Int32
	runner := deliveryRunner(t, store, &calls, checkpoint.JSONSerializer[durableTestState]{})
	if _, err := runner.Start(ctx, "run", durableTestState{}); err != nil {
		t.Fatal(err)
	}
	delivery := deliveryForArmedWait(t, store)
	contract := deliveryContract(&matches, &applies)
	// Act: acceptance is durable before any node continuation is executed.
	accepted, err := runner.DeliverWait(ctx, "run", delivery, contract)
	if err != nil {
		t.Fatal(err)
	}
	source, loadErr := store.LoadExecution(ctx, "run")
	if loadErr != nil {
		t.Fatal(loadErr)
	}
	state, decodeErr := (checkpoint.JSONSerializer[durableTestState]{}).Unmarshal(source.Progress.StatePayload)
	if decodeErr != nil {
		t.Fatal(decodeErr)
	}
	brokenCodecRunner := deliveryRunner(t, store, &calls, failingDecode{calls: &decodes})
	contract.Match, contract.Apply = nil, nil
	duplicate, duplicateErr := brokenCodecRunner.DeliverWait(ctx, "run", delivery, contract)
	timer := delivery
	timer.ID, timer.Kind, timer.Payload = "timer", flowy.WaitTimer, nil
	loser, loserErr := brokenCodecRunner.DeliverWait(ctx, "run", timer, contract)
	// Assert: same outcome, no state codec/callback/continuation on duplicate or loser.
	if duplicateErr != nil || loserErr != nil || !duplicate.Replay || duplicate.Decision != accepted.Decision ||
		loser.Decision.Status != flowy.WaitLost || accepted.Decision.Status != flowy.WaitAccepted ||
		state.Value != 11 || source.Progress.ExecutionPointer != "accepted" || source.Activation != 2 ||
		calls.Load() != 1 || matches.Load() != 1 || applies.Load() != 1 || decodes.Load() != 0 ||
		string(delivery.Payload) != "approved" || source.Revision != 3 {
		t.Fatalf("atomic delivery/replay failed: accepted=%+v duplicate=%+v/%v loser=%+v/%v state=%+v",
			accepted, duplicate, duplicateErr, loser, loserErr, state)
	}
	recovered, recoverErr := runner.Resume(ctx, loser.ResumeToken)
	if recoverErr != nil || recovered.State.Value != 11 || recovered.Status != flowy.RunStatusCompleted ||
		calls.Load() != 2 {
		t.Fatalf("accepted continuation lost after recovery: %+v err=%v calls=%d", recovered, recoverErr, calls.Load())
	}
}

func TestWaitDeliveryCommitFaultPublishesNoPartialDecisionOrState(t *testing.T) {
	t.Parallel()
	// Arrange.
	ctx := context.Background()
	store := newWaitRegistrationStore()
	store.failAt = 3
	var calls, matches, applies atomic.Int32
	runner := deliveryRunner(t, store, &calls, checkpoint.JSONSerializer[durableTestState]{})
	if _, err := runner.Start(ctx, "run", durableTestState{}); err != nil {
		t.Fatal(err)
	}
	delivery := deliveryForArmedWait(t, store)
	before, err := store.LoadExecution(ctx, "run")
	if err != nil {
		t.Fatal(err)
	}
	// Act.
	uncommitted, failedErr := runner.DeliverWait(ctx, "run", delivery, deliveryContract(&matches, &applies))
	after, loadErr := store.LoadExecution(ctx, "run")
	// Assert: failed write acknowledges nothing and preserves the exact sealed source.
	if !errors.Is(failedErr, errInjectedCommit) || loadErr != nil || uncommitted.Decision.ID != "" ||
		uncommitted.ResumeToken.SnapshotRevision != 0 || after.Digest != before.Digest || after.Revision != before.Revision {
		t.Fatalf(
			"failed acceptance leaked: result=%+v err=%v after=%+v load=%v",
			uncommitted,
			failedErr,
			after,
			loadErr,
		)
	}
	retried, retryErr := runner.DeliverWait(ctx, "run", delivery, deliveryContract(&matches, &applies))
	if retryErr != nil || retried.Decision.Status != flowy.WaitAccepted || matches.Load() != 2 || applies.Load() != 2 ||
		calls.Load() != 1 || retried.ResumeToken.SnapshotRevision != 3 {
		t.Fatalf("pure callback retry failed: %+v err=%v", retried, retryErr)
	}
}

func TestWaitDeliveryUnmatchedAndCompatibilityBeforeDecode(t *testing.T) {
	t.Parallel()
	// Arrange.
	ctx := context.Background()
	store := newWaitRegistrationStore()
	var calls, matches, applies, decodes atomic.Int32
	runner := deliveryRunner(t, store, &calls, checkpoint.JSONSerializer[durableTestState]{})
	if _, err := runner.Start(ctx, "run", durableTestState{}); err != nil {
		t.Fatal(err)
	}
	delivery := deliveryForArmedWait(t, store)
	brokenCodec := deliveryRunner(t, store, &calls, failingDecode{calls: &decodes})
	contract := deliveryContract(&matches, &applies)
	contract.MatcherLabel = "incompatible"
	// Act.
	_, incompatible := brokenCodec.DeliverWait(ctx, "run", delivery, contract)
	// Assert.
	if !errors.Is(incompatible, flowy.ErrExecutionIncompatible) || decodes.Load() != 0 || matches.Load() != 0 ||
		applies.Load() != 0 {
		t.Fatalf(
			"incompatible delivery invoked host: err=%v decodes=%d matches=%d",
			incompatible,
			decodes.Load(),
			matches.Load(),
		)
	}
	contract = deliveryContract(&matches, &applies)
	delivery.Payload = []byte("unmatched")
	rejected, rejectErr := brokenCodec.DeliverWait(ctx, "run", delivery, contract)
	if rejectErr != nil || rejected.Decision.Status != flowy.WaitUnmatched || decodes.Load() != 0 ||
		applies.Load() != 0 {
		t.Fatalf("unmatched decoded/continued: %+v err=%v", rejected, rejectErr)
	}
	contract.Match, contract.Apply = nil, nil
	cached, cachedErr := brokenCodec.DeliverWait(ctx, "run", delivery, contract)
	if cachedErr != nil || !cached.Replay || cached.Decision != rejected.Decision ||
		matches.Load() != 1 || decodes.Load() != 0 {
		t.Fatalf("unmatched duplicate was reinterpreted: %+v err=%v", cached, cachedErr)
	}
	contract = deliveryContract(&matches, &applies)
	delivery.ID, delivery.Payload, delivery.ExpectedRevision = "matched", []byte(
		"approved",
	), rejected.ResumeToken.SnapshotRevision
	accepted, acceptErr := runner.DeliverWait(ctx, "run", delivery, contract)
	if acceptErr != nil || accepted.Decision.Status != flowy.WaitAccepted || matches.Load() != 2 ||
		applies.Load() != 1 {
		t.Fatalf("unmatched blocked new event: %+v err=%v", accepted, acceptErr)
	}
}

func TestWaitDeliveryConcurrentTimerRetriesToDurableLoser(t *testing.T) {
	t.Parallel()
	// Arrange: hold event matcher while another caller tries to own its timer.
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	store := newWaitRegistrationStore()
	var calls, matches, applies atomic.Int32
	runner := deliveryRunner(t, store, &calls, checkpoint.JSONSerializer[durableTestState]{})
	if _, err := runner.Start(ctx, "run", durableTestState{}); err != nil {
		t.Fatal(err)
	}
	delivery := deliveryForArmedWait(t, store)
	entered, proceed := make(chan struct{}), make(chan struct{})
	contract := deliveryContract(&matches, &applies)
	contract.Match = func(ctx context.Context, _ []byte) (bool, error) {
		matches.Add(1)
		close(entered)
		select {
		case <-proceed:
			return true, nil
		case <-ctx.Done():
			return false, ctx.Err()
		}
	}
	eventDone := make(chan error, 1)
	go func() { _, err := runner.DeliverWait(ctx, "run", delivery, contract); eventDone <- err }()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	timer := delivery
	timer.ID, timer.Kind, timer.Payload = "timer", flowy.WaitTimer, nil
	// Act.
	unowned, heldErr := runner.DeliverWait(ctx, "run", timer, contract)
	close(proceed)
	if eventErr := <-eventDone; eventErr != nil {
		t.Fatal(eventErr)
	}
	loser, loserErr := runner.DeliverWait(ctx, "run", timer, contract)
	// Assert: inability to acquire is not an acknowledgement; redelivery stores the loser.
	if !errors.Is(heldErr, flowy.ErrThreadLeaseBusy) || unowned.Decision.ID != "" || loserErr != nil ||
		loser.Decision.Status != flowy.WaitLost || calls.Load() != 1 || matches.Load() != 1 || applies.Load() != 1 {
		t.Fatalf("timer arbitration failed: held=%v loser=%+v err=%v", heldErr, loser, loserErr)
	}
}

func TestWaitDeliveryBeforeArmReturnsNotArmedWithoutMutation(t *testing.T) {
	t.Parallel()
	// Arrange.
	store := newWaitRegistrationStore()
	var calls, matches, applies atomic.Int32
	runner := deliveryRunner(t, store, &calls, checkpoint.JSONSerializer[durableTestState]{})
	// Act.
	_, err := runner.DeliverWait(
		context.Background(),
		"absent",
		flowy.WaitDelivery{ID: "event", Generation: "not-yet-armed"},
		deliveryContract(&matches, &applies),
	)
	// Assert.
	if !errors.Is(err, flowy.ErrWaitNotArmed) || store.commits.Load() != 0 || calls.Load() != 0 ||
		matches.Load() != 0 ||
		applies.Load() != 0 {
		t.Fatalf("early delivery mutated or acknowledged: err=%v", err)
	}
}

type waitOutcomeFailCodec struct{}

func TestWaitDeliveryTimerWinsAndLateEventCannotReplaceTimeout(t *testing.T) {
	t.Parallel()
	// Arrange: the injected deadline is due and no event has yet been accepted.
	ctx := context.Background()
	store := newWaitRegistrationStore()
	var calls, matches, applies, decodes atomic.Int32
	runner := deliveryRunner(t, store, &calls, checkpoint.JSONSerializer[durableTestState]{})
	if _, err := runner.Start(ctx, "run", durableTestState{}); err != nil {
		t.Fatal(err)
	}
	event := deliveryForArmedWait(t, store)
	timer := event
	timer.ID, timer.Kind, timer.Payload = "timer", flowy.WaitTimer, nil
	// Act.
	accepted, err := runner.DeliverWait(ctx, "run", timer, deliveryContract(&matches, &applies))
	if err != nil {
		t.Fatal(err)
	}
	brokenCodec := deliveryRunner(t, store, &calls, failingDecode{calls: &decodes})
	contract := deliveryContract(&matches, &applies)
	contract.Match, contract.Apply = nil, nil
	late, lateErr := brokenCodec.DeliverWait(ctx, "run", event, contract)
	envelope, loadErr := store.LoadExecution(ctx, "run")
	// Assert: event is a durable loser; timer selected the timeout cursor exactly once.
	if lateErr != nil || loadErr != nil || accepted.Decision.Status != flowy.WaitAccepted ||
		late.Decision.Status != flowy.WaitLost || envelope.Progress.ExecutionPointer != "timed-out" ||
		envelope.Activation != 2 || matches.Load() != 0 || applies.Load() != 1 || decodes.Load() != 0 || calls.Load() != 1 {
		t.Fatalf(
			"late event replaced timeout: accepted=%+v late=%+v/%v envelope=%+v load=%v",
			accepted,
			late,
			lateErr,
			envelope,
			loadErr,
		)
	}
}

func (waitOutcomeFailCodec) Marshal(state durableTestState) ([]byte, error) {
	if state.Value >= 11 {
		return nil, errInjectedCommit
	}
	return checkpoint.JSONSerializer[durableTestState]{}.Marshal(state)
}

func (waitOutcomeFailCodec) Unmarshal(payload []byte) (durableTestState, error) {
	return checkpoint.JSONSerializer[durableTestState]{}.Unmarshal(payload)
}

func TestWaitDeliveryCallbackAndCodecFailurePreservesSource(t *testing.T) {
	t.Parallel()
	for _, stage := range []string{"matcher", "apply", "decode", "encode"} {
		t.Run(stage, func(t *testing.T) {
			t.Parallel()
			// Arrange.
			ctx := context.Background()
			store := newWaitRegistrationStore()
			var calls, matches, applies, decodes atomic.Int32
			runner := deliveryRunner(t, store, &calls, checkpoint.JSONSerializer[durableTestState]{})
			if _, err := runner.Start(ctx, "run", durableTestState{}); err != nil {
				t.Fatal(err)
			}
			before, err := store.LoadExecution(ctx, "run")
			if err != nil {
				t.Fatal(err)
			}
			contract := deliveryContract(&matches, &applies)
			switch stage {
			case "matcher":
				contract.Match = func(context.Context, []byte) (bool, error) { return false, errInjectedCommit }
			case "apply":
				contract.Apply = func(_ context.Context, state durableTestState, _ flowy.WaitDelivery) (durableTestState, error) {
					state.Value = 999
					return state, errInjectedCommit
				}
			case "decode":
				runner = deliveryRunner(t, store, &calls, failingDecode{calls: &decodes})
			case "encode":
				runner = deliveryRunner(t, store, &calls, waitOutcomeFailCodec{})
			}
			// Act.
			result, failedErr := runner.DeliverWait(ctx, "run", deliveryForArmedWait(t, store), contract)
			after, loadErr := store.LoadExecution(ctx, "run")
			// Assert: pure host failure is not a committed acceptance or terminal failure.
			if failedErr == nil || result.Decision.ID != "" || result.ResumeToken.SnapshotRevision != 0 ||
				loadErr != nil || after.Digest != before.Digest || after.Revision != before.Revision ||
				after.Terminal != nil || calls.Load() != 1 {
				t.Fatalf(
					"%s failure mutated source: result=%+v err=%v after=%+v load=%v",
					stage,
					result,
					failedErr,
					after,
					loadErr,
				)
			}
		})
	}
}
