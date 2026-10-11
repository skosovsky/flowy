package flowy_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/skosovsky/flowy"
	"github.com/skosovsky/flowy/checkpoint"
	"github.com/skosovsky/flowy/testutil"
)

type leaseObservationProbe struct {
	activityObserver

	losses          atomic.Int32
	canceled        atomic.Bool
	carrier         atomic.Value
	releaseFinished *atomic.Bool
	badReleaseOrder atomic.Bool
}

func (o *leaseObservationProbe) ObserveLifecycle(ctx context.Context, event flowy.LifecycleObservation) {
	if event.Operation == flowy.LifecycleLease {
		if o.releaseFinished != nil && !o.releaseFinished.Load() {
			o.badReleaseOrder.Store(true)
		}
		o.losses.Add(1)
		o.canceled.Store(errors.Is(context.Cause(ctx), flowy.ErrLeaseLost))
		carrier, _ := ctx.Value(durableTraceKey{}).(string)
		o.carrier.Store(carrier)
	}
	o.activityObserver.ObserveLifecycle(ctx, event)
}

type releaseObservationStore struct {
	flowy.ExecutionStore

	finished *atomic.Bool
}

func (s *releaseObservationStore) ReleaseExecution(ctx context.Context, lease flowy.ExecutionLease) error {
	err := s.ExecutionStore.ReleaseExecution(ctx, lease)
	s.finished.Store(true)
	return err
}

func TestLeaseObservationIncludesLossBeforeNodeExecution(t *testing.T) {
	for _, phase := range []string{"initial codec", "migration"} {
		t.Run(phase, func(t *testing.T) {
			// Arrange: an observer around the existing deterministic heartbeat barriers.
			observer := &leaseObservationProbe{}
			flowy.SetLifecycleObserver(observer)
			t.Cleanup(func() { flowy.SetLifecycleObserver(nil) })
			// Act: the helper blocks pre-run host work until renewal loses ownership.
			assertPreExecutionLease(t, phase, true)
			// Assert: loss is emitted once, after shutdown/release, with canceled callback context.
			if observer.losses.Load() != 1 || !observer.canceled.Load() {
				t.Fatalf("phase=%s losses=%d canceled=%t", phase, observer.losses.Load(), observer.canceled.Load())
			}
			event := requireRuntimeObservation(
				t,
				observer.snapshot(),
				flowy.LifecycleLease,
				flowy.LifecycleFailed,
				"lease_lost",
			)
			if event.ExecutionID != "run" || event.LeaseIncarnation == 0 {
				t.Fatalf("lease address missing: %+v", event)
			}
		})
	}
}

func TestManualWaitCommitLeaseLossRestoresCarrierAndReleasesBeforeObserver(t *testing.T) {
	// Arrange: a real memory fence clock, an armed wait, and the source carrier.
	var releaseFinished atomic.Bool
	observer := &leaseObservationProbe{releaseFinished: &releaseFinished}
	flowy.SetLifecycleObserver(observer)
	t.Cleanup(func() { flowy.SetLifecycleObserver(nil) })
	var injections atomic.Int32
	flowy.SetTelemetryBridge(durableTraceBridge{injections: &injections})
	t.Cleanup(func() { flowy.SetTelemetryBridge(nil) })
	clock := &testExecutionClock{}
	clock.set(waitSpecForTest().Deadline)
	store := newWaitRegistrationStore()
	store.ExecutionStore = &releaseObservationStore{
		ExecutionStore: testutil.NewMemoryExecutionStore(clock.Now),
		finished:       &releaseFinished,
	}
	var calls, matches, applies atomic.Int32
	runner := deliveryRunner(t, store, &calls, checkpoint.JSONSerializer[durableTestState]{})
	sourceContext := context.WithValue(context.Background(), durableTraceKey{}, "lease-source-trace")
	if _, err := runner.Start(sourceContext, "run", durableTestState{}); err != nil {
		t.Fatal(err)
	}
	delivery := deliveryForArmedWait(t, store)
	releaseFinished.Store(false)
	contract := deliveryContract(&matches, &applies)
	originalApply := contract.Apply
	contract.Apply = func(ctx context.Context, state durableTestState, input flowy.WaitDelivery) (durableTestState, error) {
		clock.set(clock.Now().Add(2 * time.Minute))
		return originalApply(ctx, state, input)
	}
	// Act: pure continuation finishes after its storage lease has expired.
	_, err := runner.DeliverWait(context.Background(), "run", delivery, contract)
	// Assert: no acknowledged acceptance; exactly one loss diagnostic with source trace.
	if !errors.Is(err, flowy.ErrLeaseLost) || observer.losses.Load() != 1 || !observer.canceled.Load() ||
		observer.carrier.Load() != "lease-source-trace" || calls.Load() != 1 || observer.badReleaseOrder.Load() {
		t.Fatalf("manual loss: err=%v losses=%d canceled=%t carrier=%v", err,
			observer.losses.Load(), observer.canceled.Load(), observer.carrier.Load())
	}
	event := requireRuntimeObservation(
		t,
		observer.snapshot(),
		flowy.LifecycleLease,
		flowy.LifecycleFailed,
		"lease_lost",
	)
	if event.SourceRevision != delivery.ExpectedRevision || event.Node != "waiting" || event.LeaseIncarnation == 0 {
		t.Fatalf("manual source address: %+v", event)
	}
	assertNoAcknowledgedOutcome(t, observer.snapshot(), flowy.LifecycleWaitDelivery)
}
