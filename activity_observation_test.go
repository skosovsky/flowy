package flowy_test

import (
	"bytes"
	"context"
	"errors"
	"reflect"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/skosovsky/flowy"
	"github.com/skosovsky/flowy/testutil"
)

type activityObserver struct {
	mu     sync.Mutex
	events []flowy.LifecycleObservation
}

func (o *activityObserver) ObserveLifecycle(_ context.Context, event flowy.LifecycleObservation) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.events = append(o.events, event)
}

func (o *activityObserver) snapshot() []flowy.LifecycleObservation {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]flowy.LifecycleObservation(nil), o.events...)
}

func TestActivityObservationsRespectAcknowledgementAndReplay(t *testing.T) {
	for _, failAt := range []int32{2, 3, 4, 5} {
		t.Run(strconv.Itoa(int(failAt)), func(t *testing.T) {
			// Arrange: distinct intent/running/outcome/step publication faults.
			observer := &activityObserver{}
			flowy.SetLifecycleObserver(observer)
			t.Cleanup(func() { flowy.SetLifecycleObserver(nil) })
			store := &faultExecutionStore{ExecutionStore: testutil.NewMemoryExecutionStore(nil), failAt: failAt}
			var calls atomic.Int32
			runner := activityTestRunner(t, store, &calls, false)
			// Act: fail an acknowledgement, then recover with host reconciliation.
			failed, err := runner.Start(context.Background(), "observed", durableTestState{})
			beforeRecovery := observer.snapshot()
			if !errors.Is(err, errInjectedCommit) || failed == nil {
				t.Fatalf("missing fault: result=%+v err=%v", failed, err)
			}
			recovery := activityTestRunner(t, store, &calls, true)
			resumed, resumeErr := recovery.Resume(context.Background(), failed.ResumeToken)
			// Assert: dispatch is never repeated; committed belongs to acknowledged outcomes only.
			if resumeErr != nil || resumed.State.Value != 1 || calls.Load() != 1 {
				t.Fatalf("recovery: result=%+v err=%v calls=%d", resumed, resumeErr, calls.Load())
			}
			assertActivityPublicationEvents(t, beforeRecovery, failAt)
			assertActivityRecoveryEvents(t, observer.snapshot(), failAt)
		})
	}
}

func assertActivityPublicationEvents(t *testing.T, events []flowy.LifecycleObservation, failAt int32) {
	t.Helper()
	var got [3]int // started, committed, failed
	for _, event := range events {
		if event.Operation != flowy.LifecycleActivity {
			continue
		}
		assertActivityEventAddress(t, event)
		if event.Stage == flowy.LifecycleStarted {
			got[0]++
		}
		if event.Stage == flowy.LifecycleCommitted {
			got[1]++
		}
		if event.Stage == flowy.LifecycleFailed {
			got[2]++
		}
	}
	want := [3]int{0, 0, 1}
	if failAt >= 4 {
		want[0] = 1
	}
	if failAt == 5 {
		want[1], want[2] = 1, 0
	}
	if got != want {
		t.Fatalf("fault=%d counts=%v want=%v", failAt, got, want)
	}
}

func assertActivityEventAddress(t *testing.T, event flowy.LifecycleObservation) {
	t.Helper()
	if event.ExecutionID != "observed" || event.ActivityID == "" || event.Node != "write" {
		t.Fatalf("missing address: %+v", event)
	}
	if event.Stage == flowy.LifecycleCommitted &&
		(event.Revision <= event.SourceRevision || event.Code != "outcome_completed") {
		t.Fatalf("unacknowledged outcome: %+v", event)
	}
	if event.Stage == flowy.LifecycleFailed && event.Revision != 0 {
		t.Fatalf("failed publication claims revision: %+v", event)
	}
}

func assertActivityRecoveryEvents(t *testing.T, events []flowy.LifecycleObservation, failAt int32) {
	t.Helper()
	for _, event := range events {
		if failAt == 4 && event.Operation == flowy.LifecycleReconcile && event.Stage == flowy.LifecycleCommitted ||
			failAt == 5 && event.Operation == flowy.LifecycleActivity && event.Stage == flowy.LifecycleReplayed ||
			failAt <= 3 && event.Operation == flowy.LifecycleActivity && event.Stage == flowy.LifecycleCommitted {
			if event.Revision == 0 || event.Attempt != 1 {
				t.Fatalf("missing acknowledged attempt: %+v", event)
			}
			return
		}
	}
	t.Fatalf("missing recovery observation for fault %d: %+v", failAt, events)
}

type panicActivityObserver struct{}

func (panicActivityObserver) ObserveLifecycle(context.Context, flowy.LifecycleObservation) {
	panic("observer failed")
}

type reentrantActivityObserver struct {
	busy error
}

func (o *reentrantActivityObserver) ObserveLifecycle(ctx context.Context, event flowy.LifecycleObservation) {
	if event.Operation != flowy.LifecycleActivity || event.Stage != flowy.LifecycleStarted {
		return
	}
	// Reenter the same checkpointer. It must return Busy, not deadlock behind a
	// mutex held by the observer's calling frame or dispatch a second effect.
	_, o.busy = flowy.CallActivity(ctx, flowy.ActivityRequest{
		Key: "write", Implementation: "stable", Input: []byte("same input"),
		Dispatch: func(context.Context, flowy.ActivityInvocation) ([]byte, error) {
			return nil, errors.New("forbidden second dispatch")
		},
	})
}

func TestActivityObservationDoesNotHoldCheckpointerMutex(t *testing.T) {
	// Arrange.
	observer := &reentrantActivityObserver{}
	flowy.SetLifecycleObserver(observer)
	t.Cleanup(func() { flowy.SetLifecycleObserver(nil) })
	var calls atomic.Int32
	runner := activityTestRunner(t, testutil.NewMemoryExecutionStore(nil), &calls, false)
	// Act: a started observation reenters the activity backend before dispatch.
	result, err := runner.Start(context.Background(), "reentrant", durableTestState{})
	// Assert: no held internal mutex and no competing external attempt.
	if err != nil || result.State.Value != 1 || calls.Load() != 1 || !errors.Is(observer.busy, flowy.ErrActivityBusy) {
		t.Fatalf("reentry: result=%+v err=%v busy=%v calls=%d", result, err, observer.busy, calls.Load())
	}
}

func TestActivityObserverDoesNotChangeExecutionProtocol(t *testing.T) {
	// Arrange: identical durable runs with disabled, recording and panicking observers.
	var baseline flowy.ExecutionEnvelope
	for index, observer := range []flowy.LifecycleObserver{nil, &activityObserver{}, panicActivityObserver{}} {
		flowy.SetLifecycleObserver(observer)
		t.Cleanup(func() { flowy.SetLifecycleObserver(nil) })
		store := testutil.NewMemoryExecutionStore(nil)
		var calls atomic.Int32
		runner := activityTestRunner(t, store, &calls, false)
		// Act: execute and replay the terminal token under each observer policy.
		finished, err := runner.Start(context.Background(), "same", durableTestState{})
		if err != nil {
			t.Fatal(err)
		}
		replayed, replayErr := runner.Resume(context.Background(), finished.ResumeToken)
		head, loadErr := store.LoadExecution(context.Background(), "same")
		// Assert: telemetry is not a commit or dispatch dependency.
		if replayErr != nil || loadErr != nil || calls.Load() != 1 ||
			replayed.ResumeToken != finished.ResumeToken || replayed.State.Value != 1 {
			t.Fatalf(
				"policy=%d result=%+v replayErr=%v loadErr=%v calls=%d",
				index,
				replayed,
				replayErr,
				loadErr,
				calls.Load(),
			)
		}
		if index == 0 {
			baseline = head
			continue
		}
		if head.Revision != baseline.Revision || head.Activation != baseline.Activation ||
			!bytes.Equal(head.Progress.StatePayload, baseline.Progress.StatePayload) ||
			!bytes.Equal(head.EffectsPayload, baseline.EffectsPayload) ||
			head.Progress.ExecutionPointer != baseline.Progress.ExecutionPointer ||
			head.RunMeta.StepCount != baseline.RunMeta.StepCount ||
			!reflect.DeepEqual(head.RunMeta.BudgetCounts, baseline.RunMeta.BudgetCounts) {
			t.Fatalf("policy=%d changed semantic checkpoint/protocol", index)
		}
	}
}
