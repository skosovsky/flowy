package flowy_test

import (
	"bytes"
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/skosovsky/flowy"
	"github.com/skosovsky/flowy/testutil"
)

func TestChildOutcomeAndCancellationRaceHaveOneWinner(t *testing.T) {
	// Arrange: same current token addresses two incompatible host decisions.
	ctx := context.Background()
	store := testutil.NewMemoryExecutionStore(nil)
	var calls, merges atomic.Int32
	body := task24MigrationOutcomeNode(&calls, &merges)
	runner := childMigrationNodeRunner(t, store, "current", "node", body, nil)
	_, err := runner.Start(ctx, "run", durableTestState{})
	if !errors.Is(err, flowy.ErrChildrenUnresolved) {
		t.Fatal(err)
	}
	token, decision, group := task24ChildDecision(t, store)
	cancel := cancellationDecision(group)
	gate := make(chan struct{})
	results := make(chan error, 2)
	// Act.
	go func() { <-gate; _, e := runner.ResolveChildOutcome(ctx, token, decision); results <- e }()
	go func() { <-gate; _, e := runner.ConfirmChildCancellation(ctx, token, cancel); results <- e }()
	close(gate)
	successes := 0
	for range 2 {
		e := <-results
		if e == nil {
			successes++
		} else if !errors.Is(e, flowy.ErrThreadLeaseBusy) && !errors.Is(e, flowy.ErrConcurrencyConflict) {
			t.Fatal(e)
		}
	}
	latest, _, after := task24ChildDecision(t, store)
	// Assert: exactly one revision/terminal provenance, never mixed evidence.
	child := after.Children[0]
	if successes != 1 ||
		latest.SnapshotRevision != token.SnapshotRevision+1 ||
		child.Revision != decision.ChildRevision+1 ||
		(child.OutcomeResolution == nil) == (child.CancelConfirmation == nil) ||
		calls.Load() != 1 {
		t.Fatalf("successes=%d child=%+v calls=%d", successes, child, calls.Load())
	}
	_, outcomeErr := runner.ResolveChildOutcome(ctx, latest, decision)
	if child.State == flowy.ChildCanceled && !errors.Is(outcomeErr, flowy.ErrChildRevision) {
		t.Fatal(outcomeErr)
	}
}

func TestChildOutcomeFencesNonCooperativeWorkerAndConcurrentJoin(t *testing.T) {
	// Arrange: worker holds lease while remote dispatch ignores cancellation.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	base := testutil.NewMemoryExecutionStore(nil)
	store := &task24LateStore{ExecutionStore: base, late: make(chan error, 1)}
	gate := make(chan struct{})
	started := make(chan struct{})
	stopped := make(chan error, 1)
	var calls atomic.Int32
	dispatch := func(context.Context, flowy.ChildInvocation) (flowy.ChildResult, error) {
		calls.Add(1)
		close(started)
		<-gate
		return flowy.ChildResult{State: flowy.ChildCompleted, Payload: []byte("late")}, nil
	}
	workerCtx, stopWorker := context.WithCancel(ctx)
	runner := task24ChildJoinRunner(t, store, dispatch)
	go func() { _, e := runner.Start(workerCtx, "run", durableTestState{}); stopped <- e }()
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	token, decision, _ := task24ChildDecision(t, store)
	_, busyErr := runner.ResolveChildOutcome(ctx, token, decision)
	if !errors.Is(busyErr, flowy.ErrThreadLeaseBusy) {
		close(gate)
		t.Fatal(busyErr)
	}
	stopWorker()
	select {
	case <-stopped:
	case <-ctx.Done():
		close(gate)
		t.Fatal(ctx.Err())
	}
	// Act: new owner resolves; a late worker callback is fenced, join competes with replay.
	resolved, err := runner.ResolveChildOutcome(ctx, token, decision)
	if err != nil {
		close(gate)
		t.Fatal(err)
	}
	close(gate)
	select {
	case lateErr := <-store.late:
		if !errors.Is(lateErr, flowy.ErrLeaseLost) && !errors.Is(lateErr, flowy.ErrConcurrencyConflict) {
			t.Fatal("stale callback committed")
		}
	case <-ctx.Done():
		t.Fatal("late callback not observed")
	}
	joinGate := make(chan struct{})
	outcomes := make(chan error, 2)
	go func() { <-joinGate; _, e := runner.Resume(ctx, resolved); outcomes <- e }()
	go func() { <-joinGate; _, e := runner.ResolveChildOutcome(ctx, resolved, decision); outcomes <- e }()
	close(joinGate)
	for range 2 {
		e := <-outcomes
		if e != nil && !errors.Is(e, flowy.ErrThreadLeaseBusy) && !errors.Is(e, flowy.ErrConcurrencyConflict) &&
			!errors.Is(e, flowy.ErrChildRevision) {
			t.Fatal(e)
		}
	}
	// Finish join if the competing resolution held ownership first.
	latest, _, group := task24ChildDecision(t, store)
	_, err = runner.Resume(ctx, latest)
	// Assert: authoritative confirmed outcome remains, remote dispatch was never repeated.
	if err != nil || calls.Load() != 1 || string(group.Children[0].Result) != "done" ||
		group.Children[0].OutcomeResolution == nil {
		t.Fatalf("join=%v calls=%d child=%+v", err, calls.Load(), group.Children[0])
	}
}

type task24LateStore struct {
	flowy.ExecutionStore

	late chan error
}

func (s *task24LateStore) CommitExecution(
	ctx context.Context,
	rev uint64,
	lease flowy.ExecutionLease,
	e flowy.ExecutionEnvelope,
) (flowy.ExecutionEnvelope, error) {
	// The old callback cannot be rescued by its canceled context: test the storage fence itself.
	if bytes.Contains(e.ChildrenPayload, []byte(`"result":"bGF0ZQ=="`)) {
		ctx = context.WithoutCancel(ctx)
	}
	committed, err := s.ExecutionStore.CommitExecution(ctx, rev, lease, e)
	if bytes.Contains(e.ChildrenPayload, []byte(`"result":"bGF0ZQ=="`)) {
		s.late <- err
	}
	return committed, err
}

func TestChildOutcomeAndWaitResolutionCannotCrossStates(t *testing.T) {
	// Arrange: one real wait has a current token and addressed wait ID.
	ctx := context.Background()
	store := testutil.NewMemoryExecutionStore(nil)
	runner := cancelWaitingRunner(t, store)
	_, err := runner.Start(ctx, "run", durableTestState{})
	if !errors.Is(err, flowy.ErrChildrenUnresolved) {
		t.Fatal(err)
	}
	token, outcome, group := task24ChildDecision(t, store)
	child := group.Children[0]
	wait := flowy.ChildWaitResolution{
		Node:          group.Node,
		Activation:    group.Activation,
		GroupKey:      group.Plan.Key,
		ChildID:       child.Spec.ID,
		ExecutionID:   child.ExecutionID,
		ChildRevision: child.Revision,
		WaitID:        child.WaitID,
		DecisionID:    "wait-receipt",
		Result:        outcome.Result,
	}
	gate := make(chan struct{})
	results := make(chan error, 2)
	// Act: APIs compete under the same lease; only wait boundary is eligible.
	go func() { <-gate; _, e := runner.ResolveChildOutcome(ctx, token, outcome); results <- e }()
	go func() { <-gate; _, e := runner.ResolveChildWait(ctx, token, wait); results <- e }()
	close(gate)
	for range 2 {
		e := <-results
		if e != nil && !errors.Is(e, flowy.ErrThreadLeaseBusy) && !errors.Is(e, flowy.ErrChildRevision) &&
			!errors.Is(e, flowy.ErrConcurrencyConflict) {
			t.Fatal(e)
		}
	}
	latest, _, after := task24ChildDecision(t, store)
	if after.Children[0].State == flowy.ChildWaiting {
		latest, err = runner.ResolveChildWait(ctx, latest, wait)
		if err != nil {
			t.Fatal(err)
		}
	}
	// Assert: wait evidence remains sole terminal provenance.
	_, _, after = task24ChildDecision(t, store)
	_, rejectErr := runner.ResolveChildOutcome(ctx, latest, outcome)
	if !errors.Is(rejectErr, flowy.ErrChildRevision) || after.Children[0].WaitResolution == nil ||
		after.Children[0].OutcomeResolution != nil {
		t.Fatalf("err=%v child=%+v", rejectErr, after.Children[0])
	}
}
