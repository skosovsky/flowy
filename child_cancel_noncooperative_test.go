package flowy_test

import (
	"context"
	"encoding/json"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/skosovsky/flowy"
	"github.com/skosovsky/flowy/testutil"
)

type childLateOutcomeStore struct {
	flowy.ExecutionStore

	late chan childLateOutcomeObservation
}

type childLateOutcomeObservation struct {
	err         error
	deadline    time.Time
	hasDeadline bool
	contextErr  error
}

func (s *childLateOutcomeStore) CommitExecution(ctx context.Context, revision uint64, lease flowy.ExecutionLease,
	envelope flowy.ExecutionEnvelope) (flowy.ExecutionEnvelope, error) {
	committed, err := s.ExecutionStore.CommitExecution(ctx, revision, lease, envelope)
	var groups map[string]flowy.ChildGroupRecord
	if json.Unmarshal(envelope.ChildrenPayload, &groups) == nil {
		for _, group := range groups {
			for _, child := range group.Children {
				if child.State == flowy.ChildCompleted {
					deadline, hasDeadline := ctx.Deadline()
					s.late <- childLateOutcomeObservation{err: err, deadline: deadline, hasDeadline: hasDeadline, contextErr: ctx.Err()}
				}
			}
		}
	}
	return committed, err
}

func TestChildCancellationReleasesNonCooperativeCoordinatorAndFencesLateOutcome(t *testing.T) {
	// Arrange: remote work ignores context until the test explicitly lets it finish.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	base := testutil.NewMemoryExecutionStore(nil)
	store := &childLateOutcomeStore{ExecutionStore: base, late: make(chan childLateOutcomeObservation, 1)}
	gate := make(chan struct{})
	defer func() {
		select {
		case <-gate:
		default:
			close(gate)
		}
	}()
	var calls atomic.Int32
	runner := childJoinRunner(
		t,
		store,
		func(ctx context.Context, state durableTestState) (durableTestState, flowy.Directive, error) {
			if calls.Load() != 0 {
				group, runErr := flowy.RunChildren(
					ctx,
					persistedChildPlan(),
					nil,
					func(context.Context, flowy.ChildInvocation) (flowy.ChildResult, error) {
						calls.Add(100)
						return flowy.ChildResult{State: flowy.ChildCompleted}, nil
					},
				)
				if runErr == nil {
					_, runErr = flowy.JoinChildren(
						ctx,
						group,
						func(context.Context, []flowy.ChildRecord) ([]byte, error) { return nil, nil },
					)
				}
				return state, flowy.End(), runErr
			}
			started := make(chan struct{})
			finished := make(chan error, 1)
			go func() {
				_, runErr := flowy.RunChildren(
					ctx,
					persistedChildPlan(),
					nil,
					func(context.Context, flowy.ChildInvocation) (flowy.ChildResult, error) {
						calls.Add(1)
						close(started)
						<-gate
						return flowy.ChildResult{
							State:   flowy.ChildCompleted,
							Payload: []byte("late external result"),
						}, nil
					},
				)
				finished <- runErr
			}()
			<-started
			group, prepareErr := flowy.PrepareChildren(ctx, persistedChildPlan(), nil)
			if prepareErr != nil {
				return state, flowy.End(), prepareErr
			}
			_, cancelErr := flowy.CancelChildren(
				ctx,
				group,
				flowy.ChildCancelRequest{ID: "stop", Reason: "host requested"},
				func(context.Context, flowy.ChildCancelNotice) error { return nil },
			)
			if cancelErr != nil {
				return state, flowy.End(), cancelErr
			}
			select {
			case runErr := <-finished:
				return state, flowy.End(), runErr
			case <-ctx.Done():
				return state, flowy.End(), ctx.Err()
			}
		},
	)
	// Act: coordinator releases ownership, recovery marks unknown, host confirms stop.
	first, err := runner.Start(ctx, "noncooperative", durableTestState{})
	if first == nil || !errors.Is(err, flowy.ErrChildrenUnresolved) {
		t.Fatalf("coordinator did not release: %v", err)
	}
	assertChildCancelNonCooperativeRecovery(ctx, t, runner, store, first.ResumeToken, gate, &calls)
}

func assertChildCancelNonCooperativeRecovery(ctx context.Context, t *testing.T,
	runner *flowy.DurableRunner[durableTestState, flowy.NoEffect], store *childLateOutcomeStore,
	token flowy.ResumeToken, gate chan struct{}, calls *atomic.Int32) {
	t.Helper()
	base := store.ExecutionStore
	second, err := runner.Resume(ctx, token)
	group := storedChildGroup(t, base, "noncooperative")
	if second == nil || !errors.Is(err, flowy.ErrChildrenUnresolved) || group.Children[0].State != flowy.ChildUnknown ||
		group.Children[0].CancelConfirmed {
		t.Fatalf("recovery fabricated stop: %v child=%+v", err, group.Children[0])
	}
	token, err = runner.ConfirmChildCancellation(ctx, second.ResumeToken, cancellationDecision(group))
	if err != nil {
		t.Fatal(err)
	}
	_, err = runner.Resume(ctx, token)
	if err != nil {
		t.Fatal(err)
	}
	before, err := base.LoadExecution(ctx, "noncooperative")
	if err != nil {
		t.Fatal(err)
	}
	close(gate)
	select {
	case late := <-store.late:
		if !late.hasDeadline || late.deadline.IsZero() || time.Until(late.deadline) > 5*time.Second ||
			late.contextErr != nil {
			t.Fatalf("late outcome I/O lacks detached bounded context: %+v", late)
		}
		if late.err == nil {
			t.Fatal("old worker committed after confirmation")
		}
	case <-ctx.Done():
		t.Fatal("late outcome commit not observed")
	}
	// Assert: stale outcome cannot rewrite confirmation/terminal or dispatch again.
	after, err := base.LoadExecution(ctx, "noncooperative")
	if err != nil || before.Digest != after.Digest || calls.Load() != 1 || after.Terminal == nil {
		t.Fatalf("late outcome changed execution: %v calls=%d", err, calls.Load())
	}
}
