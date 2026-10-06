package flowy_test

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/skosovsky/flowy"
	"github.com/skosovsky/flowy/testutil"
)

func TestChildCancellationAcknowledgementDoesNotConfirmWait(t *testing.T) {
	// Arrange: delivery acknowledges the request but gives no evidence of stopping.
	ctx := context.Background()
	store := testutil.NewMemoryExecutionStore(nil)
	var dispatches, notices atomic.Int32
	runner := childJoinRunner(
		t,
		store,
		func(ctx context.Context, state durableTestState) (durableTestState, flowy.Directive, error) {
			group, err := flowy.RunChildren(
				ctx,
				persistedChildPlan(),
				nil,
				func(context.Context, flowy.ChildInvocation) (flowy.ChildResult, error) {
					dispatches.Add(1)
					return flowy.ChildResult{State: flowy.ChildWaiting, WaitID: "external"}, nil
				},
			)
			if !errors.Is(err, flowy.ErrChildrenUnresolved) {
				return state, flowy.End(), err
			}
			_, err = flowy.CancelChildren(
				ctx,
				group,
				flowy.ChildCancelRequest{ID: "stop", Reason: "host requested"},
				func(_ context.Context, notice flowy.ChildCancelNotice) error {
					notices.Add(1)
					persisted := storedChildGroup(t, store, "cancel")
					if !persisted.CancelRequested || persisted.CancelRequest.ID != notice.RequestID {
						return errors.New("notification preceded request commit")
					}
					return nil
				},
			)
			return state, flowy.End(), err
		},
	)
	// Act: run and recover, including a repeated idempotent stop notification.
	first, err := runner.Start(ctx, "cancel", durableTestState{})
	if first == nil || !errors.Is(err, flowy.ErrChildrenUnresolved) {
		t.Fatalf("request falsely completed parent: %v", err)
	}
	_, resumeErr := runner.Resume(ctx, first.ResumeToken)
	group := storedChildGroup(t, store, "cancel")
	// Assert: no new dispatch, no fabricated canceled state, stable request identity.
	if !errors.Is(resumeErr, flowy.ErrChildrenUnresolved) || dispatches.Load() != 1 || notices.Load() != 2 ||
		group.Children[0].State != flowy.ChildWaiting || !group.Children[0].CancelRequested || group.Children[0].CancelConfirmed {
		t.Fatalf("ack interpreted as termination: %v group=%+v", resumeErr, group)
	}
}

func TestChildCancellationPreservesConcurrentOutcomeAndStopsAdmission(t *testing.T) {
	// Arrange: a has an external effect in flight; b has not been admitted.
	ctx := context.Background()
	store := testutil.NewMemoryExecutionStore(nil)
	plan := persistedChildPlan()
	plan.MaxConcurrency = 1
	plan.Children = []flowy.ChildSpec{{ID: "a"}, {ID: "b"}}
	observed := &childOutcomeSignalStore{ExecutionStore: store, committed: make(chan struct{}), once: sync.Once{}}
	var dispatches atomic.Int32
	runner := childJoinRunner(
		t,
		observed,
		func(ctx context.Context, state durableTestState) (durableTestState, flowy.Directive, error) {
			started := make(chan struct{})
			gate := make(chan struct{})
			finished := make(chan error, 1)
			go func() {
				_, runErr := flowy.RunChildren(
					ctx,
					plan,
					nil,
					func(context.Context, flowy.ChildInvocation) (flowy.ChildResult, error) {
						dispatches.Add(1)
						close(started)
						<-gate
						return flowy.ChildResult{State: flowy.ChildCompleted, Payload: []byte("external result")}, nil
					},
				)
				finished <- runErr
			}()
			<-started
			group, err := flowy.PrepareChildren(ctx, plan, nil)
			if err == nil {
				_, err = flowy.CancelChildren(
					ctx,
					group,
					flowy.ChildCancelRequest{ID: "stop", Reason: "host requested"},
					func(context.Context, flowy.ChildCancelNotice) error {
						close(gate)
						<-observed.committed
						return nil
					},
				)
			}
			if err != nil {
				return state, flowy.End(), err
			}
			runErr := <-finished
			if runErr != nil && !errors.Is(runErr, flowy.ErrChildRevision) &&
				!errors.Is(runErr, flowy.ErrChildrenUnresolved) {
				return state, flowy.End(), runErr
			}
			group, err = flowy.PrepareChildren(ctx, plan, nil)
			if err == nil {
				_, err = flowy.JoinChildren(
					ctx,
					group,
					func(_ context.Context, children []flowy.ChildRecord) ([]byte, error) {
						return children[0].Result, nil
					},
				)
			}
			return state, flowy.End(), err
		},
	)
	// Act.
	_, err := runner.Start(ctx, "active", durableTestState{})
	group := storedChildGroup(t, store, "active")
	// Assert: a's external result wins; b is locally confirmed canceled without dispatch.
	if err != nil || dispatches.Load() != 1 || group.Children[0].State != flowy.ChildCompleted ||
		string(group.Children[0].Result) != "external result" || group.Children[0].CancelConfirmed ||
		group.Children[1].State != flowy.ChildCanceled || !group.Children[1].CancelConfirmed {
		t.Fatalf(
			"cancellation discarded outcome or admitted b: %v group=%+v dispatch=%d",
			err,
			group,
			dispatches.Load(),
		)
	}
}

type childOutcomeSignalStore struct {
	flowy.ExecutionStore

	committed chan struct{}
	once      sync.Once
}

func (s *childOutcomeSignalStore) CommitExecution(ctx context.Context, revision uint64, lease flowy.ExecutionLease,
	envelope flowy.ExecutionEnvelope) (flowy.ExecutionEnvelope, error) {
	committed, err := s.ExecutionStore.CommitExecution(ctx, revision, lease, envelope)
	if err != nil {
		return committed, err
	}
	var groups map[string]flowy.ChildGroupRecord
	if json.Unmarshal(committed.ChildrenPayload, &groups) == nil {
		for _, group := range groups {
			for _, child := range group.Children {
				if child.State == flowy.ChildCompleted {
					s.once.Do(func() { close(s.committed) })
				}
			}
		}
	}
	return committed, nil
}
