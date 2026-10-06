package flowy_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/skosovsky/flowy"
	"github.com/skosovsky/flowy/testutil"
)

func TestChildCancellationRequestCommitFailurePreventsNotification(t *testing.T) {
	// Arrange: the external wait is committed, but the following cancellation intent fails.
	ctx := context.Background()
	base := testutil.NewMemoryExecutionStore(nil)
	store := &faultExecutionStore{ExecutionStore: base, failAt: 6}
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
				func(context.Context, flowy.ChildCancelNotice) error {
					notices.Add(1)
					return nil
				},
			)
			return state, flowy.End(), err
		},
	)
	// Act: unavailable intent must not notify; recovery may persist and notify once.
	first, err := runner.Start(ctx, "intent-fault", durableTestState{})
	group := storedChildGroup(t, base, "intent-fault")
	if first == nil || err == nil || notices.Load() != 0 || group.CancelRequested || group.CancelRequest != nil {
		t.Fatalf("notification escaped failed intent: %v notices=%d group=%+v", err, notices.Load(), group)
	}
	_, resumeErr := runner.Resume(ctx, first.ResumeToken)
	group = storedChildGroup(t, base, "intent-fault")
	// Assert: retry does not relaunch the waiting child or fabricate stop confirmation.
	if !errors.Is(resumeErr, flowy.ErrChildrenUnresolved) || notices.Load() != 1 || dispatches.Load() != 1 ||
		!group.CancelRequested || group.Children[0].CancelConfirmed || group.Children[0].State != flowy.ChildWaiting {
		t.Fatalf(
			"intent recovery: %v notices=%d dispatches=%d group=%+v",
			resumeErr,
			notices.Load(),
			dispatches.Load(),
			group,
		)
	}
}
