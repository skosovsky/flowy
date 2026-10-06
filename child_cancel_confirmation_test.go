package flowy_test

import (
	"context"
	"errors"
	"testing"

	"github.com/skosovsky/flowy"
	"github.com/skosovsky/flowy/testutil"
)

func cancellationDecision(group flowy.ChildGroupRecord) flowy.ChildCancelConfirmation {
	child := group.Children[0]
	return flowy.ChildCancelConfirmation{Node: group.Node, Activation: group.Activation, GroupKey: group.Plan.Key,
		ChildID: child.Spec.ID, ExecutionID: child.ExecutionID, ChildRevision: child.Revision,
		RequestID: group.CancelRequest.ID, DecisionID: "confirmed", Reason: "remote worker stopped",
		Evidence: "host verified termination"}
}

func cancelWaitingRunner(
	t *testing.T,
	store flowy.ExecutionStore,
) *flowy.DurableRunner[durableTestState, flowy.NoEffect] {
	t.Helper()
	return childJoinRunner(
		t,
		store,
		func(ctx context.Context, state durableTestState) (durableTestState, flowy.Directive, error) {
			group, err := flowy.RunChildren(
				ctx,
				persistedChildPlan(),
				nil,
				func(context.Context, flowy.ChildInvocation) (flowy.ChildResult, error) {
					return flowy.ChildResult{State: flowy.ChildWaiting, WaitID: "external"}, nil
				},
			)
			if errors.Is(err, flowy.ErrChildrenUnresolved) {
				_, err = flowy.CancelChildren(
					ctx,
					group,
					flowy.ChildCancelRequest{ID: "stop", Reason: "host requested"},
					func(context.Context, flowy.ChildCancelNotice) error {
						return nil
					},
				)
				return state, flowy.End(), err
			}
			if err == nil {
				_, err = flowy.JoinChildren(
					ctx,
					group,
					func(context.Context, []flowy.ChildRecord) ([]byte, error) { return nil, nil },
				)
			}
			return state, flowy.End(), err
		},
	)
}

func TestChildCancellationConfirmationIsAddressedAndDurable(t *testing.T) {
	// Arrange: acknowledgement leaves an external wait pending.
	ctx := context.Background()
	store := testutil.NewMemoryExecutionStore(nil)
	runner := cancelWaitingRunner(t, store)
	first, err := runner.Start(ctx, "confirm", durableTestState{})
	if first == nil || !errors.Is(err, flowy.ErrChildrenUnresolved) {
		t.Fatalf("requested wait missing: %v", err)
	}
	decision := cancellationDecision(storedChildGroup(t, store, "confirm"))
	// Act: confirm once, then attempt stale and duplicate decisions.
	token, err := runner.ConfirmChildCancellation(ctx, first.ResumeToken, decision)
	if err != nil {
		t.Fatal(err)
	}
	_, staleErr := runner.ConfirmChildCancellation(ctx, first.ResumeToken, decision)
	_, duplicateErr := runner.ConfirmChildCancellation(ctx, token, decision)
	group := storedChildGroup(t, store, "confirm")
	_, resumeErr := runner.Resume(ctx, token)
	// Assert: exactly one provenance record and a canceled child eligible for join.
	child := group.Children[0]
	if !errors.Is(staleErr, flowy.ErrConcurrencyConflict) || !errors.Is(duplicateErr, flowy.ErrChildRevision) ||
		resumeErr != nil ||
		child.State != flowy.ChildCanceled ||
		!child.CancelConfirmed ||
		child.WaitID != "" ||
		child.CancelConfirmation == nil ||
		child.CancelConfirmation.PriorState != flowy.ChildWaiting ||
		child.Revision != decision.ChildRevision+1 {
		t.Fatalf(
			"confirmation failed: stale=%v duplicate=%v resume=%v child=%+v",
			staleErr,
			duplicateErr,
			resumeErr,
			child,
		)
	}
}

func TestChildCancellationConfirmationCannotReplaceCompletedWait(t *testing.T) {
	// Arrange: a completed external outcome wins before the confirmation arrives.
	ctx := context.Background()
	store := testutil.NewMemoryExecutionStore(nil)
	runner := cancelWaitingRunner(t, store)
	first, err := runner.Start(ctx, "outcome", durableTestState{})
	if first == nil || !errors.Is(err, flowy.ErrChildrenUnresolved) {
		t.Fatalf("requested wait missing: %v", err)
	}
	group := storedChildGroup(t, store, "outcome")
	decision := cancellationDecision(group)
	token, err := runner.ResolveChildWait(ctx, first.ResumeToken, childWaitDecision(group, 0))
	if err != nil {
		t.Fatal(err)
	}
	decision.ChildRevision++
	// Act: even using the current token/revision cannot cancel a completed child.
	_, confirmErr := runner.ConfirmChildCancellation(ctx, token, decision)
	// Assert.
	latest, loadErr := store.LoadExecution(ctx, "outcome")
	if !errors.Is(confirmErr, flowy.ErrChildRevision) || loadErr != nil || latest.Revision != token.SnapshotRevision ||
		storedChildGroup(t, store, "outcome").Children[0].State != flowy.ChildCompleted {
		t.Fatalf("confirmation replaced outcome: %v load=%v", confirmErr, loadErr)
	}
}
