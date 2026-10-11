package flowy_test

import (
	"context"
	"errors"
	"reflect"
	"sync/atomic"
	"testing"

	"github.com/skosovsky/flowy"
	"github.com/skosovsky/flowy/testutil"
)

func task24MigrationOutcomeNode(calls, merges *atomic.Int32) flowy.Node[durableTestState, flowy.NoEffect] {
	return func(ctx context.Context, s durableTestState) (durableTestState, flowy.Directive, error) {
		capacity := map[string]int{"units": 8}
		group, err := flowy.RunChildren(
			ctx,
			budgetChildPlan("group", 8),
			capacity,
			func(context.Context, flowy.ChildInvocation) (flowy.ChildResult, error) {
				calls.Add(1)
				return flowy.ChildResult{State: flowy.ChildUnknown}, nil
			},
		)
		if errors.Is(err, flowy.ErrChildrenUnresolved) {
			_, err = flowy.CancelChildren(
				ctx,
				group,
				flowy.ChildCancelRequest{ID: "stop", Reason: "host request"},
				func(context.Context, flowy.ChildCancelNotice) error { return nil },
			)
			if err == nil {
				err = flowy.ErrChildrenUnresolved
			}
			return s, flowy.End(), err
		}
		if err != nil {
			return s, flowy.End(), err
		}
		// Resolution does not return capacity; the original allocation still blocks admission.
		_, budgetErr := flowy.PrepareChildren(ctx, budgetChildPlan("second", 1), capacity)
		if !errors.Is(budgetErr, flowy.ErrBudgetExceeded) {
			return s, flowy.End(), errors.New("outcome implicitly returned budget")
		}
		claim := flowy.ChildBudgetReturn{
			ChildID:       group.Children[0].Spec.ID,
			ChildRevision: group.Children[0].Revision - 1,
			DecisionID:    "usage",
			Reason:        "meter",
			Evidence:      "receipt",
			Used:          map[string]int{"units": 3},
		}
		_, claimErr := flowy.ReturnChildBudget(ctx, group, claim)
		if !errors.Is(claimErr, flowy.ErrChildRevision) {
			return s, flowy.End(), errors.New("old revision returned budget")
		}
		claim.ChildRevision++
		group, err = flowy.ReturnChildBudget(ctx, group, claim)
		if err != nil {
			return s, flowy.End(), err
		}
		group, err = flowy.ReturnChildBudget(ctx, group, claim)
		if err == nil {
			_, err = flowy.JoinChildren(
				ctx,
				group,
				func(context.Context, []flowy.ChildRecord) ([]byte, error) { merges.Add(1); return nil, nil },
			)
		}
		return s, flowy.End(), err
	}
}

func TestChildOutcomeMigrationRetainsIdentityAndRequiresUsageClaim(t *testing.T) {
	// Arrange: unknown child with a cancellation request and fully allocated budget.
	ctx := context.Background()
	store := testutil.NewMemoryExecutionStore(nil)
	var calls, merges atomic.Int32
	body := task24MigrationOutcomeNode(&calls, &merges)
	_, err := childMigrationNodeRunner(t, store, "old", "old-node", body, nil).Start(ctx, "run", durableTestState{})
	if !errors.Is(err, flowy.ErrChildrenUnresolved) {
		t.Fatal(err)
	}
	source, err := store.LoadExecution(ctx, "run")
	if err != nil {
		t.Fatal(err)
	}
	migration := childReferenceMigration(source, childMigrationIdentity(t, source))
	target := childMigrationNodeRunner(t, store, "new", "new-node", body, []flowy.ExecutionMigration{migration})
	pending, err := target.Resume(ctx, flowy.ResumeToken{ThreadID: "run", SnapshotRevision: source.Revision})
	if pending == nil || !errors.Is(err, flowy.ErrChildrenUnresolved) {
		t.Fatal(err)
	}
	token, decision, before := task24ChildDecision(t, store)
	wrong := decision
	wrong.Node = "new-node"
	// Act: only the original address bound to the new cursor is valid.
	_, wrongErr := target.ResolveChildOutcome(ctx, token, wrong)
	resolved, err := target.ResolveChildOutcome(ctx, token, decision)
	if err != nil {
		t.Fatal(err)
	}
	_, _, after := task24ChildDecision(t, store)
	_, cancelErr := target.ConfirmChildCancellation(ctx, resolved, cancellationDecision(after))
	result, resumeErr := target.Resume(ctx, resolved)
	_, _, joined := task24ChildDecision(t, store)
	// Assert: cancellation request loses to confirmed success; budget changes only by explicit claim.
	if !errors.Is(wrongErr, flowy.ErrChildRevision) ||
		!errors.Is(cancelErr, flowy.ErrChildRevision) ||
		resumeErr != nil ||
		result.Status != flowy.RunStatusCompleted ||
		calls.Load() != 1 ||
		merges.Load() != 1 ||
		after.Node != "old-node" ||
		!reflect.DeepEqual(before.Children[0].Spec, after.Children[0].Spec) ||
		!reflect.DeepEqual(before.CancelRequest, after.CancelRequest) ||
		len(after.BudgetReturns) != 0 ||
		joined.BudgetReturns[decision.ChildID].Returned["units"] != 5 ||
		joined.Children[0].OutcomeResolution == nil {
		t.Fatalf(
			"wrong=%v cancel=%v resume=%v calls=%d merges=%d after=%+v joined=%+v",
			wrongErr,
			cancelErr,
			resumeErr,
			calls.Load(),
			merges.Load(),
			after,
			joined,
		)
	}
}

func TestChildOutcomeRacingMigrationPreservesOriginalBinding(t *testing.T) {
	// Arrange: source and target runners compete for the same parent revision.
	ctx := context.Background()
	store := testutil.NewMemoryExecutionStore(nil)
	var calls, merges atomic.Int32
	body := task24MigrationOutcomeNode(&calls, &merges)
	old := childMigrationNodeRunner(t, store, "old", "old-node", body, nil)
	_, err := old.Start(ctx, "run", durableTestState{})
	if !errors.Is(err, flowy.ErrChildrenUnresolved) {
		t.Fatal(err)
	}
	source, err := store.LoadExecution(ctx, "run")
	if err != nil {
		t.Fatal(err)
	}
	migration := childReferenceMigration(source, childMigrationIdentity(t, source))
	target := childMigrationNodeRunner(t, store, "new", "new-node", body, []flowy.ExecutionMigration{migration})
	token, decision, _ := task24ChildDecision(t, store)
	gate := make(chan struct{})
	results := make(chan error, 2)
	// Act.
	go func() { <-gate; _, e := old.ResolveChildOutcome(ctx, token, decision); results <- e }()
	go func() { <-gate; _, e := target.Resume(ctx, token); results <- e }()
	close(gate)
	for range 2 {
		e := <-results
		if e != nil && !errors.Is(e, flowy.ErrThreadLeaseBusy) && !errors.Is(e, flowy.ErrConcurrencyConflict) &&
			!errors.Is(e, flowy.ErrChildrenUnresolved) {
			t.Fatal(e)
		}
	}
	latest, _, group := task24ChildDecision(t, store)
	head, err := store.LoadExecution(ctx, "run")
	if err != nil {
		t.Fatal(err)
	}
	if group.Children[0].OutcomeResolution == nil {
		if head.Descriptor.GraphRevision != "new" {
			t.Fatal("neither operation committed")
		}
		latest, err = target.ResolveChildOutcome(ctx, latest, decision)
		if err != nil {
			t.Fatal(err)
		}
		_, err = target.Resume(ctx, latest)
	} else {
		_, err = old.Resume(ctx, latest)
	}
	// Assert: whichever fence won, original child identity survives with one outcome/join.
	_, _, group = task24ChildDecision(t, store)
	if err != nil ||
		calls.Load() != 1 ||
		merges.Load() != 1 || group.Node != "old-node" ||
		group.Children[0].OutcomeResolution == nil {
		t.Fatalf("err=%v calls=%d merges=%d group=%+v", err, calls.Load(), merges.Load(), group)
	}
}
