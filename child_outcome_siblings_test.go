package flowy_test

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/skosovsky/flowy"
	"github.com/skosovsky/flowy/testutil"
)

func TestChildOutcomePreservesSiblingsAndDecisionNamespace(t *testing.T) {
	// Arrange: a completed sibling and two unknown children have real allocations.
	ctx := context.Background()
	store := &task24SiblingStore{
		ExecutionStore: testutil.NewMemoryExecutionStore(nil),
		firstCommitted: make(chan struct{}),
	}
	var calls atomic.Int32
	plan := persistedChildPlan()
	plan.MaxConcurrency = 3
	plan.Children = []flowy.ChildSpec{
		{ID: "a", Input: []byte("a"), Allocation: map[string]int{"units": 2}},
		{ID: "b", Input: []byte("b"), Allocation: map[string]int{"units": 3}},
		{ID: "c", Input: []byte("c"), Allocation: map[string]int{"units": 4}},
	}
	runner := childJoinRunner(
		t,
		store,
		func(ctx context.Context, s durableTestState) (durableTestState, flowy.Directive, error) {
			group, err := flowy.RunChildren(
				ctx,
				plan,
				map[string]int{"units": 9},
				func(_ context.Context, i flowy.ChildInvocation) (flowy.ChildResult, error) {
					calls.Add(1)
					if i.ChildID == "a" {
						return flowy.ChildResult{State: flowy.ChildCompleted, Payload: []byte("sibling")}, nil
					}
					<-store.firstCommitted
					return flowy.ChildResult{State: flowy.ChildUnknown}, nil
				},
			)
			if err == nil {
				_, err = flowy.JoinChildren(
					ctx,
					group,
					func(context.Context, []flowy.ChildRecord) ([]byte, error) { return nil, nil },
				)
			}
			return s, flowy.End(), err
		},
	)
	_, err := runner.Start(ctx, "run", durableTestState{})
	if !errors.Is(err, flowy.ErrChildrenUnresolved) {
		t.Fatal(err)
	}
	token, decision, before := task24ChildDecision(t, store)
	child := before.Children[1]
	decision.ChildID, decision.ExecutionID, decision.ChildRevision = child.Spec.ID, child.ExecutionID, child.Revision
	decision.Result = flowy.ChildResult{State: flowy.ChildFailed, Error: "confirmed failure"}
	// Act: settle b, attempt duplicate ID on c, then settle c with its own identity.
	resolved, err := runner.ResolveChildOutcome(ctx, token, decision)
	if err != nil {
		t.Fatal(err)
	}
	_, _, after := task24ChildDecision(t, store)
	third := after.Children[2]
	duplicate := decision
	duplicate.ChildID, duplicate.ExecutionID, duplicate.ChildRevision = third.Spec.ID, third.ExecutionID, third.Revision
	_, duplicateErr := runner.ResolveChildOutcome(ctx, resolved, duplicate)
	duplicate.DecisionID = "third-decision"
	final, err := runner.ResolveChildOutcome(ctx, resolved, duplicate)
	if err != nil {
		t.Fatal(err)
	}
	_, joinErr := runner.Resume(ctx, final)
	latest, _, joined := task24ChildDecision(t, store)
	_, joinedErr := runner.ResolveChildOutcome(ctx, latest, decision)
	// Assert: only addressed child changes, decision IDs cannot move between children, joined outcome stays final.
	if !errors.Is(duplicateErr, flowy.ErrChildRevision) || !errors.Is(joinedErr, flowy.ErrChildRevision) ||
		joinErr != nil ||
		calls.Load() != 3 ||
		!reflect.DeepEqual(before.Children[0], after.Children[0]) ||
		!reflect.DeepEqual(before.Children[2], after.Children[2]) ||
		!reflect.DeepEqual(before.Plan, after.Plan) ||
		!reflect.DeepEqual(before.Children[1].Spec, after.Children[1].Spec) ||
		len(after.BudgetReturns) != 0 ||
		len(joined.MergedIDs) != 3 {
		t.Fatalf(
			"duplicate=%v joined=%v join=%v calls=%d before=%+v after=%+v",
			duplicateErr,
			joinedErr,
			joinErr,
			calls.Load(),
			before,
			after,
		)
	}
}

// Remote unknown callbacks wait for the completed sibling's durable outcome,
// not merely its dispatch return, so the assertion fixture is deterministic.
type task24SiblingStore struct {
	flowy.ExecutionStore

	firstCommitted chan struct{}
	once           sync.Once
}

func (s *task24SiblingStore) CommitExecution(
	ctx context.Context,
	rev uint64,
	lease flowy.ExecutionLease,
	e flowy.ExecutionEnvelope,
) (flowy.ExecutionEnvelope, error) {
	committed, err := s.ExecutionStore.CommitExecution(ctx, rev, lease, e)
	if err != nil {
		return committed, err
	}
	var groups map[string]flowy.ChildGroupRecord
	if json.Unmarshal(e.ChildrenPayload, &groups) == nil {
		for _, group := range groups {
			for _, child := range group.Children {
				if child.Spec.ID == "a" && child.State == flowy.ChildCompleted {
					s.once.Do(func() { close(s.firstCommitted) })
				}
			}
		}
	}
	return committed, nil
}
