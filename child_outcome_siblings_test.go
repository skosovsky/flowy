package flowy_test

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/skosovsky/flowy"
	"github.com/skosovsky/flowy/testutil"
)

func TestChildOutcomePreservesSiblingsAndDecisionNamespace(t *testing.T) {
	// Arrange: a completed sibling and two unknown children have real allocations.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	store := &task24SiblingStore{
		ExecutionStore:    testutil.NewMemoryExecutionStore(nil),
		firstCommitted:    make(chan struct{}),
		outcomesCommitted: make(chan struct{}),
	}
	dispatched := make(chan struct{})
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
				task24SiblingDispatcher(store, dispatched, &calls),
			)
			// RunChildren can return after the first unknown. Hold the node lease
			// until every fixture outcome is committed before inspecting siblings.
			if errors.Is(err, flowy.ErrChildrenUnresolved) {
				select {
				case <-store.outcomesCommitted:
				case <-ctx.Done():
					return s, flowy.End(), context.Cause(ctx)
				}
			}
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
	if calls.Load() != 3 || before.Children[0].State != flowy.ChildCompleted ||
		before.Children[1].State != flowy.ChildUnknown || before.Children[2].State != flowy.ChildUnknown {
		t.Fatalf("fixture not ready: calls=%d children=%+v", calls.Load(), before.Children)
	}
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

func task24SiblingDispatcher(
	store *task24SiblingStore,
	dispatched chan struct{},
	calls *atomic.Int32,
) flowy.ChildDispatcher {
	return func(ctx context.Context, i flowy.ChildInvocation) (flowy.ChildResult, error) {
		if calls.Add(1) == 3 {
			close(dispatched)
		}
		select {
		case <-dispatched:
		case <-ctx.Done():
			return flowy.ChildResult{}, context.Cause(ctx)
		}
		if i.ChildID == "a" {
			return flowy.ChildResult{State: flowy.ChildCompleted, Payload: []byte("sibling")}, nil
		}
		select {
		case <-store.firstCommitted:
		case <-ctx.Done():
			return flowy.ChildResult{}, context.Cause(ctx)
		}
		return flowy.ChildResult{State: flowy.ChildUnknown}, nil
	}
}

// Dispatcher entry and durable outcomes have separate barriers. Commit callbacks
// only signal: waiting while CommitExecution holds the runtime mutex would deadlock.
type task24SiblingStore struct {
	flowy.ExecutionStore

	firstCommitted    chan struct{}
	outcomesCommitted chan struct{}
	once              sync.Once
	outcomesOnce      sync.Once
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
			if len(group.Children) == 3 && group.Children[0].State == flowy.ChildCompleted &&
				group.Children[1].State == flowy.ChildUnknown && group.Children[2].State == flowy.ChildUnknown {
				s.outcomesOnce.Do(func() { close(s.outcomesCommitted) })
			}
			for _, child := range group.Children {
				if child.Spec.ID == "a" && child.State == flowy.ChildCompleted {
					s.once.Do(func() { close(s.firstCommitted) })
				}
			}
		}
	}
	return committed, nil
}
