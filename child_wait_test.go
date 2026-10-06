package flowy_test

import (
	"context"
	"encoding/json"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/skosovsky/flowy"
	"github.com/skosovsky/flowy/testutil"
)

func storedChildGroup(t *testing.T, store flowy.ExecutionStore, id string) flowy.ChildGroupRecord {
	t.Helper()
	envelope, err := store.LoadExecution(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	var groups map[string]flowy.ChildGroupRecord
	if err := json.Unmarshal(envelope.ChildrenPayload, &groups); err != nil {
		t.Fatal(err)
	}
	for _, group := range groups {
		return group
	}
	t.Fatal("missing child group")
	return flowy.ChildGroupRecord{}
}

func childWaitDecision(group flowy.ChildGroupRecord, index int) flowy.ChildWaitResolution {
	child := group.Children[index]
	return flowy.ChildWaitResolution{Node: group.Node, Activation: group.Activation, GroupKey: group.Plan.Key,
		ChildID: child.Spec.ID, ExecutionID: child.ExecutionID, ChildRevision: child.Revision,
		WaitID: child.WaitID, DecisionID: "decision-" + child.Spec.ID,
		Result: flowy.ChildResult{State: flowy.ChildCompleted, Payload: []byte(child.Spec.ID)}}
}

func TestChildWaitResolutionPreservesSiblingAndRejectsStale(t *testing.T) {
	// Arrange: one completed child and two independent external waits.
	ctx := context.Background()
	store := testutil.NewMemoryExecutionStore(nil)
	plan := persistedChildPlan()
	plan.Children = []flowy.ChildSpec{{ID: "a"}, {ID: "b"}, {ID: "c"}}
	var dispatches, merges atomic.Int32
	runner := childJoinRunner(
		t,
		store,
		func(ctx context.Context, state durableTestState) (durableTestState, flowy.Directive, error) {
			group, err := flowy.RunChildren(
				ctx,
				plan,
				nil,
				func(_ context.Context, invocation flowy.ChildInvocation) (flowy.ChildResult, error) {
					dispatches.Add(1)
					if invocation.ChildID == "a" {
						return flowy.ChildResult{State: flowy.ChildCompleted, Payload: []byte("a")}, nil
					}
					return flowy.ChildResult{State: flowy.ChildWaiting, WaitID: "wait-" + invocation.ChildID}, nil
				},
			)
			if err == nil {
				_, err = flowy.JoinChildren(
					ctx,
					group,
					func(_ context.Context, children []flowy.ChildRecord) ([]byte, error) {
						merges.Add(1)
						return children[0].Result, nil
					},
				)
			}
			return state, flowy.End(), err
		},
	)
	first, err := runner.Start(ctx, "waits", durableTestState{})
	if !errors.Is(err, flowy.ErrChildrenUnresolved) || first == nil {
		t.Fatalf("initial waits: %v", err)
	}
	group := storedChildGroup(t, store, "waits")
	decision := childWaitDecision(group, 1)
	// Act: resolve only b, then recover the same parent activation.
	token, err := runner.ResolveChildWait(ctx, first.ResumeToken, decision)
	if err != nil {
		t.Fatal(err)
	}
	_, staleErr := runner.ResolveChildWait(ctx, first.ResumeToken, decision)
	_, duplicateErr := runner.ResolveChildWait(ctx, token, decision)
	second, resumeErr := runner.Resume(ctx, token)
	group = storedChildGroup(t, store, "waits")
	// Assert: c remains waiting and a/b are not dispatched again.
	if !errors.Is(staleErr, flowy.ErrConcurrencyConflict) || !errors.Is(duplicateErr, flowy.ErrChildRevision) ||
		!errors.Is(resumeErr, flowy.ErrChildrenUnresolved) || second == nil || dispatches.Load() != 3 ||
		group.Children[0].State != flowy.ChildCompleted || group.Children[1].State != flowy.ChildCompleted ||
		group.Children[1].WaitResolution == nil || group.Children[2].State != flowy.ChildWaiting || group.Children[2].WaitID != "wait-c" {
		t.Fatalf(
			"sibling wait lost: stale=%v duplicate=%v resume=%v group=%+v",
			staleErr,
			duplicateErr,
			resumeErr,
			group,
		)
	}
	token, err = runner.ResolveChildWait(ctx, second.ResumeToken, childWaitDecision(group, 2))
	if err != nil {
		t.Fatal(err)
	}
	_, err = runner.Resume(ctx, token)
	if err != nil || dispatches.Load() != 3 || merges.Load() != 1 {
		t.Fatalf("final child join: %v dispatches=%d merges=%d", err, dispatches.Load(), merges.Load())
	}
}
