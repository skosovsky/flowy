package flowy_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/skosovsky/flowy"
	"github.com/skosovsky/flowy/checkpoint"
	"github.com/skosovsky/flowy/testutil"
)

func task24ChildJoinRunner(
	t *testing.T,
	store flowy.ExecutionStore,
	dispatch flowy.ChildDispatcher,
) *flowy.DurableRunner[durableTestState, flowy.NoEffect] {
	t.Helper()
	b := flowy.NewGraph[durableTestState, flowy.NoEffect](func(_, u durableTestState) durableTestState { return u })
	b.AddNode("node", func(ctx context.Context, s durableTestState) (durableTestState, flowy.Directive, error) {
		group, err := flowy.RunChildren(ctx, persistedChildPlan(), nil, dispatch)
		if err != nil {
			return s, flowy.Fail("children"), err
		}
		payload, err := flowy.JoinChildren(
			ctx,
			group,
			func(_ context.Context, children []flowy.ChildRecord) ([]byte, error) { return children[0].Result, nil },
		)
		if err != nil {
			return s, flowy.Fail("join"), err
		}
		s.Value = len(payload)
		return s, flowy.End(), nil
	}).SetEntryPoint("node").AllowNoOutgoingRoute("node")
	g, err := b.Compile()
	if err != nil {
		t.Fatal(err)
	}
	r, err := flowy.NewDurableRunner(
		g,
		store,
		durableDescriptor("current"),
		checkpoint.JSONSerializer[durableTestState]{},
		checkpoint.JSONSerializer[[]flowy.NoEffect]{},
		flowy.DurableOptions{Owner: "worker", LeaseTTL: time.Minute},
	)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func task24ChildDecision(
	t *testing.T,
	store flowy.ExecutionStore,
) (flowy.ResumeToken, flowy.ChildOutcomeResolution, flowy.ChildGroupRecord) {
	t.Helper()
	e, err := store.LoadExecution(context.Background(), "run")
	if err != nil {
		t.Fatal(err)
	}
	var groups map[string]flowy.ChildGroupRecord
	if err = json.Unmarshal(e.ChildrenPayload, &groups); err != nil {
		t.Fatal(err)
	}
	if len(groups) != 1 {
		t.Fatalf("groups=%+v", groups)
	}
	for _, group := range groups {
		child := group.Children[0]
		return flowy.ResumeToken{
			ThreadID:         "run",
			SnapshotRevision: e.Revision,
		}, flowy.ChildOutcomeResolution{
			Node:          group.Node,
			Activation:    group.Activation,
			GroupKey:      group.Plan.Key,
			GroupLabel:    group.Plan.Label,
			ChildID:       child.Spec.ID,
			ExecutionID:   child.ExecutionID,
			ChildRevision: child.Revision,
			DecisionID:    "found-outcome",
			Reason:        "host receipt",
			Evidence:      "receipt-1",
			Result:        flowy.ChildResult{State: flowy.ChildCompleted, Payload: []byte("done")},
		}, group
	}
	t.Fatal("no group")
	return flowy.ResumeToken{}, flowy.ChildOutcomeResolution{}, flowy.ChildGroupRecord{}
}

//nolint:gocognit // abandoned running/unknown and completed/failed recovery matrix
func TestChildOutcomeResolutionAfterLostOutcome(t *testing.T) {
	for _, normalize := range []bool{false, true} {
		for _, failed := range []bool{false, true} {
			t.Run(fmt.Sprintf("normalize=%t/failed=%t", normalize, failed), func(t *testing.T) {
				// Arrange: external write completed, but its parent outcome commit failed.
				ctx := context.Background()
				base := testutil.NewMemoryExecutionStore(nil)
				faulty := &faultExecutionStore{ExecutionStore: base, failAt: 5}
				var calls atomic.Int32
				dispatch := func(context.Context, flowy.ChildInvocation) (flowy.ChildResult, error) {
					calls.Add(1)
					return flowy.ChildResult{State: flowy.ChildCompleted, Payload: []byte("done")}, nil
				}
				_, err := task24ChildJoinRunner(t, faulty, dispatch).Start(ctx, "run", durableTestState{})
				if !errors.Is(err, errInjectedCommit) || calls.Load() != 1 {
					t.Fatalf("fault err=%v calls=%d", err, calls.Load())
				}
				restarted := task24ChildJoinRunner(t, base, dispatch)
				token, _, _ := task24ChildDecision(t, base)
				if normalize {
					_, err = restarted.Resume(ctx, token)
					if !errors.Is(err, flowy.ErrChildrenUnresolved) {
						t.Fatal(err)
					}
				}
				token, decision, before := task24ChildDecision(t, base)
				if failed {
					decision.Result.State = flowy.ChildFailed
					decision.Result.Error = "confirmed remote failure"
				}
				// Act: resolution never dispatches or joins implicitly.
				resolved, err := restarted.ResolveChildOutcome(ctx, token, decision)
				if err != nil {
					t.Fatal(err)
				}
				_, _, after := task24ChildDecision(t, base)
				replay, replayErr := restarted.ResolveChildOutcome(ctx, resolved, decision)
				_, staleErr := restarted.ResolveChildOutcome(ctx, token, decision)
				conflicting := decision
				conflicting.Result.Payload = []byte("changed")
				_, conflictErr := restarted.ResolveChildOutcome(ctx, resolved, conflicting)
				result, resumeErr := restarted.Resume(ctx, resolved)
				// Assert: exact replay is read-only, stale/conflicting decisions do not overwrite.
				child := after.Children[0]
				if calls.Load() != 1 || replayErr != nil || replay != resolved ||
					!errors.Is(staleErr, flowy.ErrConcurrencyConflict) ||
					!errors.Is(conflictErr, flowy.ErrChildRevision) ||
					child.Revision != before.Children[0].Revision+1 ||
					child.State != decision.Result.State ||
					child.OutcomeResolution == nil ||
					child.OutcomeResolution.PriorState != before.Children[0].State ||
					!reflect.DeepEqual(child.Spec, before.Children[0].Spec) ||
					child.ExecutionID != before.Children[0].ExecutionID ||
					len(after.MergedIDs) != 0 ||
					len(after.BudgetReturns) != 0 {
					t.Fatalf(
						"before=%+v after=%+v calls=%d replay=%v stale=%v conflict=%v",
						before,
						after,
						calls.Load(),
						replayErr,
						staleErr,
						conflictErr,
					)
				}
				if resumeErr != nil || result.Status != flowy.RunStatusCompleted || result.State.Value != 4 {
					t.Fatalf("join=%+v err=%v", result, resumeErr)
				}
			})
		}
	}
}
