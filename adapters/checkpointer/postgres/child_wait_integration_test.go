//go:build integration

package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/skosovsky/flowy"
	"github.com/skosovsky/flowy/checkpoint"
)

func postgresChildWaitRunner(
	t *testing.T,
	store flowy.ExecutionStore,
	dispatches, merges *atomic.Int32,
) *flowy.DurableRunner[intState, flowy.NoEffect] {
	t.Helper()
	plan := flowy.ChildGroupPlan{Key: "waits", Label: "isolated", MergeLabel: "ordered", BudgetLabel: "fixed",
		CancelLabel: "confirmed", MaxConcurrency: 2, FailurePolicy: flowy.ChildCollectErrors,
		Children: []flowy.ChildSpec{{ID: "a"}, {ID: "b"}, {ID: "c"}}}
	builder := flowy.NewGraph[intState, flowy.NoEffect](func(_, update intState) intState { return update })
	builder.AddNode("node", func(ctx context.Context, state intState) (intState, flowy.Directive, error) {
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
					return append(append(children[0].Result, children[1].Result...), children[2].Result...), nil
				},
			)
		}
		return state, flowy.End(), err
	}).AllowNoOutgoingRoute("node").SetEntryPoint("node")
	graph, err := builder.Compile()
	if err != nil {
		t.Fatal(err)
	}
	runner, err := flowy.NewDurableRunner(graph, store, referenceDescriptor("current"),
		checkpoint.JSONSerializer[intState]{}, checkpoint.JSONSerializer[[]flowy.NoEffect]{},
		flowy.DurableOptions{Owner: "worker", LeaseTTL: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	return runner
}

func postgresStoredChildGroup(
	ctx context.Context,
	t *testing.T,
	store flowy.ExecutionStore,
	id string,
) flowy.ChildGroupRecord {
	t.Helper()
	source, err := store.LoadExecution(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	var groups map[string]flowy.ChildGroupRecord
	if err := json.Unmarshal(source.ChildrenPayload, &groups); err != nil {
		t.Fatal(err)
	}
	for _, group := range groups {
		return group
	}
	t.Fatal("missing child group")
	return flowy.ChildGroupRecord{}
}

func postgresChildWaitDecision(group flowy.ChildGroupRecord, index int) flowy.ChildWaitResolution {
	child := group.Children[index]
	return flowy.ChildWaitResolution{Node: group.Node, Activation: group.Activation, GroupKey: group.Plan.Key,
		ChildID: child.Spec.ID, ExecutionID: child.ExecutionID, ChildRevision: child.Revision,
		WaitID: child.WaitID, DecisionID: "decision-" + child.Spec.ID,
		Result: flowy.ChildResult{State: flowy.ChildCompleted, Payload: []byte(child.Spec.ID)}}
}

func TestChildWaitPersistentRestartPreservesIndependentSibling(t *testing.T) {
	// Arrange: completed a and external waits b/c are persisted before losing the pool.
	ctx, pool := racePool(t)
	if _, err := pool.Exec(ctx, ExecutionSchemaSQL()); err != nil {
		t.Fatal(err)
	}
	id := testThreadID(t)
	var dispatches, merges atomic.Int32
	store := NewExecutionStore(pool)
	first, err := postgresChildWaitRunner(t, store, &dispatches, &merges).Start(ctx, id, intState{})
	if first == nil || !errors.Is(err, flowy.ErrChildrenUnresolved) {
		t.Fatalf("waits missing: %v", err)
	}
	pool.Close()
	resolveCtx, resolvePool := racePool(t)
	resolveStore := NewExecutionStore(resolvePool)
	group := postgresStoredChildGroup(resolveCtx, t, resolveStore, id)
	// Act: resolve b through a new connection, then recover through another one.
	token, err := postgresChildWaitRunner(
		t,
		resolveStore,
		&dispatches,
		&merges,
	).ResolveChildWait(resolveCtx, first.ResumeToken, postgresChildWaitDecision(group, 1))
	if err != nil {
		t.Fatal(err)
	}
	resolvePool.Close()
	resumeCtx, resumePool := racePool(t)
	resumeStore := NewExecutionStore(resumePool)
	runner := postgresChildWaitRunner(t, resumeStore, &dispatches, &merges)
	second, resumeErr := runner.Resume(resumeCtx, token)
	group = postgresStoredChildGroup(resumeCtx, t, resumeStore, id)
	// Assert: exactly one resolved wait; the other and the completed sibling survive.
	if second == nil || !errors.Is(resumeErr, flowy.ErrChildrenUnresolved) || dispatches.Load() != 3 ||
		group.Children[0].State != flowy.ChildCompleted || group.Children[1].State != flowy.ChildCompleted ||
		group.Children[1].WaitResolution == nil || group.Children[2].State != flowy.ChildWaiting || group.Children[2].WaitID != "wait-c" {
		t.Fatalf("sibling lost: resume=%v group=%+v calls=%d", resumeErr, group, dispatches.Load())
	}
	token, err = runner.ResolveChildWait(resumeCtx, second.ResumeToken, postgresChildWaitDecision(group, 2))
	if err != nil {
		t.Fatal(err)
	}
	resumePool.Close()
	finalCtx, finalPool := racePool(t)
	_, err = postgresChildWaitRunner(t, NewExecutionStore(finalPool), &dispatches, &merges).Resume(finalCtx, token)
	if err != nil || dispatches.Load() != 3 || merges.Load() != 1 {
		t.Fatalf("wait/join restart: %v calls=%d merges=%d", err, dispatches.Load(), merges.Load())
	}
}
