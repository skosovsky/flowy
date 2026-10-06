//go:build integration

package postgres

import (
	"context"
	"encoding/json"
	"sync/atomic"
	"testing"
	"time"

	"github.com/skosovsky/flowy"
	"github.com/skosovsky/flowy/checkpoint"
)

func postgresChildOutcomeRunner(t *testing.T, store flowy.ExecutionStore,
	dispatch flowy.ChildDispatcher, merges *atomic.Int32) *flowy.DurableRunner[intState, flowy.NoEffect] {
	t.Helper()
	plan := flowy.ChildGroupPlan{
		Key:            "outcome",
		Label:          "isolated",
		MergeLabel:     "ordered",
		BudgetLabel:    "fixed",
		CancelLabel:    "confirmed",
		MaxConcurrency: 1,
		FailurePolicy:  flowy.ChildCollectErrors,
		Children:       []flowy.ChildSpec{{ID: "child", Input: []byte("input")}},
	}
	b := flowy.NewGraph[intState, flowy.NoEffect](func(_, u intState) intState { return u })
	b.AddNode("node", func(ctx context.Context, s intState) (intState, flowy.Directive, error) {
		group, err := flowy.RunChildren(ctx, plan, nil, dispatch)
		if err != nil {
			return s, flowy.End(), err
		}
		payload, err := flowy.JoinChildren(
			ctx,
			group,
			func(_ context.Context, children []flowy.ChildRecord) ([]byte, error) {
				merges.Add(1)
				return children[0].Result, nil
			},
		)
		if err == nil {
			s.Value = len(payload)
		}
		return s, flowy.End(), err
	}).SetEntryPoint("node").AllowNoOutgoingRoute("node")
	g, err := b.Compile()
	if err != nil {
		t.Fatal(err)
	}
	r, err := flowy.NewDurableRunner(
		g,
		store,
		referenceDescriptor("outcome"),
		checkpoint.JSONSerializer[intState]{},
		checkpoint.JSONSerializer[[]flowy.NoEffect]{},
		flowy.DurableOptions{Owner: "worker", LeaseTTL: time.Minute},
	)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func TestChildOutcomeResolutionPersistentRestart(t *testing.T) {
	for _, failed := range []bool{false, true} {
		t.Run(map[bool]string{false: "completed", true: "failed"}[failed], func(t *testing.T) {
			// Arrange: the remote child finishes; the actual originating pool closes before its outcome commit.
			ctx, pool := racePool(t)
			if _, err := pool.Exec(ctx, ExecutionSchemaSQL()); err != nil {
				t.Fatal(err)
			}
			id := testThreadID(t)
			var calls, merges atomic.Int32
			remote := flowy.ChildResult{State: flowy.ChildCompleted, Payload: []byte("remote receipt")}
			if failed {
				remote.State = flowy.ChildFailed
				remote.Error = "confirmed failure"
			}
			dispatch := func(context.Context, flowy.ChildInvocation) (flowy.ChildResult, error) {
				calls.Add(1)
				return remote, nil
			}
			fault := &activityPoolFaultStore{ExecutionStore: NewExecutionStore(pool), pool: pool, failAt: 5}
			result, startErr := postgresChildOutcomeRunner(t, fault, dispatch, &merges).Start(ctx, id, intState{})
			if startErr == nil || result == nil || calls.Load() != 1 || merges.Load() != 0 {
				t.Fatalf("fault result=%+v err=%v calls=%d merges=%d", result, startErr, calls.Load(), merges.Load())
			}
			restartCtx, restartPool := racePool(t)
			store := NewExecutionStore(restartPool)
			source, err := store.LoadExecution(restartCtx, id)
			if err != nil {
				t.Fatal(err)
			}
			group := postgresChildOutcomeGroup(t, source)
			child := group.Children[0]
			if source.Revision != 4 || source.Terminal != nil || child.State != flowy.ChildRunning ||
				child.Revision != 2 ||
				source.RunMeta.StepCount != 0 {
				t.Fatalf("false committed outcome: %+v group=%+v", source, group)
			}
			runner := postgresChildOutcomeRunner(t, store, dispatch, &merges)
			token := flowy.ResumeToken{ThreadID: id, SnapshotRevision: source.Revision}
			assertActivityCrashLeaseExpiry(restartCtx, t, restartPool, runner, token, source.Revision)
			decision := flowy.ChildOutcomeResolution{
				Node:          group.Node,
				Activation:    group.Activation,
				GroupKey:      group.Plan.Key,
				GroupLabel:    group.Plan.Label,
				ChildID:       child.Spec.ID,
				ExecutionID:   child.ExecutionID,
				ChildRevision: child.Revision,
				DecisionID:    "receipt-decision",
				Reason:        "host lookup",
				Evidence:      "receipt",
				Result:        remote,
			}
			// Act: persist the found result without child dispatch or merge, then discard the resolver pool.
			resolved, err := runner.ResolveChildOutcome(restartCtx, token, decision)
			if err != nil {
				t.Fatal(err)
			}
			if calls.Load() != 1 || merges.Load() != 0 {
				t.Fatal("resolution ran host work")
			}
			restartPool.Close()
			joinCtx, joinPool := racePool(t)
			joined, joinErr := postgresChildOutcomeRunner(
				t,
				NewExecutionStore(joinPool),
				dispatch,
				&merges,
			).Resume(joinCtx, resolved)
			latest, loadErr := NewExecutionStore(joinPool).LoadExecution(joinCtx, id)
			// Assert: durable decision/join survive independent connections; one remote dispatch total.
			if joinErr != nil || loadErr != nil || joined.Status != flowy.RunStatusCompleted ||
				joined.State.Value != len(remote.Payload) ||
				calls.Load() != 1 ||
				merges.Load() != 1 ||
				latest.Terminal == nil ||
				latest.Terminal.Status != flowy.RunStatusCompleted {
				t.Fatalf(
					"join=%+v err=%v load=%v calls=%d merges=%d",
					joined,
					joinErr,
					loadErr,
					calls.Load(),
					merges.Load(),
				)
			}
			settled := postgresChildOutcomeGroup(t, latest)
			provenance := settled.Children[0].OutcomeResolution
			if len(settled.MergedIDs) != 1 || settled.Children[0].State != remote.State ||
				settled.Children[0].Revision != 3 ||
				provenance == nil ||
				provenance.PriorState != flowy.ChildRunning ||
				provenance.ChildRevision != 2 ||
				provenance.SourceRevision != source.Revision ||
				provenance.Incarnation <= provenance.PriorIncarnation {
				t.Fatalf("lost provenance: %+v", settled)
			}
		})
	}
}

func postgresChildOutcomeGroup(t *testing.T, envelope flowy.ExecutionEnvelope) flowy.ChildGroupRecord {
	t.Helper()
	var groups map[string]flowy.ChildGroupRecord
	if err := json.Unmarshal(envelope.ChildrenPayload, &groups); err != nil || len(groups) != 1 {
		t.Fatalf("groups=%+v err=%v", groups, err)
	}
	for _, group := range groups {
		return group
	}
	t.Fatal("missing group")
	return flowy.ChildGroupRecord{}
}
