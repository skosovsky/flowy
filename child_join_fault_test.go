package flowy_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/skosovsky/flowy"
	"github.com/skosovsky/flowy/checkpoint"
	"github.com/skosovsky/flowy/testutil"
)

func childJoinRunner(
	t *testing.T,
	store flowy.ExecutionStore,
	node flowy.Node[durableTestState, flowy.NoEffect],
) *flowy.DurableRunner[durableTestState, flowy.NoEffect] {
	t.Helper()
	builder := flowy.NewGraph[durableTestState, flowy.NoEffect](
		func(_, update durableTestState) durableTestState { return update },
	)
	builder.AddNode("node", node).AllowNoOutgoingRoute("node").SetEntryPoint("node")
	graph, err := builder.Compile()
	if err != nil {
		t.Fatal(err)
	}
	runner, err := flowy.NewDurableRunner(graph, store, durableDescriptor("current"),
		checkpoint.JSONSerializer[durableTestState]{}, checkpoint.JSONSerializer[[]flowy.NoEffect]{},
		flowy.DurableOptions{Owner: "worker", LeaseTTL: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	return runner
}

func TestChildJoinCommitFaultRetainsOutcomes(t *testing.T) {
	for _, failAt := range []int32{6, 7} {
		t.Run(map[int32]string{6: "join", 7: "terminal"}[failAt], func(t *testing.T) {
			// Arrange: fail either join publication or the following terminal save.
			ctx := context.Background()
			store := &faultExecutionStore{ExecutionStore: testutil.NewMemoryExecutionStore(nil), failAt: failAt}
			var dispatches, merges atomic.Int32
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
							return flowy.ChildResult{State: flowy.ChildCompleted, Payload: []byte("done")}, nil
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
			// Act.
			first, err := runner.Start(ctx, "fault", durableTestState{})
			if err == nil || first == nil {
				t.Fatalf("commit fault missing: %v", err)
			}
			_, resumeErr := runner.Resume(ctx, first.ResumeToken)
			// Assert: only an uncommitted pure merge may be recomputed.
			wantMerges := int32(1)
			if failAt == 6 {
				wantMerges = 2
			}
			if resumeErr != nil || dispatches.Load() != 1 || merges.Load() != wantMerges {
				t.Fatalf("recovery: %v dispatch=%d merge=%d", resumeErr, dispatches.Load(), merges.Load())
			}
		})
	}
}

func TestChildJoinRejectsStaleAndCanceledMerge(t *testing.T) {
	// Arrange.
	ctx := context.Background()
	store := testutil.NewMemoryExecutionStore(nil)
	var merges atomic.Int32
	runner := childJoinRunner(
		t,
		store,
		func(ctx context.Context, state durableTestState) (durableTestState, flowy.Directive, error) {
			group, err := flowy.RunChildren(
				ctx,
				persistedChildPlan(),
				nil,
				func(context.Context, flowy.ChildInvocation) (flowy.ChildResult, error) {
					return flowy.ChildResult{State: flowy.ChildCompleted}, nil
				},
			)
			if err != nil {
				return state, flowy.End(), err
			}
			group.Children[0].Revision--
			_, staleErr := flowy.JoinChildren(ctx, group, func(context.Context, []flowy.ChildRecord) ([]byte, error) {
				merges.Add(100)
				return nil, nil
			})
			if !errors.Is(staleErr, flowy.ErrChildRevision) {
				return state, flowy.End(), errors.New("stale revision accepted")
			}
			group.Children[0].Revision++
			mergeCtx, cancel := context.WithCancel(ctx)
			_, err = flowy.JoinChildren(mergeCtx, group, func(context.Context, []flowy.ChildRecord) ([]byte, error) {
				merges.Add(1)
				cancel()
				return []byte("must not commit"), nil
			})
			return state, flowy.End(), err
		},
	)
	// Act.
	_, err := runner.Start(ctx, "canceled", durableTestState{})
	latest, loadErr := store.LoadExecution(ctx, "canceled")
	// Assert: canceled merge cannot make the parent terminal or hide its children.
	if !errors.Is(err, context.Canceled) || loadErr != nil || latest.Terminal != nil || latest.Revision != 5 ||
		merges.Load() != 1 {
		t.Fatalf("canceled/stale join: %v load=%v revision=%d merges=%d", err, loadErr, latest.Revision, merges.Load())
	}
}
