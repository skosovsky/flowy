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

func TestChildWaitResolutionDoesNotInvokeCodecOrNode(t *testing.T) {
	// Arrange: persist a wait, then construct an operator runner with unusable decoding.
	ctx := context.Background()
	store := testutil.NewMemoryExecutionStore(nil)
	var probes, dispatches atomic.Int32
	first, err := childLaunchRunner(
		t,
		store,
		persistedChildPlan(),
		func(context.Context, flowy.ChildInvocation) (flowy.ChildResult, error) {
			dispatches.Add(1)
			return flowy.ChildResult{State: flowy.ChildWaiting, WaitID: "external"}, nil
		},
	).Start(ctx, "boundary", durableTestState{})
	if first == nil || !errors.Is(err, flowy.ErrChildrenUnresolved) {
		t.Fatalf("wait missing: %v", err)
	}
	builder := flowy.NewGraph[durableTestState, flowy.NoEffect](
		func(_, update durableTestState) durableTestState { return update },
	)
	builder.AddNode("node", func(_ context.Context, state durableTestState) (durableTestState, flowy.Directive, error) {
		probes.Add(100)
		return state, flowy.End(), nil
	}).AllowNoOutgoingRoute("node").SetEntryPoint("node")
	graph, err := builder.Compile()
	if err != nil {
		t.Fatal(err)
	}
	runner, err := flowy.NewDurableRunner(graph, store, durableDescriptor("current"), failingDecode{calls: &probes},
		checkpoint.JSONSerializer[[]flowy.NoEffect]{}, flowy.DurableOptions{Owner: "operator", LeaseTTL: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	// Act.
	token, err := runner.ResolveChildWait(
		ctx,
		first.ResumeToken,
		childWaitDecision(storedChildGroup(t, store, "boundary"), 0),
	)
	// Assert: one raw metadata commit and zero execution/codec calls.
	if err != nil || probes.Load() != 0 || dispatches.Load() != 1 ||
		token.SnapshotRevision != first.ResumeToken.SnapshotRevision+1 {
		t.Fatalf("resolution crossed boundary: %v probes=%d dispatches=%d", err, probes.Load(), dispatches.Load())
	}
}
