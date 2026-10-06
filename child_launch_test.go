package flowy_test

import (
	"context"
	"encoding/json"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/skosovsky/flowy"
	"github.com/skosovsky/flowy/checkpoint"
	"github.com/skosovsky/flowy/testutil"
)

func childLaunchRunner(t *testing.T, store flowy.ExecutionStore, plan flowy.ChildGroupPlan,
	dispatch flowy.ChildDispatcher) *flowy.DurableRunner[durableTestState, flowy.NoEffect] {
	t.Helper()
	builder := flowy.NewGraph[durableTestState, flowy.NoEffect](
		func(_, update durableTestState) durableTestState { return update },
	)
	builder.AddNode("node", func(ctx context.Context, state durableTestState) (durableTestState, flowy.Directive, error) {
		_, err := flowy.RunChildren(ctx, plan, nil, dispatch)
		return state, flowy.End(), err
	}).
		AllowNoOutgoingRoute("node").
		SetEntryPoint("node")
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

func TestChildLaunchBoundsConcurrencyAndReplaysCommittedResults(t *testing.T) {
	// Arrange: the first two dispatches cannot finish until the test releases them.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	store := testutil.NewMemoryExecutionStore(nil)
	plan := persistedChildPlan()
	plan.Children = []flowy.ChildSpec{{ID: "e"}, {ID: "c"}, {ID: "a"}, {ID: "d"}, {ID: "b"}}
	started := make(chan string, 5)
	gate := make(chan struct{})
	var active, peak, calls atomic.Int32
	dispatch := func(ctx context.Context, invocation flowy.ChildInvocation) (flowy.ChildResult, error) {
		if _, inherited := flowy.ExecutionLeaseFromContext(ctx); inherited {
			return flowy.ChildResult{}, errors.New("parent lease leaked")
		}
		calls.Add(1)
		current := active.Add(1)
		defer active.Add(-1)
		for previous := peak.Load(); current > previous; previous = peak.Load() {
			if peak.CompareAndSwap(previous, current) {
				break
			}
		}
		started <- invocation.ChildID
		select {
		case <-gate:
		case <-ctx.Done():
			return flowy.ChildResult{}, ctx.Err()
		}
		return flowy.ChildResult{State: flowy.ChildCompleted, Payload: []byte(invocation.ChildID)}, nil
	}
	runner := childLaunchRunner(t, store, plan, dispatch)
	type outcome struct {
		result *flowy.RunResult[durableTestState, flowy.NoEffect]
		err    error
	}
	done := make(chan outcome, 1)
	go func() {
		result, err := runner.Start(ctx, "run", durableTestState{})
		done <- outcome{result: result, err: err}
	}()
	// Act: admission order follows IDs; only two children can be in flight.
	first, second := <-started, <-started
	if first+second != "ab" && first+second != "ba" {
		t.Fatalf("launch order: %s %s", first, second)
	}
	close(gate)
	finished := <-done
	if !errors.Is(finished.err, flowy.ErrChildrenUnresolved) || finished.result == nil {
		t.Fatalf("parent bypassed join: %v", finished.err)
	}
	_, replayErr := runner.Resume(ctx, finished.result.ResumeToken)
	// Assert: no replay dispatch, independently committed sorted outcomes, and no false terminal.
	assertChildLaunchResults(ctx, t, store, replayErr, peak.Load(), calls.Load())
}

func assertChildLaunchResults(
	ctx context.Context,
	t *testing.T,
	store flowy.ExecutionStore,
	replayErr error,
	peak, calls int32,
) {
	t.Helper()
	if !errors.Is(replayErr, flowy.ErrChildrenUnresolved) || peak != 2 || calls != 5 {
		t.Fatalf("bound/replay: %v peak=%d calls=%d", replayErr, peak, calls)
	}
	latest, err := store.LoadExecution(ctx, "run")
	if err != nil {
		t.Fatal(err)
	}
	var groups map[string]flowy.ChildGroupRecord
	if err := json.Unmarshal(latest.ChildrenPayload, &groups); err != nil {
		t.Fatal(err)
	}
	if len(groups) != 1 || latest.Terminal != nil {
		t.Fatal("missing group or false terminal")
	}
	for _, group := range groups {
		for index, child := range group.Children {
			want := string(rune('a' + index))
			if child.Spec.ID != want || child.State != flowy.ChildCompleted || string(child.Result) != want ||
				child.Revision != 3 {
				t.Fatalf("child outcome lost: %+v", child)
			}
		}
	}
}
