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

func fakeChildForkRunner(t *testing.T, store flowy.ExecutionStore, policy *flowy.ForkExecutionPolicy,
	childLive, notifications *atomic.Int32, cancel bool,
) *flowy.DurableRunner[forkTestState, flowy.NoEffect] {
	t.Helper()
	plan := persistedChildPlan()
	b := flowy.NewGraph[forkTestState, flowy.NoEffect](func(_, update forkTestState) forkTestState { return update })
	b.AddNode("write", func(ctx context.Context, state forkTestState) (forkTestState, flowy.Directive, error) {
		group, err := flowy.RunChildren(
			ctx,
			plan,
			nil,
			func(context.Context, flowy.ChildInvocation) (flowy.ChildResult, error) {
				childLive.Add(1)
				return flowy.ChildResult{State: flowy.ChildCompleted, Payload: []byte("real")}, nil
			},
		)
		if err != nil {
			return state, flowy.Fail("children"), err
		}
		if cancel {
			_, err = flowy.CancelChildren(ctx, group, flowy.ChildCancelRequest{ID: "stop", Reason: "test-stop"},
				func(context.Context, flowy.ChildCancelNotice) error { notifications.Add(1); return nil })
			return state, flowy.End(), err
		}
		payload, err := flowy.JoinChildren(
			ctx,
			group,
			func(_ context.Context, children []flowy.ChildRecord) ([]byte, error) {
				return children[0].Result, nil
			},
		)
		if err != nil {
			return state, flowy.Fail("join"), err
		}
		if string(payload) != "fake-child" {
			return state, flowy.Fail("join"), errors.New("unexpected child payload")
		}
		state.Value++
		return state, flowy.End(), nil
	}).AllowNoOutgoingRoute("write").SetEntryPoint("write")
	graph, err := b.Compile()
	if err != nil {
		t.Fatal(err)
	}
	runner, err := flowy.NewDurableRunner(graph, store, durableDescriptor("fork-target"),
		checkpoint.JSONSerializer[forkTestState]{}, checkpoint.JSONSerializer[[]flowy.NoEffect]{},
		flowy.DurableOptions{Owner: "fork-worker", LeaseTTL: time.Minute, ForkPolicy: policy})
	if err != nil {
		t.Fatal(err)
	}
	return runner
}

func TestForkFakeChildrenNeverUseLiveDispatchOrCancellation(t *testing.T) {
	t.Parallel()
	for _, kind := range []string{"completed", "waiting-cancel", "missing-dispatcher"} {
		t.Run(kind, func(t *testing.T) {
			t.Parallel()
			// Arrange: graph retains live callbacks; only fork policy may replace them.
			ctx := context.Background()
			store := testutil.NewMemoryExecutionStore(nil)
			source := seedForkSource(t, store)
			var live, notifications, fake atomic.Int32
			policy := fakeChildForkPolicy(kind, &fake)
			runner := fakeChildForkRunner(t, store, policy, &live, &notifications, kind == "waiting-cancel")
			token, err := runner.Fork(ctx, forkRequestForTest(source, "target"))
			if err != nil {
				t.Fatal(err)
			}
			// Act.
			result, resumeErr := runner.Resume(ctx, token)
			// Assert: even cancellation acknowledgement cannot call real transport.
			if live.Load() != 0 || notifications.Load() != 0 {
				t.Fatalf("fake fork invoked live callbacks: live=%d notify=%d", live.Load(), notifications.Load())
			}
			assertFakeChildForkOutcome(t, kind, result, resumeErr, fake.Load())
		})
	}
}

func fakeChildForkPolicy(kind string, fake *atomic.Int32) *flowy.ForkExecutionPolicy {
	policy := &flowy.ForkExecutionPolicy{Label: "fake", Mode: flowy.ForkFake,
		FakeActivity: func(context.Context, flowy.ActivityInvocation) ([]byte, error) { return nil, nil }}
	if kind != "missing-dispatcher" {
		policy.FakeChild = func(context.Context, flowy.ChildInvocation) (flowy.ChildResult, error) {
			fake.Add(1)
			if kind == "waiting-cancel" {
				return flowy.ChildResult{State: flowy.ChildWaiting, WaitID: "fake-wait"}, nil
			}
			return flowy.ChildResult{State: flowy.ChildCompleted, Payload: []byte("fake-child")}, nil
		}
	}
	return policy
}

func assertFakeChildForkOutcome(t *testing.T, kind string, result *flowy.RunResult[forkTestState, flowy.NoEffect],
	resumeErr error, fake int32,
) {
	t.Helper()
	switch kind {
	case "completed":
		if resumeErr != nil || result.State.Value != 6 || fake != 1 {
			t.Fatalf("fake child join failed: %+v/%v fake=%d", result, resumeErr, fake)
		}
	case "waiting-cancel":
		if !errors.Is(resumeErr, flowy.ErrChildrenUnresolved) || fake != 1 {
			t.Fatalf("fake cancel falsely settled child: %v fake=%d", resumeErr, fake)
		}
	case "missing-dispatcher":
		if !errors.Is(resumeErr, flowy.ErrForkPolicy) || fake != 0 {
			t.Fatalf("missing fake capability fell back: %v", resumeErr)
		}
	}
}
