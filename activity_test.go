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

var errInjectedCommit = errors.New("injected aggregate commit failure")

type faultExecutionStore struct {
	flowy.ExecutionStore

	commits atomic.Int32
	failAt  int32
}

func (s *faultExecutionStore) CommitExecution(
	ctx context.Context,
	revision uint64,
	lease flowy.ExecutionLease,
	envelope flowy.ExecutionEnvelope,
) (flowy.ExecutionEnvelope, error) {
	if s.commits.Add(1) == s.failAt {
		return flowy.ExecutionEnvelope{}, errInjectedCommit
	}
	return s.ExecutionStore.CommitExecution(ctx, revision, lease, envelope)
}

func activityTestRunner(
	t *testing.T,
	store flowy.ExecutionStore,
	calls *atomic.Int32,
	reconcile bool,
) *flowy.DurableRunner[durableTestState, flowy.NoEffect] {
	t.Helper()
	builder := flowy.NewGraph[durableTestState, flowy.NoEffect](
		func(_, update durableTestState) durableTestState { return update },
	)
	builder.AddNode("write", func(ctx context.Context, state durableTestState) (durableTestState, flowy.Directive, error) {
		request := flowy.ActivityRequest{
			Key:            "write",
			Implementation: "stable",
			Input:          []byte("same input"),
			Dispatch: func(context.Context, flowy.ActivityInvocation) ([]byte, error) {
				calls.Add(1)
				return []byte("done"), nil
			},
		}
		if reconcile {
			request.Reconcile = func(context.Context, flowy.ActivityRecord) ([]byte, error) {
				return []byte("confirmed downstream"), nil
			}
		}
		_, err := flowy.CallActivity(ctx, request)
		if err != nil {
			return state, flowy.Fail("activity"), err
		}
		state.Value++
		return state, flowy.End(), nil
	}).
		AllowNoOutgoingRoute("write").
		SetEntryPoint("write")
	graph, err := builder.Compile()
	if err != nil {
		t.Fatal(err)
	}
	runner, err := flowy.NewDurableRunner(
		graph,
		store,
		durableDescriptor("current"),
		checkpoint.JSONSerializer[durableTestState]{},
		checkpoint.JSONSerializer[[]flowy.NoEffect]{},
		flowy.DurableOptions{Owner: "same-worker", LeaseTTL: time.Minute},
	)
	if err != nil {
		t.Fatal(err)
	}
	return runner
}

func TestActivityFaultBoundariesAndRecovery(t *testing.T) {
	for _, test := range []struct {
		name         string
		failAt       int32
		initialCalls int32
		unknown      bool
	}{
		{name: "intent unavailable", failAt: 2, initialCalls: 0},
		{name: "outcome unavailable", failAt: 4, initialCalls: 1, unknown: true},
		{name: "step checkpoint unavailable", failAt: 5, initialCalls: 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			// Arrange.
			ctx := context.Background()
			store := &faultExecutionStore{ExecutionStore: testutil.NewMemoryExecutionStore(nil), failAt: test.failAt}
			var calls atomic.Int32
			runner := activityTestRunner(t, store, &calls, false)
			// Act: crash point after an explicit aggregate transition.
			failed, err := runner.Start(ctx, "run", durableTestState{})
			// Assert: intent failure prevents dispatch; later failures expose a current recovery token.
			if !errors.Is(err, errInjectedCommit) || calls.Load() != test.initialCalls || failed == nil ||
				failed.ResumeToken.SnapshotRevision == 0 {
				t.Fatalf("fault boundary violated: %+v %v calls=%d", failed, err, calls.Load())
			}
			assertActivityJournalFailure(t, test.failAt, err)
			resumed, resumeErr := runner.Resume(ctx, failed.ResumeToken)
			if test.unknown {
				if !errors.Is(resumeErr, flowy.ErrActivityUnknown) || calls.Load() != 1 {
					t.Fatalf("blind retry: %v calls=%d", resumeErr, calls.Load())
				}
				reconciled := activityTestRunner(t, store, &calls, true)
				resumed, resumeErr = reconciled.Resume(ctx, resumed.ResumeToken)
			}
			if resumeErr != nil || resumed.State.Value != 1 || calls.Load() != 1 {
				t.Fatalf("recovery failed: %+v %v calls=%d", resumed, resumeErr, calls.Load())
			}
		})
	}
}

func assertActivityJournalFailure(t *testing.T, failAt int32, err error) {
	t.Helper()
	if failAt < 5 && !errors.Is(err, flowy.ErrActivityJournalUnavailable) {
		t.Fatalf("storage failure lacks journal classification: %v", err)
	}
}

func TestActivitySameKeyConflictsBeforeDispatch(t *testing.T) {
	// Arrange.
	ctx := context.Background()
	store := testutil.NewMemoryExecutionStore(nil)
	var calls atomic.Int32
	builder := flowy.NewGraph[durableTestState, flowy.NoEffect](
		func(_, update durableTestState) durableTestState { return update },
	)
	builder.AddNode("node", func(ctx context.Context, state durableTestState) (durableTestState, flowy.Directive, error) {
		request := flowy.ActivityRequest{
			Key:            "same",
			Implementation: "stable",
			Input:          []byte("one"),
			Dispatch: func(context.Context, flowy.ActivityInvocation) ([]byte, error) {
				calls.Add(1)
				return []byte("result"), nil
			},
		}
		if _, err := flowy.CallActivity(ctx, request); err != nil {
			return state, flowy.Fail("first"), err
		}
		request.Input = []byte("two")
		_, err := flowy.CallActivity(ctx, request)
		return state, flowy.Fail("conflict"), err
	}).
		AllowNoOutgoingRoute("node").
		SetEntryPoint("node")
	graph, err := builder.Compile()
	if err != nil {
		t.Fatal(err)
	}
	runner, err := flowy.NewDurableRunner(
		graph,
		store,
		durableDescriptor("current"),
		checkpoint.JSONSerializer[durableTestState]{},
		checkpoint.JSONSerializer[[]flowy.NoEffect]{},
		flowy.DurableOptions{Owner: "worker", LeaseTTL: time.Minute},
	)
	if err != nil {
		t.Fatal(err)
	}
	// Act.
	_, err = runner.Start(ctx, "run", durableTestState{})
	// Assert.
	if !errors.Is(err, flowy.ErrActivityConflict) || calls.Load() != 1 {
		t.Fatalf("conflicting input dispatched: %v calls=%d", err, calls.Load())
	}
}

func TestActivityUnknownCannotBeIgnoredByNode(t *testing.T) {
	for _, suspend := range []bool{false, true} {
		t.Run(map[bool]string{false: "terminal", true: "suspend"}[suspend], func(t *testing.T) {
			assertActivityUnknownBlocksAdvance(t, suspend)
		})
	}
}

func assertActivityUnknownBlocksAdvance(t *testing.T, suspend bool) {
	t.Helper()
	// Arrange: a remote operation returns an ambiguous transport error.
	ctx := context.Background()
	store := testutil.NewMemoryExecutionStore(nil)
	var calls atomic.Int32
	builder := flowy.NewGraph[durableTestState, flowy.NoEffect](
		func(_, update durableTestState) durableTestState { return update },
	)
	builder.AddNode("node", func(ctx context.Context, state durableTestState) (durableTestState, flowy.Directive, error) {
		_, _ = flowy.CallActivity(ctx, flowy.ActivityRequest{
			Key: "write", Implementation: "stable", Input: []byte("input"),
			Dispatch: func(context.Context, flowy.ActivityInvocation) ([]byte, error) {
				calls.Add(1)
				return nil, errors.New("connection lost after remote write")
			},
		})
		state.Value++
		if suspend {
			return state, flowy.Suspend("wait"), nil
		}
		return state, flowy.End(), nil
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
	// Act: the node swallows the activity error and requests advancement.
	result, err := runner.Start(ctx, "run", durableTestState{})
	// Assert: neither a terminal nor a suspension checkpoint hides the unknown.
	if !errors.Is(err, flowy.ErrActivityUnknown) || result == nil {
		t.Fatalf("unknown ignored: %+v %v", result, err)
	}
	envelope, err := store.LoadExecution(ctx, "run")
	if err != nil {
		t.Fatal(err)
	}
	if envelope.Terminal != nil || envelope.Activation != 1 || envelope.Revision != 4 {
		t.Fatalf("cursor advanced past unknown: %+v", envelope)
	}
	var persisted durableTestState
	persisted, err = (checkpoint.JSONSerializer[durableTestState]{}).Unmarshal(envelope.Progress.StatePayload)
	if err != nil || persisted.Value != 0 {
		t.Fatalf("unresolved state committed: %+v %v", persisted, err)
	}
	_, err = runner.Resume(ctx, result.ResumeToken)
	if !errors.Is(err, flowy.ErrActivityUnknown) || calls.Load() != 1 {
		t.Fatalf("unknown retried: %v calls=%d", err, calls.Load())
	}
}

func TestActivityCycleSeparatesIdentityFromRecovery(t *testing.T) {
	for _, retry := range []bool{false, true} {
		t.Run(map[bool]string{false: "cycle", true: "self retry"}[retry], func(t *testing.T) {
			assertActivityActivationRecovery(t, retry)
		})
	}
}

func assertActivityActivationRecovery(t *testing.T, retry bool) {
	t.Helper()
	// Arrange: commit failure interrupts the first cycle after its outcome.
	ctx := context.Background()
	store := &faultExecutionStore{ExecutionStore: testutil.NewMemoryExecutionStore(nil), failAt: 5}
	var identities []string
	builder := flowy.NewGraph[durableTestState, flowy.NoEffect](
		func(_, update durableTestState) durableTestState { return update },
	)
	builder.AddNode("cycle", func(ctx context.Context, state durableTestState) (durableTestState, flowy.Directive, error) {
		_, callErr := flowy.CallActivity(ctx, flowy.ActivityRequest{
			Key: "write", Implementation: "stable", Input: []byte("same"),
			Dispatch: func(_ context.Context, invocation flowy.ActivityInvocation) ([]byte, error) {
				if invocation.Identity == "" || invocation.Attempt != 1 || string(invocation.Input) != "same" {
					return nil, errors.New("invalid dispatch identity or payload")
				}
				identities = append(identities, invocation.Identity)
				return []byte("done"), nil
			},
		})
		if callErr != nil {
			return state, flowy.Fail("activity"), callErr
		}
		state.Value++
		if state.Value == 1 {
			if retry {
				return state, flowy.Retry(2), nil
			}
			return state, flowy.Completed(), nil
		}
		return state, flowy.End(), nil
	}).
		SetEntryPoint("cycle")
	if retry {
		builder.AddRetryRoute("cycle", "cycle").AllowNoOutgoingRoute("cycle")
	} else {
		builder.AddEdge("cycle", "cycle")
	}
	runner := compileActivityTestRunner(t, builder, store)
	// Act: recover the same activation, then enter a new activation of the node.
	failed, startErr := runner.Start(ctx, "run", durableTestState{})
	if !errors.Is(startErr, errInjectedCommit) || failed == nil {
		t.Fatalf("fault not reached: %+v %v", failed, startErr)
	}
	resumed, resumeErr := runner.Resume(ctx, failed.ResumeToken)
	// Assert: recovery replays the first result, while the cycle creates a fresh operation.
	wanted := 2
	if retry {
		wanted = 1
	}
	if resumeErr != nil || resumed.State.Value != 2 || len(identities) != wanted {
		t.Fatalf("activity identity violated: %+v %v ids=%v", resumed, resumeErr, identities)
	}
	if !retry && identities[0] == identities[1] {
		t.Fatalf("cycle reused identity: %v", identities)
	}
}

func compileActivityTestRunner(
	t *testing.T,
	builder *flowy.GraphBuilder[durableTestState, flowy.NoEffect],
	store flowy.ExecutionStore,
) *flowy.DurableRunner[durableTestState, flowy.NoEffect] {
	t.Helper()
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
