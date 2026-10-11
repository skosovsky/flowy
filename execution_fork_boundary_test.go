package flowy_test

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/skosovsky/flowy"
	"github.com/skosovsky/flowy/testutil"
)

func TestForkUnresolvedSourceRejectsBeforeTransformAndTargetLease(t *testing.T) {
	t.Parallel()
	for _, kind := range []string{"prepared activity", "running activity", "unknown activity", "retry activity",
		"planned child", "waiting child", "unknown child", "unjoined completed child", "armed wait"} {
		t.Run(kind, func(t *testing.T) {
			t.Parallel()
			// Arrange: real runtime transitions produce the unresolved source.
			store, source := unresolvedForkFixture(t, kind)
			var nodes, live, transforms atomic.Int32
			runner := forkRunnerForTest(t, store, nil, &nodes, &live)
			request := forkRequestForTest(source, "target")
			request.Transform.Transform = func(state flowy.ExecutionProgress) (flowy.ExecutionProgress, error) {
				transforms.Add(1)
				return state, nil
			}
			// Act.
			token, err := runner.Fork(context.Background(), request)
			_, targetErr := store.LoadExecution(context.Background(), "target")
			after, sourceErr := store.LoadExecution(context.Background(), source.ExecutionID)
			// Assert: no callbacks, target or source mutation; even fencing was not acquired.
			if !errors.Is(err, flowy.ErrForkUnresolved) || token != (flowy.ResumeToken{}) || transforms.Load() != 0 ||
				nodes.Load() != 0 || live.Load() != 0 || !errors.Is(targetErr, flowy.ErrThreadNotFound) || sourceErr != nil || after.Digest != source.Digest {
				t.Fatalf(
					"unresolved source forked/mutated: token=%+v err=%v source=%+v/%v target=%v",
					token,
					err,
					after,
					sourceErr,
					targetErr,
				)
			}
			assertNoForkTargetAcquisition(t, store)
		})
	}
}

func assertNoForkTargetAcquisition(t *testing.T, store *testutil.MemoryExecutionStore) {
	t.Helper()
	lease, err := store.AcquireExecution(context.Background(), "target", "probe", time.Minute)
	if err != nil || lease.Incarnation != 1 {
		t.Fatalf("invalid source acquired target: %+v/%v", lease, err)
	}
	if err = store.ReleaseExecution(context.Background(), lease); err != nil {
		t.Fatal(err)
	}
}

func unresolvedForkFixture(t *testing.T, kind string) (*testutil.MemoryExecutionStore, flowy.ExecutionEnvelope) {
	t.Helper()
	ctx := context.Background()
	store := testutil.NewMemoryExecutionStore(nil)
	switch kind {
	case "armed wait":
		backend := newWaitRegistrationStore()
		backend.faultExecutionStore.ExecutionStore = store
		var nodes atomic.Int32
		if _, err := mustWaitRunner(
			t,
			backend,
			&backend.profile,
			&nodes,
		).Start(ctx, "run", durableTestState{}); err != nil {
			t.Fatal(err)
		}
	case "planned child", "waiting child", "unknown child", "unjoined completed child":
		seedUnresolvedForkChildren(t, store, kind)
	default:
		seedUnresolvedForkActivity(t, store, kind)
	}
	source, err := store.LoadExecution(ctx, "run")
	if err != nil {
		t.Fatal(err)
	}
	return store, source
}

func seedUnresolvedForkActivity(t *testing.T, store *testutil.MemoryExecutionStore, kind string) {
	t.Helper()
	var backend flowy.ExecutionStore = store
	classification := flowy.ActivityRetryable
	if kind == "prepared activity" {
		backend = &faultExecutionStore{ExecutionStore: store, failAt: 3}
	}
	if kind == "running activity" {
		backend = &faultExecutionStore{ExecutionStore: store, failAt: 4}
	}
	if kind == "unknown activity" {
		classification = flowy.ActivityAmbiguous
	}
	var calls atomic.Int32
	policy := flowy.ActivityRetryPolicy{
		Label:       "bounded",
		MaxAttempts: 2,
		Schedule: flowy.ActivityRetrySchedule{
			Kind:         flowy.ActivityRetryFixed,
			InitialDelay: time.Hour,
			MaxDelay:     time.Hour,
		},
		SafeRetryContract: "host-idempotent-write",
	}
	_, err := retryActivityRunner(t, backend, wallFixtureClock{}, policy, classification, &calls).
		Start(context.Background(), "run", durableTestState{})
	if err == nil {
		t.Fatal("unresolved activity fixture completed")
	}
}

type wallFixtureClock struct{}

func (wallFixtureClock) Now() time.Time { return time.Now().UTC() }

func seedUnresolvedForkChildren(t *testing.T, store *testutil.MemoryExecutionStore, kind string) {
	t.Helper()
	plan := persistedChildPlan()
	b := flowy.NewGraph[durableTestState, flowy.NoEffect](
		func(_, update durableTestState) durableTestState { return update },
	)
	b.AddNode("node", func(ctx context.Context, state durableTestState) (durableTestState, flowy.Directive, error) {
		if kind == "planned child" {
			_, err := flowy.PrepareChildren(ctx, plan, nil)
			return state, flowy.End(), err
		}
		_, err := flowy.RunChildren(
			ctx,
			plan,
			nil,
			func(context.Context, flowy.ChildInvocation) (flowy.ChildResult, error) {
				switch kind {
				case "waiting child":
					return flowy.ChildResult{State: flowy.ChildWaiting, WaitID: "child-wait"}, nil
				case "unknown child":
					return flowy.ChildResult{}, errors.New("unconfirmed child")
				default:
					return flowy.ChildResult{State: flowy.ChildCompleted, Payload: []byte("done")}, nil
				}
			},
		)
		return state, flowy.End(), err
	}).AllowNoOutgoingRoute("node").SetEntryPoint("node")
	_, err := compileActivityTestRunner(t, b, store).Start(context.Background(), "run", durableTestState{})
	if err == nil {
		t.Fatal("unjoined child fixture completed")
	}
}

func TestForkTransformProjectionAndCodecFailuresPublishNoTarget(t *testing.T) {
	t.Parallel()
	for _, kind := range []string{"transform error", "projection error", "bad pointer", "bad state", "wrong source descriptor",
		"source digest mismatch", "missing checkpoint", "zero revision"} {
		t.Run(kind, func(t *testing.T) {
			t.Parallel()
			// Arrange.
			ctx := context.Background()
			store := testutil.NewMemoryExecutionStore(nil)
			source := seedForkSource(t, store)
			var nodes, live atomic.Int32
			runner := forkRunnerForTest(t, store, nil, &nodes, &live)
			request := forkRequestForTest(source, "target")
			want := invalidForkTransformFixture(&request, kind)
			// Act.
			token, err := runner.Fork(ctx, request)
			_, targetErr := store.LoadExecution(ctx, "target")
			after, sourceErr := store.LoadExecution(ctx, "source")
			// Assert: callbacks may fail, but no boundary was acknowledged or source rewritten.
			if !errors.Is(err, want) || token != (flowy.ResumeToken{}) ||
				!errors.Is(targetErr, flowy.ErrThreadNotFound) ||
				sourceErr != nil ||
				after.Digest != source.Digest ||
				nodes.Load() != 0 ||
				live.Load() != 0 {
				t.Fatalf(
					"failed transform exposed target: token=%+v err=%v source=%+v/%v target=%v",
					token,
					err,
					after,
					sourceErr,
					targetErr,
				)
			}
		})
	}
}

func invalidForkTransformFixture(request *flowy.ForkRequest, kind string) error {
	want := flowy.ErrExecutionIncompatible
	switch kind {
	case "source digest mismatch":
		request.Source.Digest = strings.Repeat("0", 64)
		want = flowy.ErrExecutionSourceDigest
	case "missing checkpoint":
		request.Source.Revision = 99
		want = flowy.ErrExecutionCheckpointUnavailable
	case "zero revision":
		request.Source.Revision = 0
		want = flowy.ErrExecutionLifecycleInvalid
	case "transform error":
		request.Transform.Transform = func(flowy.ExecutionProgress) (flowy.ExecutionProgress, error) {
			return flowy.ExecutionProgress{}, errInjectedCommit
		}
		want = flowy.ErrForkTransform
	case "projection error":
		request.Projection = &flowy.ForkProjection{
			Label: "failed-sanitation",
			Project: func(flowy.ExecutionProgress) (flowy.ExecutionProgress, error) {
				return flowy.ExecutionProgress{}, errInjectedCommit
			},
		}
		want = flowy.ErrForkTransform
	case "wrong source descriptor":
		request.Transform.Source = durableDescriptor("wrong")
	default:
		request.Transform.Transform = func(state flowy.ExecutionProgress) (flowy.ExecutionProgress, error) {
			if kind == "bad pointer" {
				state.ExecutionPointer = "absent"
			} else {
				state.StatePayload = []byte("invalid-json")
			}
			return state, nil
		}
	}
	return want
}
