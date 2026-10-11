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

type forkTestState struct {
	Value    int
	Approved bool
}

func seedForkSource(t *testing.T, store flowy.ExecutionStore) flowy.ExecutionEnvelope {
	t.Helper()
	ctx := context.Background()
	lease, err := store.AcquireExecution(ctx, "source", "seed", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	source, err := store.CommitExecution(ctx, 0, lease, flowy.ExecutionEnvelope{
		ExecutionID: "source",
		Descriptor:  durableDescriptor("current"),
		Activation:  1,
		Progress: flowy.ExecutionProgress{
			ExecutionPointer: "write",
			StatePayload:     []byte(`{"Value":5,"Approved":true}`),
		},
		EffectsPayload: []byte("[]"),
		Terminal:       &flowy.ExecutionTerminal{Status: flowy.RunStatusCompleted},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err = store.ReleaseExecution(ctx, lease); err != nil {
		t.Fatal(err)
	}
	return source
}

func forkRunnerForTest(t *testing.T, store flowy.ExecutionStore, policy *flowy.ForkExecutionPolicy,
	nodes, live *atomic.Int32,
) *flowy.DurableRunner[forkTestState, flowy.NoEffect] {
	t.Helper()
	b := flowy.NewGraph[forkTestState, flowy.NoEffect](func(_, update forkTestState) forkTestState { return update })
	b.AddNode("write", func(ctx context.Context, state forkTestState) (forkTestState, flowy.Directive, error) {
		nodes.Add(1)
		_, err := flowy.CallActivity(
			ctx,
			flowy.ActivityRequest{Key: "write", Implementation: "host-write", Input: []byte("write"),
				Dispatch: func(context.Context, flowy.ActivityInvocation) ([]byte, error) {
					live.Add(1)
					return []byte("written"), nil
				}},
		)
		if err != nil {
			return state, flowy.Fail("activity"), err
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

func forkRequestForTest(source flowy.ExecutionEnvelope, target string) flowy.ForkRequest {
	return flowy.ForkRequest{Source: flowy.HistoricalCheckpointReference{ExecutionID: source.ExecutionID,
		Revision: source.Revision, Digest: source.Digest}, TargetID: target,
		Transform: flowy.ForkTransform{Label: "correction", Source: source.Descriptor,
			Transform: func(state flowy.ExecutionProgress) (flowy.ExecutionProgress, error) { return state, nil }}}
}

func TestForkDefaultFakeRequiresPolicyAndSeparatesNewIdentities(t *testing.T) {
	t.Parallel()
	// Arrange.
	ctx := context.Background()
	store := testutil.NewMemoryExecutionStore(nil)
	source := seedForkSource(t, store)
	var nodes, live atomic.Int32
	readonly := forkRunnerForTest(t, store, nil, &nodes, &live)
	request := forkRequestForTest(source, "first")
	// Act: creation is a boundary, not execution; missing policy refuses recovery.
	token, err := readonly.Fork(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	_, denied := readonly.Resume(ctx, token)
	if !errors.Is(denied, flowy.ErrForkPolicy) || nodes.Load() != 0 || live.Load() != 0 {
		t.Fatalf("default fork executed live: %v", denied)
	}
	var identities []string
	policy := &flowy.ForkExecutionPolicy{Label: "fake", Mode: flowy.ForkFake,
		FakeActivity: func(_ context.Context, invocation flowy.ActivityInvocation) ([]byte, error) {
			identities = append(identities, invocation.Identity)
			return []byte("simulated"), nil
		}}
	runner := forkRunnerForTest(t, store, policy, &nodes, &live)
	first, firstErr := runner.Resume(ctx, token)
	request.TargetID = "second"
	secondToken, secondErr := runner.Fork(ctx, request)
	if secondErr != nil {
		t.Fatal(secondErr)
	}
	second, resumeErr := runner.Resume(ctx, secondToken)
	// Assert: fresh identities, fake outcomes and immutable source.
	if firstErr != nil || resumeErr != nil || first.State.Value != 6 || second.State.Value != 6 ||
		live.Load() != 0 || nodes.Load() != 2 || len(identities) != 2 || identities[0] == identities[1] {
		t.Fatalf(
			"fork inherited source/live action: first=%+v/%v second=%+v/%v identities=%v",
			first,
			firstErr,
			second,
			resumeErr,
			identities,
		)
	}
	assertForkSourceAndLineage(ctx, t, store, source, "first")
}

func assertForkSourceAndLineage(ctx context.Context, t *testing.T, store *testutil.MemoryExecutionStore,
	source flowy.ExecutionEnvelope, target string,
) {
	t.Helper()
	retained, err := store.LoadExecution(ctx, source.ExecutionID)
	if err != nil || retained.Digest != source.Digest {
		t.Fatalf("fork changed source: %+v err=%v", retained, err)
	}
	head, err := store.LoadExecution(ctx, target)
	if err != nil || head.Fork == nil || head.Fork.Source.Digest != source.Digest || head.Fork.Mode != flowy.ForkFake {
		t.Fatalf("fork lineage missing: %+v err=%v", head, err)
	}
	var journal map[string]flowy.ActivityRecord
	if err = json.Unmarshal(head.JournalPayload, &journal); err != nil {
		t.Fatal(err)
	}
	for _, activity := range journal {
		if activity.Origin != flowy.ActivitySimulated || activity.ExecutionID != target {
			t.Fatalf("fake outcome fabricated live provenance: %+v", activity)
		}
	}
}

func TestForkLiveRequiresProjectionAndRechecksAuthorization(t *testing.T) {
	t.Parallel()
	// Arrange.
	ctx := context.Background()
	store := testutil.NewMemoryExecutionStore(nil)
	source := seedForkSource(t, store)
	var nodes, live, transforms, gates atomic.Int32
	var permitted atomic.Bool
	permitted.Store(true)
	policy := &flowy.ForkExecutionPolicy{Label: "host-authorized", Mode: flowy.ForkLive,
		Authorize: func(context.Context, flowy.ForkLineage) error {
			gates.Add(1)
			if !permitted.Load() {
				return errors.New("revoked authorization")
			}
			return nil
		}}
	runner := forkRunnerForTest(t, store, policy, &nodes, &live)
	request := forkRequestForTest(source, "live-target")
	request.Mode, request.PolicyLabel = flowy.ForkLive, policy.Label
	request.Transform.Transform = func(state flowy.ExecutionProgress) (flowy.ExecutionProgress, error) {
		transforms.Add(1)
		return state, nil
	}
	// Act: no projection means refusal before transform or node work.
	_, missingProjection := runner.Fork(ctx, request)
	if !errors.Is(missingProjection, flowy.ErrForkPolicy) || transforms.Load() != 0 || gates.Load() != 0 {
		t.Fatalf("missing projection accepted: %v", missingProjection)
	}
	request.Projection = &flowy.ForkProjection{
		Label: "remove-approval",
		Project: func(state flowy.ExecutionProgress) (flowy.ExecutionProgress, error) {
			state.StatePayload = []byte(`{"Value":5,"Approved":false}`)
			return state, nil
		},
	}
	token, err := runner.Fork(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	permitted.Store(false)
	_, denied := runner.Resume(ctx, token)
	// Assert: persisted live policy is not stale permission to run.
	if !errors.Is(denied, flowy.ErrForkPolicy) || nodes.Load() != 0 || live.Load() != 0 {
		t.Fatalf("revoked fork ran: %v", denied)
	}
	permitted.Store(true)
	result, resumeErr := runner.Resume(ctx, token)
	if resumeErr != nil || result.State.Approved || result.State.Value != 6 || live.Load() != 1 || nodes.Load() != 1 {
		t.Fatalf("live projection/authorization lost: %+v err=%v live=%d", result, resumeErr, live.Load())
	}
}
