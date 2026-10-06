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

type replayPolicySourceStore struct {
	flowy.ExecutionStore

	policy flowy.StepReplayPolicy
}

func (s replayPolicySourceStore) LoadExecution(ctx context.Context, id string) (flowy.ExecutionEnvelope, error) {
	envelope, err := s.ExecutionStore.LoadExecution(ctx, id)
	envelope.Descriptor.ReplayPolicy = s.policy
	if err == nil {
		envelope, err = flowy.SealExecutionEnvelope(envelope)
	}
	return envelope, err
}

func replayPolicyGraph(t *testing.T, calls *atomic.Int32) *flowy.Graph[durableTestState, flowy.NoEffect] {
	t.Helper()
	builder := flowy.NewGraph[durableTestState, flowy.NoEffect](
		func(_, update durableTestState) durableTestState { return update },
	)
	builder.AddNode("node", func(_ context.Context, state durableTestState) (durableTestState, flowy.Directive, error) {
		calls.Add(1)
		state.Value++
		return state, flowy.End(), nil
	}).AllowNoOutgoingRoute("node").SetEntryPoint("node")
	graph, err := builder.Compile()
	if err != nil {
		t.Fatal(err)
	}
	return graph
}

func TestDurableReplayPolicyRequiredAtConstruction(t *testing.T) {
	t.Parallel()
	for _, policy := range []flowy.StepReplayPolicy{{}, {Label: "declared", Mode: "unsupported"}, {Mode: flowy.StepReplaySafe}} {
		// Arrange.
		var calls atomic.Int32
		graph := replayPolicyGraph(t, &calls)
		descriptor := durableDescriptor("current")
		descriptor.ReplayPolicy = policy
		// Act.
		_, err := flowy.NewDurableRunner(
			graph,
			testutil.NewMemoryExecutionStore(nil),
			descriptor,
			checkpoint.JSONSerializer[durableTestState]{},
			checkpoint.JSONSerializer[[]flowy.NoEffect]{},
			flowy.DurableOptions{Owner: "worker", LeaseTTL: time.Minute},
		)
		// Assert.
		if !errors.Is(err, flowy.ErrExecutionIncompatible) || calls.Load() != 0 {
			t.Fatalf("implicit replay policy accepted: %+v %v", policy, err)
		}
	}
}

func TestDurableReplayPolicyMismatchRejectsBeforeCodec(t *testing.T) {
	t.Parallel()
	for name, policy := range map[string]flowy.StepReplayPolicy{
		"missing": {}, "unsupported": {Label: "old", Mode: "unsupported"}, "changed": {Label: "old", Mode: flowy.StepReplaySafe},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			// Arrange: a legacy/corrupt backend supplies a policy incompatible with the worker.
			ctx := context.Background()
			base := testutil.NewMemoryExecutionStore(nil)
			lease, err := base.AcquireExecution(ctx, "run", "seed", time.Minute)
			if err != nil {
				t.Fatal(err)
			}
			source, err := base.CommitExecution(
				ctx,
				0,
				lease,
				flowy.ExecutionEnvelope{
					ExecutionID: "run",
					Descriptor:  durableDescriptor("current"),
					Progress:    flowy.MigrationState{ExecutionPointer: "node", StatePayload: []byte("opaque")},
				},
			)
			if err != nil {
				t.Fatal(err)
			}
			if releaseErr := base.ReleaseExecution(ctx, lease); releaseErr != nil {
				t.Fatal(releaseErr)
			}
			var probes atomic.Int32
			store := replayPolicySourceStore{ExecutionStore: base, policy: policy}
			runner, err := flowy.NewDurableRunner(
				replayPolicyGraph(t, &probes),
				store,
				durableDescriptor("current"),
				failingDecode{calls: &probes},
				checkpoint.JSONSerializer[[]flowy.NoEffect]{},
				flowy.DurableOptions{Owner: "worker", LeaseTTL: time.Minute},
			)
			if err != nil {
				t.Fatal(err)
			}
			token := flowy.ResumeToken{ThreadID: "run", SnapshotRevision: source.Revision}
			// Act: neither invocation form may decode/dispatch an implicit policy upgrade.
			_, resumeErr := runner.Resume(ctx, token)
			_, streamErr := runner.ResumeStream(ctx, token)
			// Assert.
			latest, loadErr := base.LoadExecution(ctx, "run")
			if !errors.Is(resumeErr, flowy.ErrExecutionIncompatible) ||
				!errors.Is(streamErr, flowy.ErrExecutionIncompatible) ||
				probes.Load() != 0 ||
				loadErr != nil ||
				latest.Revision != source.Revision {
				t.Fatalf(
					"unsafe recovery: resume=%v stream=%v probes=%d latest=%+v load=%v",
					resumeErr,
					streamErr,
					probes.Load(),
					latest,
					loadErr,
				)
			}
		})
	}
}

func TestDurableReplayPolicyChangeRequiresMigration(t *testing.T) {
	t.Parallel()
	// Arrange: explicit migration authorizes a policy-label change but does not execute the source.
	ctx := context.Background()
	store := testutil.NewMemoryExecutionStore(nil)
	old := durableDescriptor("current")
	old.ReplayPolicy.Label = "old-safe-contract"
	lease, err := store.AcquireExecution(ctx, "run", "seed", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	source, err := store.CommitExecution(
		ctx,
		0,
		lease,
		flowy.ExecutionEnvelope{
			ExecutionID:    "run",
			Descriptor:     old,
			Progress:       flowy.MigrationState{ExecutionPointer: "node", StatePayload: []byte(`{"Value":41}`)},
			EffectsPayload: []byte(`[]`),
			Activation:     1,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if releaseErr := store.ReleaseExecution(ctx, lease); releaseErr != nil {
		t.Fatal(releaseErr)
	}
	var calls atomic.Int32
	target := durableDescriptor("current")
	migration := flowy.ExecutionMigration{
		ID:        "replay-policy-change",
		Source:    old,
		Target:    target,
		Transform: func(state flowy.MigrationState) (flowy.MigrationState, error) { return state, nil },
	}
	runner, err := flowy.NewDurableRunner(
		replayPolicyGraph(t, &calls),
		store,
		target,
		checkpoint.JSONSerializer[durableTestState]{},
		checkpoint.JSONSerializer[[]flowy.NoEffect]{},
		flowy.DurableOptions{Owner: "worker", LeaseTTL: time.Minute, Migrations: []flowy.ExecutionMigration{migration}},
	)
	if err != nil {
		t.Fatal(err)
	}
	// Act.
	result, err := runner.Resume(ctx, flowy.ResumeToken{ThreadID: "run", SnapshotRevision: source.Revision})
	// Assert: migration is a real new revision, and source policy/history remain intact.
	migrated, migratedErr := store.LoadCheckpoint(ctx, "run", source.Revision+1)
	retained, retainedErr := store.LoadCheckpoint(ctx, "run", source.Revision)
	if err != nil || result.State.Value != 42 || calls.Load() != 1 || migratedErr != nil ||
		migrated.Descriptor.ReplayPolicy != target.ReplayPolicy ||
		migrated.Migration == nil ||
		retainedErr != nil ||
		retained.Descriptor.ReplayPolicy != old.ReplayPolicy {
		t.Fatalf("policy migration failed: %+v %v migrated=%+v retained=%+v", result, err, migrated, retained)
	}
}
