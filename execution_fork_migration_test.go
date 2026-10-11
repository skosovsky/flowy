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

func migratedForkRunner(t *testing.T, store flowy.ExecutionStore, policy *flowy.ForkExecutionPolicy,
	transforms, nodes *atomic.Int32,
) *flowy.DurableRunner[forkTestState, flowy.NoEffect] {
	t.Helper()
	b := flowy.NewGraph[forkTestState, flowy.NoEffect](func(_, update forkTestState) forkTestState { return update })
	b.AddNode("renamed", func(_ context.Context, state forkTestState) (forkTestState, flowy.Directive, error) {
		nodes.Add(1)
		state.Value++
		return state, flowy.End(), nil
	}).AllowNoOutgoingRoute("renamed").SetEntryPoint("renamed")
	graph, err := b.Compile()
	if err != nil {
		t.Fatal(err)
	}
	migration := flowy.ExecutionMigration{
		ID:     "fork-cursor-state",
		Source: durableDescriptor("fork-target"),
		Target: durableDescriptor(
			"fork-migrated",
		),
		Transform: func(state flowy.ExecutionProgress) (flowy.ExecutionProgress, error) {
			transforms.Add(1)
			state.ExecutionPointer = "renamed"
			state.StatePayload = []byte(`{"Value":40,"Approved":false}`)
			return state, nil
		},
	}
	runner, err := flowy.NewDurableRunner(graph, store, migration.Target,
		checkpoint.JSONSerializer[forkTestState]{}, checkpoint.JSONSerializer[[]flowy.NoEffect]{},
		flowy.DurableOptions{Owner: "migration-worker", LeaseTTL: time.Minute, ForkPolicy: policy,
			Migrations: []flowy.ExecutionMigration{migration}})
	if err != nil {
		t.Fatal(err)
	}
	return runner
}

func TestForkMigrationPreservesImmutableCreationLineageSyncAndStream(t *testing.T) {
	t.Parallel()
	for _, stream := range []bool{false, true} {
		t.Run(map[bool]string{false: "sync", true: "stream"}[stream], func(t *testing.T) {
			t.Parallel()
			// Arrange: fork creation and the original source are both immutable history.
			ctx := context.Background()
			store := testutil.NewMemoryExecutionStore(nil)
			source := seedForkSource(t, store)
			var originalNodes, live, transforms, nodes atomic.Int32
			token, err := forkRunnerForTest(
				t,
				store,
				nil,
				&originalNodes,
				&live,
			).Fork(ctx, forkRequestForTest(source, "target"))
			if err != nil {
				t.Fatal(err)
			}
			creation, err := store.LoadExecution(ctx, "target")
			if err != nil {
				t.Fatal(err)
			}
			policy := &flowy.ForkExecutionPolicy{
				Label:        "fake",
				Mode:         flowy.ForkFake,
				FakeActivity: func(context.Context, flowy.ActivityInvocation) ([]byte, error) { live.Add(1); return nil, nil },
			}
			runner := migratedForkRunner(t, store, policy, &transforms, &nodes)
			// Act.
			result, resumeErr := resumeForkMigration(ctx, t, runner, token, stream)
			// Assert: current descriptor/cursor change, never creation lineage or mode.
			if resumeErr != nil || result.State.Value != 41 || result.State.Approved || transforms.Load() != 1 ||
				nodes.Load() != 1 || live.Load() != 0 || originalNodes.Load() != 0 {
				t.Fatalf("fork migration executed old/live graph: %+v/%v", result, resumeErr)
			}
			assertForkMigrationHistory(ctx, t, store, source, creation)
		})
	}
}

func resumeForkMigration(ctx context.Context, t *testing.T, runner *flowy.DurableRunner[forkTestState, flowy.NoEffect],
	token flowy.ResumeToken, stream bool,
) (*flowy.RunResult[forkTestState, flowy.NoEffect], error) {
	t.Helper()
	if !stream {
		return runner.Resume(ctx, token)
	}
	handle, err := runner.ResumeStream(ctx, token)
	if err != nil {
		return nil, err
	}
	completed := 0
	for event := range handle.Events() {
		if event.Type == flowy.EventCompleted {
			completed++
		}
	}
	if completed != 1 {
		t.Fatalf("fork migration terminal events=%d", completed)
	}
	return handle.WaitResult()
}

func assertForkMigrationHistory(ctx context.Context, t *testing.T, store *testutil.MemoryExecutionStore,
	source, creation flowy.ExecutionEnvelope,
) {
	t.Helper()
	head, headErr := store.LoadExecution(ctx, "target")
	old, oldErr := store.LoadCheckpoint(ctx, "target", creation.Revision)
	retained, retainErr := store.LoadExecution(ctx, "source")
	if headErr != nil || oldErr != nil || retainErr != nil || head.Fork == nil || creation.Fork == nil ||
		*head.Fork != *creation.Fork || old.Digest != creation.Digest || retained.Digest != source.Digest ||
		head.Descriptor != durableDescriptor("fork-migrated") || head.Progress.ExecutionPointer != "renamed" {
		t.Fatalf(
			"fork migration rewrote lineage/history: head=%+v/%v old=%+v/%v source=%+v/%v",
			head,
			headErr,
			old,
			oldErr,
			retained,
			retainErr,
		)
	}
}

func TestForkWrongModeRejectsBeforeMigrationAndNode(t *testing.T) {
	t.Parallel()
	// Arrange: a live binding may not reinterpret a saved fake execution.
	ctx := context.Background()
	store := testutil.NewMemoryExecutionStore(nil)
	source := seedForkSource(t, store)
	var originalNodes, live, transforms, nodes, gates atomic.Int32
	token, err := forkRunnerForTest(
		t,
		store,
		nil,
		&originalNodes,
		&live,
	).Fork(ctx, forkRequestForTest(source, "target"))
	if err != nil {
		t.Fatal(err)
	}
	before, err := store.LoadExecution(ctx, "target")
	if err != nil {
		t.Fatal(err)
	}
	policy := &flowy.ForkExecutionPolicy{Label: "fake", Mode: flowy.ForkLive,
		Authorize: func(context.Context, flowy.ForkLineage) error { gates.Add(1); return nil }}
	// Act.
	_, resumeErr := migratedForkRunner(t, store, policy, &transforms, &nodes).Resume(ctx, token)
	after, loadErr := store.LoadExecution(ctx, "target")
	// Assert: not even pure migration/gate callbacks run under the wrong mode.
	if !errors.Is(resumeErr, flowy.ErrForkPolicy) || transforms.Load() != 0 || nodes.Load() != 0 || gates.Load() != 0 ||
		loadErr != nil || after.Digest != before.Digest {
		t.Fatalf("fake fork upgraded live through migration: %v after=%+v/%v", resumeErr, after, loadErr)
	}
}
