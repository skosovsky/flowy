package flowy_test

import (
	"context"
	"encoding/json"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/skosovsky/flowy"
	"github.com/skosovsky/flowy/testutil"
)

func completedForkSourceWithHandles(t *testing.T, store *testutil.MemoryExecutionStore,
	actualCalls *atomic.Int32,
) (flowy.ExecutionEnvelope, string) {
	t.Helper()
	ctx := context.Background()
	// Actual external outcome is committed; the following step commit is lost.
	fault := &faultExecutionStore{ExecutionStore: store, failAt: 5}
	_, err := activityTestRunner(t, fault, actualCalls, false).Start(ctx, "run", durableTestState{})
	if !errors.Is(err, errInjectedCommit) || actualCalls.Load() != 1 {
		t.Fatalf("completed source fixture failed: %v", err)
	}
	source, err := store.LoadExecution(ctx, "run")
	if err != nil {
		t.Fatal(err)
	}
	var journal map[string]flowy.ActivityRecord
	if err = json.Unmarshal(source.JournalPayload, &journal); err != nil {
		t.Fatal(err)
	}
	identity := ""
	for key, record := range journal {
		if record.State != flowy.ActivityCompleted || record.Origin != flowy.ActivityLive {
			t.Fatalf("source did not complete live activity: %+v", record)
		}
		identity = key
	}
	if len(journal) != 1 {
		t.Fatalf("source journal=%+v", journal)
	}
	lease, err := store.AcquireExecution(ctx, "run", "metadata-seed", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	source.Progress.ChildCursors = map[string]flowy.ExecutionPointer{"nested": "child-node"}
	source.Progress.JournalReferences = map[string]string{"write": identity}
	source.RunMeta.RetryCounts = map[string]int{"write": 2}
	source.RunMeta.BudgetCounts = map[string]int{"steps": 42}
	source.RunMeta.TelemetryContext = map[string]string{"trace": "source-only"}
	source.EffectsPayload = []byte("[{}]")
	source, err = store.CommitExecution(ctx, source.Revision, lease, source)
	if err != nil {
		t.Fatal(err)
	}
	if err = store.ReleaseExecution(ctx, lease); err != nil {
		t.Fatal(err)
	}
	return source, identity
}

func TestForkCompletedSourceOutcomesAndHandlesNeverTransfer(t *testing.T) {
	t.Parallel()
	// Arrange: genuine completed source action plus nonzero operational metadata.
	ctx := context.Background()
	store := testutil.NewMemoryExecutionStore(nil)
	var original, nodes, live, fake atomic.Int32
	source, sourceIdentity := completedForkSourceWithHandles(t, store, &original)
	identities := make(chan string, 2)
	policy := &flowy.ForkExecutionPolicy{Label: "fake", Mode: flowy.ForkFake,
		FakeActivity: func(_ context.Context, invocation flowy.ActivityInvocation) ([]byte, error) {
			fake.Add(1)
			identities <- invocation.Identity
			return []byte("new simulated outcome"), nil
		}}
	runner := forkRunnerForTest(t, store, policy, &nodes, &live)
	// Act: fork two new targets from exactly the same completed source revision.
	for _, target := range []string{"first", "second"} {
		request := forkRequestForTest(source, target)
		token, err := runner.Fork(ctx, request)
		if err != nil {
			t.Fatal(err)
		}
		boundary, err := store.LoadExecution(ctx, target)
		if err != nil {
			t.Fatal(err)
		}
		assertFreshForkOperationalBoundary(t, boundary)
		result, resumeErr := runner.Resume(ctx, token)
		if resumeErr != nil || result.State.Value != 1 || len(result.Effects) != 0 {
			t.Fatalf("new fork hit old outcome: %+v/%v", result, resumeErr)
		}
	}
	// Assert: no source hit, no old worker replay, different new operation identities.
	first, second := <-identities, <-identities
	retained, err := store.LoadExecution(ctx, "run")
	if err != nil || retained.Digest != source.Digest || original.Load() != 1 || live.Load() != 0 || fake.Load() != 2 ||
		nodes.Load() != 2 || first == second || first == sourceIdentity || second == sourceIdentity {
		t.Fatalf(
			"fork transferred source work/identity: source=%+v/%v identities=%s/%s/%s calls=%d/%d/%d",
			retained,
			err,
			sourceIdentity,
			first,
			second,
			original.Load(),
			live.Load(),
			fake.Load(),
		)
	}
}

func assertFreshForkOperationalBoundary(t *testing.T, envelope flowy.ExecutionEnvelope) {
	t.Helper()
	if envelope.Revision != 1 || envelope.Activation != 1 || envelope.Terminal != nil ||
		len(envelope.JournalPayload) != 0 || len(envelope.ChildrenPayload) != 0 || len(envelope.WaitsPayload) != 0 ||
		len(envelope.Progress.ChildCursors) != 0 || len(envelope.Progress.JournalReferences) != 0 ||
		len(
			envelope.RunMeta.RetryCounts,
		) != 0 || len(envelope.RunMeta.BudgetCounts) != 0 || len(envelope.RunMeta.TelemetryContext) != 0 {
		t.Fatalf("fork inherited operational handles/counters: %+v", envelope)
	}
}
