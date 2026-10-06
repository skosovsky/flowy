//go:build integration

package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/skosovsky/flowy"
	"github.com/skosovsky/flowy/checkpoint"
)

func pgForkDescriptor(label string) flowy.ExecutionDescriptor {
	return flowy.ExecutionDescriptor{GraphID: "fork-fixture", GraphRevision: label,
		StateCodec: "json-state", EffectsCodec: "host-effects-v1", ExecutionContract: "sync",
		ReplayPolicy: flowy.StepReplayPolicy{Label: "pure-with-activities", Mode: flowy.StepReplaySafe}}
}

func seedPersistentForkSource(ctx context.Context, t *testing.T, store *ExecutionStore,
	id string,
) (flowy.ExecutionEnvelope, flowy.ExecutionEnvelope) {
	t.Helper()
	lease, err := store.AcquireExecution(ctx, id, "seed", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	source, err := store.CommitExecution(ctx, 0, lease, flowy.ExecutionEnvelope{
		ExecutionID: id, Descriptor: pgForkDescriptor("source"), Activation: 1,
		Progress:       flowy.MigrationState{ExecutionPointer: "write", StatePayload: []byte(`{"Value":5}`)},
		EffectsPayload: []byte("[{}]"),
	})
	if err != nil {
		t.Fatal(err)
	}
	later := source
	later.Progress.StatePayload = []byte(`{"Value":20}`)
	later.Terminal = &flowy.ExecutionTerminal{Status: flowy.RunStatusCompleted}
	latest, err := store.CommitExecution(ctx, source.Revision, lease, later)
	if err != nil {
		t.Fatal(err)
	}
	if err = store.ReleaseExecution(ctx, lease); err != nil {
		t.Fatal(err)
	}
	return source, latest
}

func pgForkRunner(t *testing.T, store flowy.ExecutionStore, policy *flowy.ForkExecutionPolicy,
	nodes, live *atomic.Int32,
) *flowy.DurableRunner[intState, flowy.NoEffect] {
	t.Helper()
	b := flowy.NewGraph[intState, flowy.NoEffect](func(_, update intState) intState { return update })
	b.AddNode("write", func(ctx context.Context, state intState) (intState, flowy.Directive, error) {
		nodes.Add(1)
		_, err := flowy.CallActivity(
			ctx,
			flowy.ActivityRequest{Key: "write", Implementation: "host-write", Input: []byte("input"),
				Dispatch: func(context.Context, flowy.ActivityInvocation) ([]byte, error) {
					live.Add(1)
					return []byte("live"), nil
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
	runner, err := flowy.NewDurableRunner(graph, store, pgForkDescriptor("target"),
		checkpoint.JSONSerializer[intState]{}, checkpoint.JSONSerializer[[]flowy.NoEffect]{},
		flowy.DurableOptions{Owner: "fork-worker", LeaseTTL: time.Minute, ForkPolicy: policy})
	if err != nil {
		t.Fatal(err)
	}
	return runner
}

func TestForkPersistentHistoricalCreationStreamRecoveryAndFakeProvenance(t *testing.T) {
	// Arrange: exact historical source survives original-pool closure.
	ctx, pool := racePool(t)
	if _, err := pool.Exec(ctx, ExecutionSchemaSQL()); err != nil {
		t.Fatal(err)
	}
	store := mustExecutionStore(t, pool)
	base := testThreadID(t)
	source, latest := seedPersistentForkSource(ctx, t, store, base+"source")
	pool.Close()
	forkCtx, forkPool := racePool(t)
	forkStore := mustExecutionStore(t, forkPool)
	var nodes, live, fake atomic.Int32
	readonly := pgForkRunner(t, forkStore, nil, &nodes, &live)
	request := flowy.ForkRequest{Source: flowy.HistoricalCheckpointReference{ExecutionID: source.ExecutionID,
		Revision: source.Revision, Digest: source.Digest}, TargetID: base + "target",
		Transform: flowy.ForkTransform{Label: "historical-correction", Source: source.Descriptor,
			Transform: func(state flowy.MigrationState) (flowy.MigrationState, error) {
				state.StatePayload = []byte(`{"Value":8}`)
				return state, nil
			}}}
	token, err := readonly.Fork(forkCtx, request)
	if err != nil {
		t.Fatal(err)
	}
	if token.SnapshotRevision != 1 || nodes.Load() != 0 || live.Load() != 0 {
		t.Fatalf("creation executed work: %+v", token)
	}
	forkPool.Close()
	recoveryCtx, recoveryPool := racePool(t)
	recovered := mustExecutionStore(t, recoveryPool)
	_, denied := pgForkRunner(t, recovered, nil, &nodes, &live).Resume(recoveryCtx, token)
	if !errors.Is(denied, flowy.ErrForkPolicy) || nodes.Load() != 0 {
		t.Fatalf("restart forgot fake policy: %v", denied)
	}
	policy := &flowy.ForkExecutionPolicy{Label: "fake", Mode: flowy.ForkFake,
		FakeActivity: func(context.Context, flowy.ActivityInvocation) ([]byte, error) {
			fake.Add(1)
			return []byte("simulated"), nil
		}}
	runner := pgForkRunner(t, recovered, policy, &nodes, &live)
	// Act: compatible recovery streams only the new execution.
	handle, err := runner.ResumeStream(recoveryCtx, token)
	if err != nil {
		t.Fatal(err)
	}
	completed := 0
	for event := range handle.Events() {
		if event.Type == flowy.EventCompleted {
			completed++
		}
	}
	result, resultErr := handle.WaitResult()
	if resultErr != nil || result == nil {
		t.Fatalf("fork stream failed: %+v/%v", result, resultErr)
	}
	cached, cachedErr := runner.Resume(recoveryCtx, result.ResumeToken)
	// Assert: corrected historical state, no inherited effects, one fake intent and continuation.
	if cachedErr != nil || result.State.Value != 9 || cached.State.Value != 9 || len(result.Effects) != 0 ||
		completed != 1 || nodes.Load() != 1 || fake.Load() != 1 ||
		live.Load() != 0 {
		t.Fatalf(
			"persistent fake fork replayed live/source: result=%+v/%v cached=%+v/%v callbacks=%d/%d/%d",
			result,
			resultErr,
			cached,
			cachedErr,
			nodes.Load(),
			fake.Load(),
			live.Load(),
		)
	}
	assertPersistentForkSourceAndOutcome(recoveryCtx, t, recovered, source, latest, request.TargetID)
}

func assertPersistentForkSourceAndOutcome(ctx context.Context, t *testing.T, store *ExecutionStore,
	source, latest flowy.ExecutionEnvelope, targetID string,
) {
	t.Helper()
	historical, historyErr := store.LoadCheckpoint(ctx, source.ExecutionID, source.Revision)
	head, headErr := store.LoadExecution(ctx, source.ExecutionID)
	if historyErr != nil || headErr != nil || historical.Digest != source.Digest || head.Digest != latest.Digest {
		t.Fatalf(
			"fork mutated persistent source: historical=%+v/%v latest=%+v/%v",
			historical,
			historyErr,
			head,
			headErr,
		)
	}
	target, err := store.LoadExecution(ctx, targetID)
	if err != nil || target.Fork == nil || target.Fork.Source.Digest != source.Digest ||
		target.Fork.Source.Revision != source.Revision ||
		target.Fork.Mode != flowy.ForkFake ||
		target.Fork.TargetID != targetID ||
		target.Terminal == nil {
		t.Fatalf("persistent fork lineage missing: %+v/%v", target, err)
	}
	var journal map[string]flowy.ActivityRecord
	if err = json.Unmarshal(target.JournalPayload, &journal); err != nil {
		t.Fatal(err)
	}
	if len(journal) != 1 {
		t.Fatalf("fake fork journal missing: %+v", journal)
	}
	for _, record := range journal {
		if record.ExecutionID != targetID || record.Origin != flowy.ActivitySimulated ||
			record.State != flowy.ActivityCompleted {
			t.Fatalf("fake persistent result claimed source/live: %+v", record)
		}
	}
}
