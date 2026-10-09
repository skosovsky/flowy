//go:build integration

package postgres

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/skosovsky/flowy"
	"github.com/skosovsky/flowy/checkpoint"
)

type interruptedState struct {
	Value int
	Items map[string]int
}

func TestIntegrationInterruptedStepPersistentEntryRecovery(t *testing.T) {
	for _, stream := range []bool{false, true} {
		for _, activity := range []bool{false, true} {
			t.Run(fmt.Sprintf("stream=%t/activity=%t", stream, activity), func(t *testing.T) {
				assertInterruptedStepPersistentRecovery(t, stream, activity)
			})
		}
	}
}

func assertInterruptedStepPersistentRecovery(t *testing.T, stream, activity bool) {
	t.Helper()
	// Arrange: both scalar output and aliased maps/effects change before cancellation.
	ctx, pool := racePool(t)
	if _, err := pool.Exec(ctx, ExecutionSchemaSQL()); err != nil {
		t.Fatal(err)
	}
	id := testThreadID(t)
	store := NewExecutionStore(pool)
	var calls, dispatches atomic.Int32
	parent, cancel := context.WithCancel(ctx)
	defer cancel()
	runner := persistentInterruptedRunner(t, store, &calls, &dispatches, activity, cancel)
	// Act: cancel before directive commit, discard pool and runner, resume from a new pool.
	first, startErr := startInterruptedRun(parent, t, runner, id, stream)
	if first == nil || startErr == nil || first.ResumeToken.SnapshotRevision == 0 {
		t.Fatalf("interruption not returned: %+v %v", first, startErr)
	}
	pool.Close()
	restartCtx, restartPool := reopenPool(t, pool)
	restartedStore := NewExecutionStore(restartPool)
	saved, err := restartedStore.LoadExecution(restartCtx, id)
	if err != nil {
		t.Fatal(err)
	}
	assertPersistentInterruptedEntry(t, saved, first)
	restarted := persistentInterruptedRunner(t, restartedStore, &calls, &dispatches, activity, cancel)
	handle, err := restarted.ResumeStream(restartCtx, first.ResumeToken)
	if err != nil {
		t.Fatal(err)
	}
	for range handle.Events() {
	}
	result, resumeErr := handle.WaitResult()
	// Assert: replay original state-dependent input, no second external dispatch or effect.
	wantDispatches := int32(0)
	if activity {
		wantDispatches = 1
	}
	if resumeErr != nil || result == nil || result.Status != flowy.RunStatusCompleted ||
		result.State.Value != 1 || result.State.Items["count"] != 1 || len(result.Effects) != 1 ||
		calls.Load() != 2 || dispatches.Load() != wantDispatches {
		t.Fatalf("persistent interruption lost replay: %+v err=%v calls=%d dispatches=%d",
			result, resumeErr, calls.Load(), dispatches.Load())
	}
	retained, err := restartedStore.LoadCheckpoint(restartCtx, id, saved.Revision)
	if err != nil || retained.Digest != saved.Digest {
		t.Fatalf("interrupted history rewritten: %+v %v", retained, err)
	}
}

func startInterruptedRun(ctx context.Context, t *testing.T,
	runner *flowy.DurableRunner[interruptedState, string], id string, stream bool,
) (*flowy.RunResult[interruptedState, string], error) {
	t.Helper()
	initial := interruptedState{Items: map[string]int{"count": 0}}
	if !stream {
		return runner.Start(ctx, id, initial)
	}
	handle, err := runner.Stream(ctx, id, initial)
	if err != nil {
		t.Fatal(err)
	}
	for range handle.Events() {
	}
	return handle.WaitResult()
}

func persistentInterruptedRunner(t *testing.T, store flowy.ExecutionStore,
	calls, dispatches *atomic.Int32, activity bool, cancel context.CancelFunc,
) *flowy.DurableRunner[interruptedState, string] {
	t.Helper()
	b := flowy.NewGraph[interruptedState, string](func(_, update interruptedState) interruptedState { return update })
	b.AddNode("node", func(ctx context.Context, state interruptedState) (interruptedState, flowy.Directive, error) {
		attempt := calls.Add(1)
		if activity {
			_, err := flowy.CallActivity(ctx, flowy.ActivityRequest{Key: "write", Implementation: "host",
				Input: []byte(fmt.Sprint(state.Value, state.Items["count"])),
				Dispatch: func(context.Context, flowy.ActivityInvocation) ([]byte, error) {
					dispatches.Add(1)
					return []byte("receipt"), nil
				}})
			if err != nil {
				return state, flowy.End(), err
			}
		}
		state.Value++
		state.Items["count"]++
		if attempt == 1 {
			cancel()
		}
		return state, flowy.Effect(flowy.End(), "committed effect"), nil
	}).AllowNoOutgoingRoute("node").SetEntryPoint("node")
	graph, err := b.Compile()
	if err != nil {
		t.Fatal(err)
	}
	runner, err := flowy.NewDurableRunner(graph, store, referenceDescriptor("interruption"),
		checkpoint.JSONSerializer[interruptedState]{}, checkpoint.JSONSerializer[[]string]{},
		flowy.DurableOptions{Owner: "host", LeaseTTL: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	return runner
}

func assertPersistentInterruptedEntry(t *testing.T, envelope flowy.ExecutionEnvelope,
	result *flowy.RunResult[interruptedState, string],
) {
	t.Helper()
	state, err := (checkpoint.JSONSerializer[interruptedState]{}).Unmarshal(envelope.Progress.StatePayload)
	if err != nil {
		t.Fatal(err)
	}
	effects, err := (checkpoint.JSONSerializer[[]string]{}).Unmarshal(envelope.EffectsPayload)
	if err != nil || state.Value != 0 || state.Items["count"] != 0 || len(effects) != 0 ||
		envelope.Progress.ExecutionPointer != "node" || envelope.Activation != 1 || envelope.Terminal != nil ||
		envelope.RunMeta.StepCount != 0 || result.State.Value != 0 || result.State.Items["count"] != 0 ||
		len(result.Effects) != 0 || result.ResumeToken.SnapshotRevision != envelope.Revision {
		t.Fatalf("partial persistent entry: envelope=%+v state=%+v effects=%v result=%+v err=%v",
			envelope, state, effects, result, err)
	}
}
