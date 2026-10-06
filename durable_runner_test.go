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

type durableTestState struct{ Value int }

func durableDescriptor(label string) flowy.ExecutionDescriptor {
	return flowy.ExecutionDescriptor{
		GraphID:           "test",
		GraphRevision:     label,
		StateCodec:        label,
		ExecutionContract: "sync",
		ReplayPolicy:      flowy.StepReplayPolicy{Label: "test-safe-steps", Mode: flowy.StepReplaySafe},
	}
}

func TestDurableRunnerCommitsEveryStepAndTerminal(t *testing.T) {
	// Arrange.
	store := testutil.NewMemoryExecutionStore(nil)
	var calls atomic.Int32
	builder := flowy.NewGraph[durableTestState, flowy.NoEffect](
		func(_, update durableTestState) durableTestState { return update },
	)
	builder.AddNode(
		"first",
		func(_ context.Context, state durableTestState) (durableTestState, flowy.Directive, error) {
			calls.Add(1)
			state.Value++
			return state, flowy.Completed(), nil
		},
	)
	builder.AddNode("last", func(_ context.Context, state durableTestState) (durableTestState, flowy.Directive, error) {
		calls.Add(1)
		state.Value++
		return state, flowy.End(), nil
	})
	builder.AddEdge("first", "last").AllowNoOutgoingRoute("last").SetEntryPoint("first")
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
	result, err := runner.Start(context.Background(), "run", durableTestState{})
	if err != nil {
		t.Fatal(err)
	}
	resumed, err := runner.Resume(context.Background(), result.ResumeToken)
	// Assert.
	if err != nil || resumed.State.Value != 2 || calls.Load() != 2 {
		t.Fatalf("terminal replay dispatched: %+v %v calls=%d", resumed, err, calls.Load())
	}
	step, err := store.LoadCheckpoint(context.Background(), "run", 2)
	if err != nil || step.Progress.ExecutionPointer != "last" || step.Terminal != nil {
		t.Fatalf("step checkpoint missing: %+v %v", step, err)
	}
	if result.ResumeToken.SnapshotRevision != 3 {
		t.Fatalf("expected initial + step + terminal, got %+v", result.ResumeToken)
	}
}

type failingDecode struct{ calls *atomic.Int32 }

func TestDurableInvalidOptionsDoNotMutateExecution(t *testing.T) {
	// Arrange.
	ctx := context.Background()
	store := &faultExecutionStore{ExecutionStore: testutil.NewMemoryExecutionStore(nil)}
	var calls atomic.Int32
	runner := activityTestRunner(t, store, &calls, false)
	option := flowy.WithCheckpointErrorPolicy[durableTestState, flowy.NoEffect]("invalid")
	// Act: reject all invocation forms before acquiring or writing execution state.
	_, startErr := runner.Start(ctx, "run", durableTestState{}, option)
	_, createStreamErr := runner.Stream(ctx, "run", durableTestState{}, option)
	token := flowy.ResumeToken{ThreadID: "run", SnapshotRevision: 1}
	_, resumeErr := runner.Resume(ctx, token, option)
	_, streamErr := runner.ResumeStream(ctx, token, option)
	// Assert.
	if !errors.Is(startErr, flowy.ErrInvalidCheckpointPolicy) ||
		!errors.Is(resumeErr, flowy.ErrInvalidCheckpointPolicy) ||
		!errors.Is(streamErr, flowy.ErrInvalidCheckpointPolicy) {
		t.Fatalf("invalid options accepted: start=%v resume=%v stream=%v", startErr, resumeErr, streamErr)
	}
	if !errors.Is(createStreamErr, flowy.ErrInvalidCheckpointPolicy) {
		t.Fatalf("invalid stream options accepted: %v", createStreamErr)
	}
	_, loadErr := store.LoadExecution(ctx, "run")
	if !errors.Is(loadErr, flowy.ErrThreadNotFound) || store.commits.Load() != 0 || calls.Load() != 0 {
		t.Fatalf("rejection mutated execution: %v commits=%d calls=%d", loadErr, store.commits.Load(), calls.Load())
	}
}

func (c failingDecode) Marshal(state durableTestState) ([]byte, error) {
	return checkpoint.JSONSerializer[durableTestState]{}.Marshal(state)
}
func (c failingDecode) Unmarshal([]byte) (durableTestState, error) {
	c.calls.Add(1)
	return durableTestState{}, errors.New("wrong codec called")
}

func TestDurableResumeChecksCompatibilityBeforeCodecAndNode(t *testing.T) {
	for _, stream := range []bool{false, true} {
		t.Run(map[bool]string{false: "sync", true: "stream"}[stream], func(t *testing.T) {
			assertDurableCompatibilityBeforeDecode(t, stream)
		})
	}
}

func assertDurableCompatibilityBeforeDecode(t *testing.T, stream bool) {
	t.Helper()
	// Arrange.
	ctx := context.Background()
	store := testutil.NewMemoryExecutionStore(nil)
	lease, err := store.AcquireExecution(ctx, "run", "seed", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	source, err := store.CommitExecution(
		ctx,
		0,
		lease,
		flowy.ExecutionEnvelope{
			ExecutionID: "run",
			Descriptor:  durableDescriptor("old"),
			Progress:    flowy.MigrationState{ExecutionPointer: "node", StatePayload: []byte("opaque")},
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if releaseErr := store.ReleaseExecution(ctx, lease); releaseErr != nil {
		t.Fatal(releaseErr)
	}
	var calls atomic.Int32
	builder := flowy.NewGraph[durableTestState, flowy.NoEffect](
		func(_, update durableTestState) durableTestState { return update },
	)
	builder.AddNode("node", func(_ context.Context, state durableTestState) (durableTestState, flowy.Directive, error) {
		calls.Add(100)
		return state, flowy.End(), nil
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
		durableDescriptor("new"),
		failingDecode{calls: &calls},
		checkpoint.JSONSerializer[[]flowy.NoEffect]{},
		flowy.DurableOptions{Owner: "worker", LeaseTTL: time.Minute},
	)
	if err != nil {
		t.Fatal(err)
	}
	// Act.
	token := flowy.ResumeToken{ThreadID: "run", SnapshotRevision: source.Revision}
	if stream {
		_, err = runner.ResumeStream(ctx, token)
	} else {
		_, err = runner.Resume(ctx, token)
	}
	// Assert.
	if !errors.Is(err, flowy.ErrExecutionIncompatible) || calls.Load() != 0 {
		t.Fatalf("incompatible execution decoded/dispatched: %v calls=%d", err, calls.Load())
	}
}
