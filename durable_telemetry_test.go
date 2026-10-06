package flowy_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/skosovsky/flowy"
	"github.com/skosovsky/flowy/checkpoint"
	"github.com/skosovsky/flowy/testutil"
)

type durableTraceKey struct{}
type durableTraceBridge struct{ injections *atomic.Int32 }

func (b durableTraceBridge) Capture(ctx context.Context) map[string]string {
	value, _ := ctx.Value(durableTraceKey{}).(string)
	if value == "" {
		return nil
	}
	return map[string]string{"trace": value}
}
func (b durableTraceBridge) Restore(ctx context.Context, carrier map[string]string) context.Context {
	b.injections.Add(1)
	return context.WithValue(ctx, durableTraceKey{}, carrier["trace"])
}

func TestDurableResumeRestoresCarrierBeforeNodeAndTerminalReplay(t *testing.T) {
	// Arrange: an injected neutral bridge, memory persistence, independent worker contexts.
	var injections atomic.Int32
	flowy.SetTelemetryBridge(durableTraceBridge{injections: &injections})
	t.Cleanup(func() { flowy.SetTelemetryBridge(nil) })
	store := testutil.NewMemoryExecutionStore(nil)
	var seen []string
	graph := flowy.NewGraph[durableTestState, flowy.NoEffect](
		func(_, next durableTestState) durableTestState { return next },
	)
	graph.AddNode("gate", func(ctx context.Context, state durableTestState) (durableTestState, flowy.Directive, error) {
		carrier, _ := ctx.Value(durableTraceKey{}).(string)
		seen = append(seen, carrier)
		state.Value++
		if state.Value == 1 {
			return state, flowy.Suspend("host pause"), nil
		}
		return state, flowy.Completed(), nil
	}).AddNode("end", func(ctx context.Context, state durableTestState) (durableTestState, flowy.Directive, error) {
		carrier, _ := ctx.Value(durableTraceKey{}).(string)
		seen = append(seen, carrier)
		return state, flowy.End(), nil
	}).AddEdge("gate", "end").AllowNoOutgoingRoute("end").SetEntryPoint("gate")
	compiled, err := graph.Compile()
	if err != nil {
		t.Fatal(err)
	}
	bind := func(owner string) *flowy.DurableRunner[durableTestState, flowy.NoEffect] {
		runner, bindErr := flowy.NewDurableRunner(
			compiled,
			store,
			durableDescriptor("trace"),
			checkpoint.JSONSerializer[durableTestState]{},
			checkpoint.JSONSerializer[[]flowy.NoEffect]{},
			flowy.DurableOptions{Owner: owner, LeaseTTL: time.Minute},
		)
		if bindErr != nil {
			t.Fatal(bindErr)
		}
		return runner
	}
	sourceCtx := context.WithValue(context.Background(), durableTraceKey{}, "source-trace")
	paused, err := bind("worker-a").Start(sourceCtx, "trace-run", durableTestState{})
	if err != nil {
		t.Fatal(err)
	}
	// Act: worker B supplies a fresh context with no local trace carrier.
	finished, err := bind("worker-b").Resume(context.Background(), paused.ResumeToken)
	if err != nil {
		t.Fatal(err)
	}
	beforeReplay := injections.Load()
	replayed, err := bind("worker-c").Resume(context.Background(), finished.ResumeToken)
	// Assert: carrier survives checkpoint, node continuation and cached terminal without dispatch.
	if err != nil || finished.Status != flowy.RunStatusCompleted || replayed.ResumeToken != finished.ResumeToken ||
		finished.RunMeta.TelemetryContext["trace"] != "source-trace" || len(seen) != 3 || injections.Load() != beforeReplay+1 {
		t.Fatalf(
			"trace recovery: finished=%+v replay=%+v err=%v seen=%v injections=%d",
			finished,
			replayed,
			err,
			seen,
			injections.Load(),
		)
	}
	for _, carrier := range seen {
		if carrier != "source-trace" {
			t.Fatalf("worker lost trace: %v", seen)
		}
	}
}
