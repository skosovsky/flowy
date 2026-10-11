package flowy_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/skosovsky/flowy"
	"github.com/skosovsky/flowy/checkpoint"
	"github.com/skosovsky/flowy/testutil"
)

func TestDurableConstructorRejectsNilStoreAndCodecs(t *testing.T) {
	t.Parallel()
	// Arrange.
	b := flowy.NewGraph[durableTestState, flowy.NoEffect](
		func(_, update durableTestState) durableTestState { return update },
	)
	b.AddNode("done", func(_ context.Context, state durableTestState) (durableTestState, flowy.Directive, error) {
		return state, flowy.End(), nil
	}).SetEntryPoint("done").AllowNoOutgoingRoute("done")
	graph, err := b.Compile()
	if err != nil {
		t.Fatal(err)
	}
	var nilStore *faultExecutionStore
	var nilStateCodec *checkpoint.JSONSerializer[durableTestState]
	var nilEffectsCodec *checkpoint.JSONSerializer[[]flowy.NoEffect]
	for _, mode := range []string{"store", "typed store", "state", "typed state", "effects", "typed effects"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			var store flowy.ExecutionStore = testutil.NewMemoryExecutionStore(nil)
			var state flowy.StateSerializer[durableTestState] = checkpoint.JSONSerializer[durableTestState]{}
			var effects flowy.StateSerializer[[]flowy.NoEffect] = checkpoint.JSONSerializer[[]flowy.NoEffect]{}
			switch mode {
			case "store":
				store = nil
			case "typed store":
				store = nilStore
			case "state":
				state = nil
			case "typed state":
				state = nilStateCodec
			case "effects":
				effects = nil
			case "typed effects":
				effects = nilEffectsCodec
			}
			// Act.
			runner, constructorErr := flowy.NewDurableRunner(
				graph,
				store,
				durableDescriptor("current"),
				state,
				effects,
				flowy.DurableOptions{Owner: "worker", LeaseTTL: time.Minute},
			)
			// Assert.
			if runner != nil || !errors.Is(constructorErr, flowy.ErrExecutionCapability) {
				t.Fatalf("runner=%v err=%v", runner, constructorErr)
			}
		})
	}
}
