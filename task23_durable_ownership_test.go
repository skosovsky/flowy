package flowy_test

import (
	"context"
	"maps"
	"testing"
	"time"

	"github.com/skosovsky/flowy"
	"github.com/skosovsky/flowy/checkpoint"
	"github.com/skosovsky/flowy/testutil"
)

func TestTask23DurableResumeStreamCloners(t *testing.T) {
	// Arrange.
	p := 0
	initial := task23Mutable{Values: map[string]int{"v": 0}, Items: []int{0}, Pointer: &p}
	b := flowy.NewGraph[task23Mutable, map[string]int](func(_, u task23Mutable) task23Mutable { return u })
	b.AddNode("work", func(_ context.Context, s task23Mutable) (task23Mutable, flowy.Directive, error) {
		n := s.Values["v"] + 1
		s.Values["v"] = n
		s.Items[0] = n
		*s.Pointer = n
		if n == 1 {
			return s, flowy.Effect(flowy.Suspend("pause"), map[string]int{"v": n}), nil
		}
		return s, flowy.Effect(flowy.End(), map[string]int{"v": n}), nil
	}).SetEntryPoint("work").AllowNoOutgoingRoute("work")
	g, err := b.Compile()
	if err != nil {
		t.Fatal(err)
	}
	store := testutil.NewMemoryExecutionStore(nil)
	r, err := flowy.NewDurableRunner(
		g,
		store,
		durableDescriptor("mutable-resume"),
		checkpoint.JSONSerializer[task23Mutable]{},
		checkpoint.JSONSerializer[[]map[string]int]{},
		flowy.DurableOptions{Owner: "worker", LeaseTTL: time.Minute},
	)
	if err != nil {
		t.Fatal(err)
	}
	cloners := flowy.WithEventCloners(cloneTask23Mutable, flowy.ValueCloner[map[string]int](maps.Clone[map[string]int]))
	// Act.
	h, err := r.Stream(context.Background(), "mutable-resume", initial, cloners)
	if err != nil {
		t.Fatal(err)
	}
	first, err := h.WaitResult()
	if err != nil {
		t.Fatal(err)
	}
	for event := range h.Events() {
		event.State.Values["v"] = 9
		event.State.Items[0] = 9
		*event.State.Pointer = 9
		if event.HasEffect {
			event.Effect["v"] = 9
		}
	}
	resumed, err := r.ResumeStream(context.Background(), first.ResumeToken, cloners)
	if err != nil {
		t.Fatal(err)
	}
	result, err := resumed.WaitResult()
	if err != nil {
		t.Fatal(err)
	}
	for event := range resumed.Events() {
		event.State.Values["v"] = 8
		event.State.Items[0] = 8
		*event.State.Pointer = 8
		if event.HasEffect {
			event.Effect["v"] = 8
		}
	}
	envelope, err := store.LoadExecution(context.Background(), "mutable-resume")
	if err != nil {
		t.Fatal(err)
	}
	state, err := (checkpoint.JSONSerializer[task23Mutable]{}).Unmarshal(envelope.Progress.StatePayload)
	if err != nil {
		t.Fatal(err)
	}
	effects, err := (checkpoint.JSONSerializer[[]map[string]int]{}).Unmarshal(envelope.EffectsPayload)
	if err != nil {
		t.Fatal(err)
	}
	// Assert.
	if first.State.Values["v"] != 1 || result.State.Values["v"] != 2 || result.State.Items[0] != 2 ||
		*result.State.Pointer != 2 ||
		result.Effects[0]["v"] != 1 ||
		result.Effects[1]["v"] != 2 ||
		state.Values["v"] != 2 ||
		len(effects) != 2 ||
		effects[0]["v"] != 1 ||
		effects[1]["v"] != 2 {
		t.Fatalf("first=%+v result=%+v state=%+v effects=%+v", first, result, state, effects)
	}
}
