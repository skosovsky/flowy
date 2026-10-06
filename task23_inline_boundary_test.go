package flowy_test

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/skosovsky/flowy"
	"github.com/skosovsky/flowy/testutil"
)

func TestTask23AsNodeEffects(t *testing.T) {
	// Arrange.
	b := flowy.NewGraph[int, string](func(_, u int) int { return u })
	b.AddNode("inner", func(_ context.Context, s int) (int, flowy.Directive, error) {
		return s + 1, flowy.Effect(flowy.Effect(flowy.End(), "one"), "two"), nil
	}).SetEntryPoint("inner").AllowNoOutgoingRoute("inner")
	inner, err := b.Compile()
	if err != nil {
		t.Fatal(err)
	}
	outer := flowy.NewGraph[int, string](func(_, u int) int { return u })
	outer.AddNode("inline", inner.AsNode()).AddEdge("inline", flowy.EndNode).SetEntryPoint("inline")
	g, err := outer.Compile()
	if err != nil {
		t.Fatal(err)
	}
	// Act.
	result, err := g.NewRunner(testutil.NewMemoryCheckpointer[int, string]()).Start(context.Background(), "as-node", 0)
	// Assert.
	if err != nil || result.State != 1 || !reflect.DeepEqual(result.Effects, []string{"one", "two"}) {
		t.Fatalf("result=%+v err=%v", result, err)
	}
}

func TestTask23OldInlineSlotRejectedBeforeHandler(t *testing.T) {
	// Arrange.
	calls := 0
	b := flowy.NewGraph[int, string](func(_, u int) int { return u })
	b.AddNode("inner", func(_ context.Context, s int) (int, flowy.Directive, error) { calls++; return s, flowy.End(), nil }).
		SetEntryPoint("inner").
		AllowNoOutgoingRoute("inner")
	inner, err := b.Compile()
	if err != nil {
		t.Fatal(err)
	}
	node := flowy.SubgraphNodeWithSlot(
		inner,
		func(s task23Parent) int { return s.Value },
		func(s task23Parent) (flowy.SubgraphSlot[int, string], bool) { return s.Slot, true },
		func(s task23Parent, slot flowy.SubgraphSlot[int, string]) task23Parent { s.Slot = slot; return s },
		func(s task23Parent, u int) task23Parent { s.Value = u; return s },
	)
	state := task23Parent{Slot: flowy.SubgraphSlot[int, string]{ExecutionPointer: "inner", Effects: []string{"prior"}}}
	// Act.
	result, _, err := node(context.Background(), state)
	// Assert.
	if !errors.Is(err, flowy.ErrInvalidSnapshot) || calls != 0 || !reflect.DeepEqual(result, state) {
		t.Fatalf("calls=%d result=%+v err=%v", calls, result, err)
	}
}

func TestTask23DefaultAdmission(t *testing.T) {
	for _, configured := range []int{0, -1} {
		t.Run(string(rune('A'-configured)), func(t *testing.T) {
			// Arrange.
			calls := 0
			b := flowy.NewGraph[int, flowy.NoEffect](func(_, u int) int { return u })
			b.AddNode("work", func(_ context.Context, s int) (int, flowy.Directive, error) {
				calls++
				return s + 1, flowy.Completed(), nil
			}).AddEdge("work", "work").SetEntryPoint("work")
			g, err := b.Compile(flowy.WithMaxSteps(configured))
			if err != nil {
				t.Fatal(err)
			}
			// Act.
			result, err := g.NewRunner(testutil.NewMemoryCheckpointer[int, flowy.NoEffect]()).
				Start(context.Background(), "default", 0)
			// Assert.
			if !errors.Is(err, flowy.ErrMaxStepsExceeded) || calls != 1000 || result.RunMeta.StepCount != 1000 {
				t.Fatalf("calls=%d result=%+v err=%v", calls, result, err)
			}
		})
	}
}
