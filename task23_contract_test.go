package flowy_test

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"math"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/skosovsky/flowy"
	"github.com/skosovsky/flowy/checkpoint"
	"github.com/skosovsky/flowy/testutil"
)

//nolint:gocognit,nestif // exhaustive durable/stream/terminal contract matrix
func TestTask23AdmissionMatrix(t *testing.T) {
	for _, durable := range []bool{false, true} {
		for _, stream := range []bool{false, true} {
			for _, terminal := range []bool{false, true} {
				t.Run(fmt.Sprintf("durable=%t/stream=%t/terminal=%t", durable, stream, terminal), func(t *testing.T) {
					// Arrange.
					var calls, dispatches atomic.Int32
					b := flowy.NewGraph[int, flowy.NoEffect](func(_, u int) int { return u })
					b.AddNode("work", func(_ context.Context, s int) (int, flowy.Directive, error) {
						calls.Add(1)
						if terminal {
							return s + 1, flowy.End(), nil
						}
						return s + 1, flowy.Completed(), nil
					})
					b.AddNode("forbidden", func(ctx context.Context, s int) (int, flowy.Directive, error) {
						calls.Add(1)
						_, activityErr := flowy.CallActivity(
							ctx,
							flowy.ActivityRequest{
								Key:            "forbidden",
								Implementation: "host",
								Input:          []byte("input"),
								Dispatch:       func(context.Context, flowy.ActivityInvocation) ([]byte, error) { dispatches.Add(1); return nil, nil },
							},
						)
						return s, flowy.End(), activityErr
					})
					b.AddEdge("work", "forbidden").AllowNoOutgoingRoute("forbidden").SetEntryPoint("work")
					g, err := b.Compile(flowy.WithMaxSteps(1))
					if err != nil {
						t.Fatal(err)
					}
					var result *flowy.RunResult[int, flowy.NoEffect]
					// Act.
					if durable {
						r, e := flowy.NewDurableRunner(
							g,
							testutil.NewMemoryExecutionStore(nil),
							durableDescriptor("admission"),
							checkpoint.JSONSerializer[int]{},
							checkpoint.JSONSerializer[[]flowy.NoEffect]{},
							flowy.DurableOptions{Owner: "worker", LeaseTTL: time.Minute},
						)
						if e != nil {
							t.Fatal(e)
						}
						if stream {
							h, e := r.Stream(context.Background(), "run", 0)
							if e != nil {
								t.Fatal(e)
							}
							result, err = h.WaitResult()
						} else {
							result, err = r.Start(context.Background(), "run", 0)
						}
					} else {
						r := g.NewRunner(testutil.NewMemoryCheckpointer[int, flowy.NoEffect]())
						if stream {
							h, e := r.Stream(context.Background(), "run", 0)
							if e != nil {
								t.Fatal(e)
							}
							result, err = h.WaitResult()
						} else {
							result, err = r.Start(context.Background(), "run", 0)
						}
					}
					// Assert.
					if calls.Load() != 1 || dispatches.Load() != 0 || result == nil || result.State != 1 ||
						result.RunMeta.StepCount != 1 {
						t.Fatalf("calls=%d result=%+v", calls.Load(), result)
					}
					if terminal {
						if err != nil || result.Status != flowy.RunStatusCompleted {
							t.Fatalf("terminal=%+v err=%v", result, err)
						}
					} else if !errors.Is(err, flowy.ErrMaxStepsExceeded) {
						t.Fatalf("err=%v", err)
					}
				})
			}
		}
	}
}

func TestTask23FailedHandlerConsumesAdmission(t *testing.T) {
	// Arrange.
	sentinel := errors.New("handler failed")
	b := flowy.NewGraph[int, flowy.NoEffect](func(_, u int) int { return u })
	b.AddNode("work", func(_ context.Context, s int) (int, flowy.Directive, error) { return s, flowy.Completed(), sentinel }).
		SetEntryPoint("work").
		AllowNoOutgoingRoute("work")
	g, err := b.Compile(flowy.WithMaxSteps(1))
	if err != nil {
		t.Fatal(err)
	}
	// Act.
	result, err := g.NewRunner(testutil.NewMemoryCheckpointer[int, flowy.NoEffect]()).
		Start(context.Background(), "failed", 0)
	// Assert.
	if !errors.Is(err, sentinel) || result.RunMeta.StepCount != 1 || result.State != 0 {
		t.Fatalf("result=%+v err=%v", result, err)
	}
}

func TestTask23BudgetValidation(t *testing.T) {
	for _, tc := range []struct {
		name         string
		used, amount int
		want         error
	}{
		{"", 0, 1, flowy.ErrBudgetInvalid}, {"bad\xff", 0, 1, flowy.ErrBudgetInvalid}, {"units", 0, -1, flowy.ErrBudgetInvalid}, {"units", math.MaxInt, 1, flowy.ErrBudgetInvalid}, {"units", -1, 0, flowy.ErrBudgetInvalid},
	} {
		t.Run(fmt.Sprintf("%q/%d/%d", tc.name, tc.used, tc.amount), func(t *testing.T) {
			// Arrange.
			ctx := flowy.ContextWithRunMetadata(
				context.Background(),
				flowy.RunMetadataInput{BudgetCounts: map[string]int{tc.name: tc.used}},
			)
			// Act.
			err := flowy.UseBudget(ctx, tc.name, tc.amount)
			// Assert.
			if !errors.Is(err, tc.want) || flowy.BudgetUsed(ctx, tc.name) != tc.used {
				t.Fatalf("err=%v used=%d", err, flowy.BudgetUsed(ctx, tc.name))
			}
		})
	}
	// Arrange.
	ctx := flowy.ContextWithRunMetadata(context.Background(), flowy.RunMetadataInput{})
	// Act / Assert.
	if !errors.Is(flowy.UseBudget(context.Background(), "units", 1), flowy.ErrBudgetContext) {
		t.Fatal("missing context accepted")
	}
	if err := flowy.UseBudget(ctx, "new", 2); err != nil || flowy.BudgetUsed(ctx, "new") != 2 {
		t.Fatalf("new counter err=%v", err)
	}
}

func TestTask23BestEffortWithoutConsumer(t *testing.T) {
	// Arrange.
	completed := make(chan struct{})
	b := flowy.NewGraph[int, flowy.NoEffect](func(_, u int) int { return u })
	b.AddNode("work", func(_ context.Context, s int) (int, flowy.Directive, error) {
		s++
		if s == 100 {
			close(completed)
			return s, flowy.End(), nil
		}
		return s, flowy.Completed(), nil
	}).AddEdge("work", "work").SetEntryPoint("work")
	g, err := b.Compile()
	if err != nil {
		t.Fatal(err)
	}
	// Act.
	h, err := g.NewRunner(testutil.NewMemoryCheckpointer[int, flowy.NoEffect]()).
		Stream(context.Background(), "undrained", 0)
	if err != nil {
		t.Fatal(err)
	}
	// Assert. Wait is deliberately called only after all handlers finish.
	select {
	case <-completed:
	case <-time.After(3 * time.Second):
		h.RequestStop()
		t.Fatal("full buffer blocked execution")
	}
	result, err := h.WaitResult()
	if err != nil || result.State != 100 {
		t.Fatalf("result=%+v err=%v", result, err)
	}
}

type task23Mutable struct {
	Values  map[string]int
	Items   []int
	Pointer *int
}

func cloneTask23Mutable(s task23Mutable) task23Mutable {
	p := *s.Pointer
	return task23Mutable{Values: maps.Clone(s.Values), Items: append([]int(nil), s.Items...), Pointer: &p}
}

func TestTask23MutableEventAndStorageIsolation(t *testing.T) {
	// Arrange.
	p := 0
	initial := task23Mutable{Values: map[string]int{"v": 0}, Items: []int{0}, Pointer: &p}
	effect := map[string]int{"v": 1}
	b := flowy.NewGraph[task23Mutable, map[string]int](func(_, u task23Mutable) task23Mutable { return u })
	b.AddNode("first", func(_ context.Context, s task23Mutable) (task23Mutable, flowy.Directive, error) {
		s.Values["v"] = 1
		s.Items[0] = 1
		*s.Pointer = 1
		return s, flowy.Effect(flowy.Completed(), effect), nil
	})
	b.AddNode("last", func(_ context.Context, s task23Mutable) (task23Mutable, flowy.Directive, error) {
		s.Values["v"] = 2
		s.Items[0] = 2
		*s.Pointer = 2
		effect["v"] = 2
		return s, flowy.End(), nil
	})
	b.AddEdge("first", "last").AllowNoOutgoingRoute("last").SetEntryPoint("first")
	g, err := b.Compile()
	if err != nil {
		t.Fatal(err)
	}
	cp := testutil.NewMemoryCheckpointerWithCloners(
		cloneTask23Mutable,
		flowy.ValueCloner[map[string]int](maps.Clone[map[string]int]),
	)
	// Act.
	h, err := g.NewRunner(cp).
		Stream(context.Background(), "mutable", initial, flowy.WithEventCloners(cloneTask23Mutable, flowy.ValueCloner[map[string]int](maps.Clone[map[string]int])))
	if err != nil {
		t.Fatal(err)
	}
	result, err := h.WaitResult()
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for ev := range h.Events() {
		if ev.HasEffect {
			found = true
			if ev.State.Values["v"] != 1 || ev.State.Items[0] != 1 || *ev.State.Pointer != 1 || ev.Effect["v"] != 1 {
				t.Fatalf("aliased event %+v", ev)
			}
		}
	}
	_, err = cp.Save(
		context.Background(),
		0,
		flowy.Snapshot[task23Mutable, map[string]int]{
			ThreadID:         "snapshot",
			ExecutionPointer: "first",
			State:            result.State,
			Effects:          []map[string]int{effect},
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	result.State.Values["v"] = 9
	effect["v"] = 9
	a, _, err := cp.Load(context.Background(), "snapshot")
	if err != nil {
		t.Fatal(err)
	}
	a.State.Items[0] = 9
	a.Effects[0]["v"] = 8
	history, err := cp.GetHistory(context.Background(), "snapshot", 1)
	if err != nil {
		t.Fatal(err)
	}
	// Assert.
	if !found || history[0].State.Values["v"] != 2 || history[0].State.Items[0] != 2 ||
		history[0].Effects[0]["v"] != 2 {
		t.Fatalf("storage aliases: %+v", history)
	}
}

type task23Parent struct {
	Slot  flowy.SubgraphSlot[int, string]
	Value int
}

func TestTask23InlineEffectsAndReentry(t *testing.T) {
	// Arrange.
	subBuilder := flowy.NewGraph[int, string](func(_, u int) int { return u })
	subBuilder.AddNode("pause", func(_ context.Context, s int) (int, flowy.Directive, error) {
		return s + 1, flowy.Effect(flowy.Suspend("pause", flowy.ResumeAt("end")), "pause"), nil
	})
	subBuilder.AddNode("end", func(_ context.Context, s int) (int, flowy.Directive, error) {
		return s + 1, flowy.Effect(flowy.End(), "end"), nil
	})
	subBuilder.AllowNoOutgoingRoute("pause").AllowNoOutgoingRoute("end").SetEntryPoint("pause")
	sub, err := subBuilder.Compile()
	if err != nil {
		t.Fatal(err)
	}
	parentBuilder := flowy.NewGraph[task23Parent, string](func(_, u task23Parent) task23Parent { return u })
	parentBuilder.AddNode("sub", flowy.SubgraphNodeWithSlot(sub, func(s task23Parent) int { return s.Value }, func(s task23Parent) (flowy.SubgraphSlot[int, string], bool) {
		return s.Slot, s.Slot.ExecutionPointer != ""
	}, func(s task23Parent, slot flowy.SubgraphSlot[int, string]) task23Parent { s.Slot = slot; return s }, func(s task23Parent, u int) task23Parent { s.Value = u; return s })).
		AddConditionalEdge("sub", func(_ context.Context, s task23Parent) (string, error) {
			if s.Value < 4 {
				return "sub", nil
			}
			return flowy.EndNode, nil
		}, "sub", flowy.EndNode).
		SetEntryPoint("sub")
	graph, err := parentBuilder.Compile()
	if err != nil {
		t.Fatal(err)
	}
	cp := testutil.NewMemoryCheckpointer[task23Parent, string]()
	r := graph.NewRunner(cp)
	// Act.
	first, err := r.Start(context.Background(), "inline", task23Parent{})
	if err != nil {
		t.Fatal(err)
	}
	second, err := r.Resume(context.Background(), first.ResumeToken)
	if err != nil {
		t.Fatal(err)
	}
	third, err := r.Resume(context.Background(), second.ResumeToken)
	if err != nil {
		t.Fatal(err)
	}
	// Assert.
	if !reflect.DeepEqual(first.Effects, []string{"pause"}) ||
		!reflect.DeepEqual(second.Effects, []string{"pause", "end", "pause"}) ||
		second.State.Slot.ExecutionPointer != "end" ||
		third.State.Slot.ExecutionPointer != "" ||
		third.State.Value != 4 ||
		!reflect.DeepEqual(third.Effects, []string{"pause", "end", "pause", "end"}) {
		t.Fatalf("first=%+v second=%+v third=%+v", first, second, third)
	}
}

func TestTask23DurableInlineRejectsBeforeDispatch(t *testing.T) {
	// Arrange.
	calls := 0
	b := flowy.NewGraph[int, flowy.NoEffect](func(_, u int) int { return u })
	b.AddNode("work", func(_ context.Context, s int) (int, flowy.Directive, error) { calls++; return s, flowy.End(), nil }).
		SetEntryPoint("work").
		AllowNoOutgoingRoute("work")
	sub, err := b.Compile()
	if err != nil {
		t.Fatal(err)
	}
	parent := flowy.NewGraph[int, flowy.NoEffect](func(_, u int) int { return u })
	parent.AddNode("sub", flowy.StatelessSubgraphNode(sub, func(s int) int { return s }, func(_ int, u int) int { return u })).
		SetEntryPoint("sub").
		AllowNoOutgoingRoute("sub")
	graph, err := parent.Compile()
	if err != nil {
		t.Fatal(err)
	}
	r, err := flowy.NewDurableRunner(
		graph,
		testutil.NewMemoryExecutionStore(nil),
		durableDescriptor("inline"),
		checkpoint.JSONSerializer[int]{},
		checkpoint.JSONSerializer[[]flowy.NoEffect]{},
		flowy.DurableOptions{Owner: "worker", LeaseTTL: time.Minute},
	)
	if err != nil {
		t.Fatal(err)
	}
	// Act.
	_, err = r.Start(context.Background(), "inline", 0)
	// Assert.
	if !errors.Is(err, flowy.ErrExecutionCapability) || calls != 0 {
		t.Fatalf("calls=%d err=%v", calls, err)
	}
}
