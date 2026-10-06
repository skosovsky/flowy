package patterns

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/skosovsky/flowy"
)

func TestPatternConfigurationRejectedBeforeCallbacks(t *testing.T) {
	t.Parallel()
	var calls atomic.Int32
	node := func(context.Context, int) (int, flowy.Directive, error) {
		calls.Add(1)
		return 0, flowy.Completed(), errors.New("configuration callback must not execute")
	}
	predicate := func(int) bool { calls.Add(1); return false }
	accessor := func(int) string { calls.Add(1); return "route" }
	// Arrange: every missing callback and invalid retry bound must reject construction.
	constructors := []func() (*flowy.GraphBuilder[int, flowy.NoEffect], error){
		func() (*flowy.GraphBuilder[int, flowy.NoEffect], error) {
			return BuildReAct[int, flowy.NoEffect](nil, node, predicate, 1)
		},
		func() (*flowy.GraphBuilder[int, flowy.NoEffect], error) {
			return BuildReAct[int, flowy.NoEffect](node, nil, predicate, 1)
		},
		func() (*flowy.GraphBuilder[int, flowy.NoEffect], error) {
			return BuildReAct[int, flowy.NoEffect](node, node, nil, 1)
		},
		func() (*flowy.GraphBuilder[int, flowy.NoEffect], error) {
			return BuildReAct[int, flowy.NoEffect](node, node, predicate, 0)
		},
		func() (*flowy.GraphBuilder[int, flowy.NoEffect], error) {
			return BuildDispatchGraph[int, flowy.NoEffect](nil, nil, accessor, nil)
		},
		func() (*flowy.GraphBuilder[int, flowy.NoEffect], error) {
			return BuildDispatchGraph[int, flowy.NoEffect](node, nil, nil, nil)
		},
		func() (*flowy.GraphBuilder[int, flowy.NoEffect], error) {
			return BuildDispatchGraph[int, flowy.NoEffect](
				node,
				map[string]flowy.Node[int, flowy.NoEffect]{"worker": nil},
				accessor,
				nil,
			)
		},
		func() (*flowy.GraphBuilder[int, flowy.NoEffect], error) {
			return BuildDispatchGraph[int, flowy.NoEffect](
				node,
				map[string]flowy.Node[int, flowy.NoEffect]{"dispatch": node},
				accessor,
				nil,
			)
		},
		func() (*flowy.GraphBuilder[int, flowy.NoEffect], error) {
			return BuildEvaluatorOptimizer[int, flowy.NoEffect](nil, node, predicate, 1)
		},
		func() (*flowy.GraphBuilder[int, flowy.NoEffect], error) {
			return BuildEvaluatorOptimizer[int, flowy.NoEffect](node, nil, predicate, 1)
		},
		func() (*flowy.GraphBuilder[int, flowy.NoEffect], error) {
			return BuildEvaluatorOptimizer[int, flowy.NoEffect](node, node, nil, 1)
		},
		func() (*flowy.GraphBuilder[int, flowy.NoEffect], error) {
			return BuildEvaluatorOptimizer[int, flowy.NoEffect](node, node, predicate, -1)
		},
	}
	for index, construct := range constructors {
		// Act.
		builder, err := construct()
		// Assert.
		if builder != nil || !errors.Is(err, ErrConfiguration) || calls.Load() != 0 {
			t.Fatalf("case=%d builder=%v error=%v callbacks=%d", index, builder, err, calls.Load())
		}
	}
}

func TestDispatchOwnsConstructionRoutesAndWorkerDefinitions(t *testing.T) {
	t.Parallel()
	// Arrange: mutating caller-owned definitions after construction cannot affect routing.
	node := func(_ context.Context, state int) (int, flowy.Directive, error) {
		return state + 1, flowy.Completed(), nil
	}
	worker := func(_ context.Context, state int) (int, flowy.Directive, error) {
		return state + 10, flowy.Completed(), nil
	}
	workers := map[string]flowy.Node[int, flowy.NoEffect]{"worker": worker}
	routes := RouteMap{"route": "worker"}
	builder, err := BuildDispatchGraph[int, flowy.NoEffect](node, workers, func(int) string { return "route" }, routes)
	if err != nil {
		t.Fatal(err)
	}
	routes["route"] = "missing"
	workers["worker"] = nil
	graph, err := builder.Compile()
	if err != nil {
		t.Fatal(err)
	}
	// Act.
	result, err := graph.NewRunner(memCP[int, flowy.NoEffect]{}).Start(context.Background(), "dispatch", 0)
	// Assert: replacement reducer preserves both full-state callback updates.
	if err != nil || result == nil || result.State != 11 || result.Status != flowy.RunStatusCompleted {
		t.Fatalf("result=%+v error=%v", result, err)
	}
}

func TestReActEvaluatesCompletedReasonPredicateOnce(t *testing.T) {
	t.Parallel()
	for _, pending := range []bool{false, true} {
		// Arrange: terminal action limits the scenario to one routing decision.
		var predicates, actions atomic.Int32
		reason := func(_ context.Context, state int) (int, flowy.Directive, error) {
			return state + 1, flowy.Completed(), nil
		}
		action := func(_ context.Context, state int) (int, flowy.Directive, error) {
			actions.Add(1)
			return state + 1, flowy.End(), nil
		}
		builder, err := BuildReAct[int, flowy.NoEffect](
			reason,
			action,
			func(int) bool { predicates.Add(1); return pending },
			1,
		)
		if err != nil {
			t.Fatal(err)
		}
		graph, err := builder.Compile()
		if err != nil {
			t.Fatal(err)
		}
		// Act.
		_, err = graph.NewRunner(memCP[int, flowy.NoEffect]{}).Start(context.Background(), "predicate", 0)
		// Assert.
		wantActions := int32(0)
		if pending {
			wantActions = 1
		}
		if err != nil || predicates.Load() != 1 || actions.Load() != wantActions {
			t.Fatalf("pending=%v error=%v predicates=%d actions=%d", pending, err, predicates.Load(), actions.Load())
		}
	}
}

func TestPatternRetryBudgetsCountActionAndCorrectionFallbacks(t *testing.T) {
	t.Parallel()
	for _, name := range []string{"react", "optimizer"} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			// Arrange: a permanently pending/invalid result exhausts two fallback rounds.
			var producers, consumers atomic.Int32
			producer := func(_ context.Context, state int) (int, flowy.Directive, error) {
				producers.Add(1)
				return state + 1, flowy.Completed(), nil
			}
			consumer := func(_ context.Context, state int) (int, flowy.Directive, error) {
				consumers.Add(1)
				return state + 1, flowy.Completed(), nil
			}
			var builder *flowy.GraphBuilder[int, flowy.NoEffect]
			var err error
			if name == "react" {
				builder, err = BuildReAct[int, flowy.NoEffect](producer, consumer, func(int) bool { return true }, 2)
			} else {
				builder, err = BuildEvaluatorOptimizer[int, flowy.NoEffect](
					producer,
					consumer,
					func(int) bool { return false },
					2,
				)
			}
			if err != nil {
				t.Fatal(err)
			}
			graph, err := builder.Compile()
			if err != nil {
				t.Fatal(err)
			}
			// Act.
			result, err := graph.NewRunner(memCP[int, flowy.NoEffect]{}).Start(context.Background(), name, 0)
			// Assert: two fallback rounds allow three action/evaluation calls, six total nodes.
			if !errors.Is(err, flowy.ErrRetryBudgetExceeded) || producers.Load() != 3 || consumers.Load() != 3 ||
				result == nil || result.State != 6 {
				t.Fatalf(
					"result=%+v error=%v producers=%d consumers=%d",
					result,
					err,
					producers.Load(),
					consumers.Load(),
				)
			}
		})
	}
}
