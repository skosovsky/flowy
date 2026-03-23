package flowy

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func idReducer[T any](_, update T) T { return update }

func noopStringNode(_ context.Context, s string) (string, error) { return s, nil }

func TestBuilder_Compile_Simple(t *testing.T) {
	b := NewGraph[string](idReducer[string])
	b.AddNode("a", func(_ context.Context, s string) (string, error) { return s + "_a", nil })
	b.AddNode("b", func(_ context.Context, s string) (string, error) { return s + "_b", nil })
	b.AddEdge("a", "b")
	b.SetEntryPoint("a")
	b.SetFinishPoint("b")

	graph, err := b.Compile()
	require.NoError(t, err)
	require.NotNil(t, graph)
}

func TestBuilder_Compile_ReActCycle(t *testing.T) {
	b := NewGraph[string](idReducer[string])
	b.AddNode("reason", func(_ context.Context, s string) (string, error) { return s + "_r", nil })
	b.AddNode("tools", func(_ context.Context, s string) (string, error) { return s + "_t", nil })
	b.AddNode("finish", func(_ context.Context, s string) (string, error) { return s, nil })
	b.AddConditionalEdge("reason", func(_ context.Context, s string) (string, error) {
		if len(s) > 10 {
			return "finish", nil
		}
		return "tools", nil
	})
	b.AddEdge("tools", "reason")
	b.SetEntryPoint("reason")
	b.SetFinishPoint("finish")

	graph, err := b.Compile()
	require.NoError(t, err)
	require.NotNil(t, graph)
}

func TestBuilder_Compile_NoEntryPoint(t *testing.T) {
	b := NewGraph[string](idReducer[string])
	b.AddNode("a", noopStringNode)
	b.SetFinishPoint("a")

	_, err := b.Compile()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "entry point")
}

func TestBuilder_Compile_NoFinishPoint(t *testing.T) {
	b := NewGraph[string](idReducer[string])
	b.AddNode("a", noopStringNode)
	b.SetEntryPoint("a")

	_, err := b.Compile()
	require.Error(t, err)
}

func TestBuilder_Compile_EdgeToUnknownNode(t *testing.T) {
	b := NewGraph[string](idReducer[string])
	b.AddNode("a", noopStringNode)
	b.AddEdge("a", "missing")
	b.SetEntryPoint("a")
	b.SetFinishPoint("a")

	_, err := b.Compile()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not registered")
}

func TestBuilder_Compile_EdgeAndConditionalConflict(t *testing.T) {
	b := NewGraph[string](idReducer[string])
	b.AddNode("a", noopStringNode)
	b.AddNode("b", noopStringNode)
	b.AddEdge("a", "b")
	b.AddConditionalEdge("a", func(_ context.Context, _ string) (string, error) { return "b", nil })
	b.SetEntryPoint("a")
	b.SetFinishPoint("b")

	_, err := b.Compile()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "both edge and conditional")
}

func TestBuilder_Compile_ConditionalEdge_NilRouter(t *testing.T) {
	b := NewGraph[string](idReducer[string])
	b.AddNode("a", noopStringNode)
	b.AddNode("b", noopStringNode)
	b.AddConditionalEdge("a", nil)
	b.AddEdge("b", "end")
	b.AddNode("end", noopStringNode)
	b.SetEntryPoint("a")
	b.SetFinishPoint("end")

	_, err := b.Compile()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "conditional edge")
	assert.Contains(t, err.Error(), "nil router")
	assert.Contains(t, err.Error(), "a")
}

func TestBuilder_AddFanOut_Compile(t *testing.T) {
	b := NewGraph[string](idReducer[string])
	b.AddNode("search_db", func(_ context.Context, s string) (string, error) { return s + "_db", nil })
	b.AddNode("search_web", func(_ context.Context, s string) (string, error) { return s + "_web", nil })
	b.AddNode("merge", func(_ context.Context, s string) (string, error) { return s, nil })
	b.AddFanOut("classify", []string{"search_db", "search_web"}, "merge")
	b.SetEntryPoint("classify")
	b.SetFinishPoint("merge")

	graph, err := b.Compile()
	require.NoError(t, err)
	require.NotNil(t, graph)
}

func TestBuilder_Compile_EdgeToFanOutSource(t *testing.T) {
	b := NewGraph[string](idReducer[string])
	b.AddNode("a", noopStringNode)
	b.AddNode("db", noopStringNode)
	b.AddNode("web", noopStringNode)
	b.AddNode("merge", noopStringNode)
	b.AddEdge("a", "classify")
	b.AddFanOut("classify", []string{"db", "web"}, "merge")
	b.SetEntryPoint("a")
	b.SetFinishPoint("merge")
	graph, err := b.Compile()
	require.NoError(t, err)
	require.NotNil(t, graph)
	ctx := context.Background()
	out, err := graph.Invoke(ctx, "start")
	require.NoError(t, err)
	assert.Contains(t, out, "start")
}

func TestBuilder_Compile_FanOutEmptyTargets(t *testing.T) {
	b := NewGraph[string](idReducer[string])
	b.AddNode("merge", noopStringNode)
	b.AddFanOut("x", []string{}, "merge")
	b.SetEntryPoint("x")
	b.SetFinishPoint("merge")
	_, err := b.Compile()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "fan-out")
	assert.Contains(t, err.Error(), "no targets")
}

func TestBuilder_Compile_FanOutAndNodeSameName(t *testing.T) {
	b := NewGraph[string](idReducer[string])
	b.AddNode("x", noopStringNode)
	b.AddNode("merge", noopStringNode)
	b.AddFanOut("x", []string{"merge"}, "merge")
	b.SetEntryPoint("x")
	b.SetFinishPoint("merge")
	_, err := b.Compile()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "both node and fan-out")
}

func TestBuilder_Compile_FanOutTargetIsFanOutSource(t *testing.T) {
	b := NewGraph[string](idReducer[string])
	b.AddNode("db", noopStringNode)
	b.AddNode("web", noopStringNode)
	b.AddNode("merge", noopStringNode)
	b.AddFanOut("inner", []string{"db", "web"}, "merge")
	b.AddFanOut("outer", []string{"inner"}, "merge") // "inner" is fan-out source, not a node
	b.SetEntryPoint("outer")
	b.SetFinishPoint("merge")
	_, err := b.Compile()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "fan-out")
	assert.Contains(t, err.Error(), "target")
	assert.Contains(t, err.Error(), "inner")
}

func TestBuilder_Compile_FanOut_JoinNodeIsFanOutSource(t *testing.T) {
	// joinNode must be an executable node; using a fan-out source as joinNode is invalid.
	b := NewGraph[string](idReducer[string])
	b.AddNode("db", noopStringNode)
	b.AddNode("web", noopStringNode)
	b.AddNode("merge", noopStringNode)
	b.AddFanOut("inner", []string{"db", "web"}, "merge")
	b.AddFanOut("outer", []string{"merge"}, "inner") // joinNode "inner" is a fan-out source, not a node
	b.SetEntryPoint("outer")
	b.SetFinishPoint("merge")
	_, err := b.Compile()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "joinNode")
	assert.Contains(t, err.Error(), "inner")
}

func TestBuilder_Compile_FanOut_JoinNodeIsDynamicFanOutSource(t *testing.T) {
	// joinNode must be an executable node; using a dynamic fan-out source as joinNode is invalid.
	b := NewGraph[string](idReducer[string])
	b.AddNode("a", noopStringNode)
	b.AddNode("merge", noopStringNode)
	b.AddDynamicFanOut(
		"route",
		func(_ context.Context, _ string) ([]string, error) { return []string{"a"}, nil },
		"merge",
	)
	b.AddFanOut("start", []string{"a"}, "route") // joinNode "route" is dynamic fan-out source, not a node
	b.SetEntryPoint("start")
	b.SetFinishPoint("merge")
	_, err := b.Compile()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "joinNode")
	assert.Contains(t, err.Error(), "route")
}

func TestBuilder_Compile_FinishPointNotRegistered(t *testing.T) {
	b := NewGraph[string](idReducer[string])
	b.AddNode("a", noopStringNode)
	b.SetEntryPoint("a")
	b.SetFinishPoint("nonexistent")
	_, err := b.Compile()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "finish point")
	assert.Contains(t, err.Error(), "not registered")
}

func TestBuilder_Compile_EmptyNodeName(t *testing.T) {
	b := NewGraph[string](idReducer[string])
	b.AddNode("", noopStringNode)
	b.AddNode("a", noopStringNode)
	b.SetEntryPoint("a")
	b.SetFinishPoint("a")
	_, err := b.Compile()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "node name must not be empty")
}

func TestBuilder_Compile_NilNodeHandler(t *testing.T) {
	b := NewGraph[string](idReducer[string])
	b.AddNode("a", nil)
	b.SetEntryPoint("a")
	b.SetFinishPoint("a")
	_, err := b.Compile()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "nil handler")
}

func TestBuilder_Compile_NilReducer(t *testing.T) {
	b := NewGraph[string](nil)
	b.AddNode("a", noopStringNode)
	b.SetEntryPoint("a")
	b.SetFinishPoint("a")
	_, err := b.Compile()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "reducer must not be nil")
}

func TestBuilder_Compile_DuplicateNode(t *testing.T) {
	b := NewGraph[string](idReducer[string])
	b.AddNode("a", noopStringNode)
	b.AddNode("a", func(_ context.Context, s string) (string, error) { return s + "x", nil })
	b.SetEntryPoint("a")
	b.SetFinishPoint("a")
	_, err := b.Compile()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "node")
	assert.Contains(t, err.Error(), "registered more than once")
	assert.Contains(t, err.Error(), "a")
}

func TestBuilder_Compile_DuplicateEdge(t *testing.T) {
	b := NewGraph[string](idReducer[string])
	b.AddNode("a", noopStringNode)
	b.AddNode("b", noopStringNode)
	b.AddEdge("a", "b")
	b.AddEdge("a", "b")
	b.SetEntryPoint("a")
	b.SetFinishPoint("b")
	_, err := b.Compile()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "edge")
	assert.Contains(t, err.Error(), "registered more than once")
	assert.Contains(t, err.Error(), "a")
}

func TestBuilder_FluentAPI(t *testing.T) {
	b := NewGraph[string](idReducer[string]).
		AddNode("a", noopStringNode).
		AddNode("b", noopStringNode).
		AddEdge("a", "b").
		SetEntryPoint("a").
		SetFinishPoint("b")
	graph, err := b.Compile()
	require.NoError(t, err)
	require.NotNil(t, graph)
}

func TestMiddlewareChain(t *testing.T) {
	var order []string
	g1 := func(ctx context.Context, state string, chain *ExecutionChain[string]) (string, error) {
		order = append(order, "g1-in-"+chain.NodeName)
		out, err := chain.Next(ctx, state)
		order = append(order, "g1-out-"+chain.NodeName)
		return out, err
	}
	g2 := func(ctx context.Context, state string, chain *ExecutionChain[string]) (string, error) {
		order = append(order, "g2-in-"+chain.NodeName)
		out, err := chain.Next(ctx, state)
		order = append(order, "g2-out-"+chain.NodeName)
		return out, err
	}
	l1 := func(ctx context.Context, state string, chain *ExecutionChain[string]) (string, error) {
		order = append(order, "l1-in-"+chain.NodeName)
		out, err := chain.Next(ctx, state)
		order = append(order, "l1-out-"+chain.NodeName)
		return out, err
	}
	l2 := func(ctx context.Context, state string, chain *ExecutionChain[string]) (string, error) {
		order = append(order, "l2-in-"+chain.NodeName)
		out, err := chain.Next(ctx, state)
		order = append(order, "l2-out-"+chain.NodeName)
		return out, err
	}
	b := NewGraph[string](idReducer[string])
	b.AddNode("a", func(_ context.Context, s string) (string, error) {
		order = append(order, "node-a")
		return s + "a", nil
	}, l1, l2)
	b.SetEntryPoint("a")
	b.SetFinishPoint("a")
	b.Use(g1, g2)
	graph, err := b.Compile()
	require.NoError(t, err)
	ctx := context.Background()
	_, err = graph.Invoke(ctx, "")
	require.NoError(t, err)
	assert.Equal(
		t,
		[]string{"g1-in-a", "g2-in-a", "l1-in-a", "l2-in-a", "node-a", "l2-out-a", "l1-out-a", "g2-out-a", "g1-out-a"},
		order,
	)
}

// TestMiddleware_Chain verifies nodeName is passed correctly and middlewares can mutate state/error.
func TestMiddleware_Chain(t *testing.T) {
	var namesSeen []string
	mw := func(ctx context.Context, state string, chain *ExecutionChain[string]) (string, error) {
		namesSeen = append(namesSeen, chain.NodeName)
		out, err := chain.Next(ctx, state)
		if err != nil {
			return out, err
		}
		return out + "[" + chain.NodeName + "]", nil
	}
	b := NewGraph[string](idReducer[string])
	b.AddNode("x", func(_ context.Context, s string) (string, error) { return s + "x", nil })
	b.AddNode("y", func(_ context.Context, s string) (string, error) { return s + "y", nil })
	b.AddEdge("x", "y")
	b.SetEntryPoint("x")
	b.SetFinishPoint("y")
	b.Use(mw)
	graph, err := b.Compile()
	require.NoError(t, err)
	ctx := context.Background()
	final, err := graph.Invoke(ctx, "")
	require.NoError(t, err)
	assert.Equal(t, []string{"x", "y"}, namesSeen)
	// idReducer keeps only last update; mw appends [nodeName] to each update, so final is "x[x]y[y]"
	assert.Equal(t, "x[x]y[y]", final)
}

// TestMiddleware_ErrorInterception verifies that middleware can wrap and return node errors; caller receives the wrapped error.
func TestMiddleware_ErrorInterception(t *testing.T) {
	origErr := errors.New("original error")
	wrapMw := func(ctx context.Context, state string, chain *ExecutionChain[string]) (string, error) {
		out, err := chain.Next(ctx, state)
		if err != nil {
			return out, fmt.Errorf("middleware wrapped: %w", err)
		}
		return out, nil
	}
	b := NewGraph[string](idReducer[string])
	b.AddNode("fail", func(_ context.Context, s string) (string, error) {
		return s, origErr
	})
	b.SetEntryPoint("fail")
	b.SetFinishPoint("fail")
	b.Use(wrapMw)
	graph, err := b.Compile()
	require.NoError(t, err)
	_, err = graph.Invoke(context.Background(), "")
	require.Error(t, err)
	require.ErrorContains(t, err, "middleware wrapped")
	require.ErrorIs(t, err, origErr)
}

func TestFallbackMiddleware(t *testing.T) {
	var fallbackInput string
	fallbackNode := func(_ context.Context, s string) string {
		fallbackInput = s
		return s + "[fallback]"
	}
	fallbackMw := func(ctx context.Context, state string, chain *ExecutionChain[string]) (string, error) {
		out, err := chain.Next(ctx, state)
		if err == nil {
			return out, nil
		}
		return fallbackNode(ctx, state), nil
	}

	b := NewGraph[string](idReducer[string])
	b.AddNode("fail", func(_ context.Context, _ string) (string, error) {
		return "DIRTY", errors.New("boom")
	}, fallbackMw)
	b.AddNode("finish", func(_ context.Context, s string) (string, error) {
		return s + "[finish]", nil
	})
	b.AddEdge("fail", "finish")
	b.SetEntryPoint("fail")
	b.SetFinishPoint("finish")

	graph, err := b.Compile()
	require.NoError(t, err)

	final, err := graph.Invoke(context.Background(), "")
	require.NoError(t, err)
	assert.Empty(t, fallbackInput)
	assert.Equal(t, "[fallback][finish]", final)
	assert.NotContains(t, final, "DIRTY")
}

func TestBuilder_Compile_DynamicFanOut_JoinNodeNotRegistered(t *testing.T) {
	b := NewGraph[string](idReducer[string])
	b.AddNode("merge", noopStringNode)
	b.AddDynamicFanOut(
		"route",
		func(_ context.Context, _ string) ([]string, error) { return []string{"merge"}, nil },
		"missing",
	)
	b.SetEntryPoint("route")
	b.SetFinishPoint("merge")
	_, err := b.Compile()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "dynamic fan-out")
	assert.Contains(t, err.Error(), "joinNode")
	assert.Contains(t, err.Error(), "not a registered node")
}

func TestBuilder_Compile_DynamicFanOut_NilRouter(t *testing.T) {
	b := NewGraph[string](idReducer[string])
	b.AddNode("merge", noopStringNode)
	b.AddDynamicFanOut("route", nil, "merge")
	b.SetEntryPoint("route")
	b.SetFinishPoint("merge")
	_, err := b.Compile()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "dynamic fan-out")
	assert.Contains(t, err.Error(), "nil router")
}

func TestBuilder_Compile_DynamicFanOut_AndNodeSameName(t *testing.T) {
	b := NewGraph[string](idReducer[string])
	b.AddNode("x", noopStringNode)
	b.AddNode("merge", noopStringNode)
	b.AddDynamicFanOut(
		"x",
		func(_ context.Context, _ string) ([]string, error) { return []string{"merge"}, nil },
		"merge",
	)
	b.SetEntryPoint("x")
	b.SetFinishPoint("merge")
	_, err := b.Compile()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "both node and dynamic fan-out source")
}

func TestBuilder_Compile_DynamicFanOut_Duplicate(t *testing.T) {
	b := NewGraph[string](idReducer[string])
	b.AddNode("merge", noopStringNode)
	router := func(_ context.Context, _ string) ([]string, error) { return []string{"merge"}, nil }
	b.AddDynamicFanOut("route", router, "merge")
	b.AddDynamicFanOut("route", router, "merge")
	b.SetEntryPoint("route")
	b.SetFinishPoint("merge")
	_, err := b.Compile()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "dynamic fan-out")
	assert.Contains(t, err.Error(), "registered more than once")
}
