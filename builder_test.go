package flowy

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func idReducer[T any](_, update T) T { return update }

var noopNode = func(_ context.Context, s string) (string, error) { return s, nil }

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
	b.AddNode("a", noopNode)
	b.SetFinishPoint("a")

	_, err := b.Compile()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "entry point")
}

func TestBuilder_Compile_NoFinishPoint(t *testing.T) {
	b := NewGraph[string](idReducer[string])
	b.AddNode("a", noopNode)
	b.SetEntryPoint("a")

	_, err := b.Compile()
	require.Error(t, err)
}

func TestBuilder_Compile_EdgeToUnknownNode(t *testing.T) {
	b := NewGraph[string](idReducer[string])
	b.AddNode("a", noopNode)
	b.AddEdge("a", "missing")
	b.SetEntryPoint("a")
	b.SetFinishPoint("a")

	_, err := b.Compile()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not registered")
}

func TestBuilder_Compile_EdgeAndConditionalConflict(t *testing.T) {
	b := NewGraph[string](idReducer[string])
	b.AddNode("a", noopNode)
	b.AddNode("b", noopNode)
	b.AddEdge("a", "b")
	b.AddConditionalEdge("a", func(_ context.Context, _ string) (string, error) { return "b", nil })
	b.SetEntryPoint("a")
	b.SetFinishPoint("b")

	_, err := b.Compile()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "both edge and conditional")
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
	b.AddNode("a", noopNode)
	b.AddNode("db", noopNode)
	b.AddNode("web", noopNode)
	b.AddNode("merge", noopNode)
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
	b.AddNode("merge", noopNode)
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
	b.AddNode("x", noopNode)
	b.AddNode("merge", noopNode)
	b.AddFanOut("x", []string{"merge"}, "merge")
	b.SetEntryPoint("x")
	b.SetFinishPoint("merge")
	_, err := b.Compile()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "both node and fan-out")
}

func TestBuilder_Compile_FanOutTargetIsFanOutSource(t *testing.T) {
	b := NewGraph[string](idReducer[string])
	b.AddNode("db", noopNode)
	b.AddNode("web", noopNode)
	b.AddNode("merge", noopNode)
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

func TestBuilder_Compile_InterruptBeforeUnknownNode(t *testing.T) {
	b := NewGraph[string](idReducer[string])
	b.AddNode("a", noopNode)
	b.SetEntryPoint("a")
	b.SetFinishPoint("a")
	b.InterruptBefore("missing")
	_, err := b.Compile()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "interruptBefore")
	assert.Contains(t, err.Error(), "not registered")
}

func TestBuilder_Compile_InterruptAfterUnknownNode(t *testing.T) {
	b := NewGraph[string](idReducer[string])
	b.AddNode("a", noopNode)
	b.SetEntryPoint("a")
	b.SetFinishPoint("a")
	b.InterruptAfter("missing")
	_, err := b.Compile()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "interruptAfter")
	assert.Contains(t, err.Error(), "not registered")
}

func TestBuilder_Compile_FinishPointNotRegistered(t *testing.T) {
	b := NewGraph[string](idReducer[string])
	b.AddNode("a", noopNode)
	b.SetEntryPoint("a")
	b.SetFinishPoint("nonexistent")
	_, err := b.Compile()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "finish point")
	assert.Contains(t, err.Error(), "not registered")
}

func TestBuilder_Compile_EmptyNodeName(t *testing.T) {
	b := NewGraph[string](idReducer[string])
	b.AddNode("", noopNode)
	b.AddNode("a", noopNode)
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
	b.AddNode("a", noopNode)
	b.SetEntryPoint("a")
	b.SetFinishPoint("a")
	_, err := b.Compile()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "reducer must not be nil")
}

func TestBuilder_Compile_InterruptBefore_FanOutTarget(t *testing.T) {
	b := NewGraph[string](idReducer[string])
	b.AddNode("db", noopNode)
	b.AddNode("web", noopNode)
	b.AddNode("merge", noopNode)
	b.AddFanOut("start", []string{"db", "web"}, "merge")
	b.SetEntryPoint("start")
	b.SetFinishPoint("merge")
	b.InterruptBefore("db")
	_, err := b.Compile()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "interruptBefore")
	assert.Contains(t, err.Error(), "fan-out target")
	assert.Contains(t, err.Error(), "db")
}

func TestBuilder_Compile_InterruptAfter_FanOutTarget(t *testing.T) {
	b := NewGraph[string](idReducer[string])
	b.AddNode("db", noopNode)
	b.AddNode("web", noopNode)
	b.AddNode("merge", noopNode)
	b.AddFanOut("start", []string{"db", "web"}, "merge")
	b.SetEntryPoint("start")
	b.SetFinishPoint("merge")
	b.InterruptAfter("web")
	_, err := b.Compile()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "interruptAfter")
	assert.Contains(t, err.Error(), "fan-out target")
	assert.Contains(t, err.Error(), "web")
}

func TestBuilder_Compile_DuplicateNode(t *testing.T) {
	b := NewGraph[string](idReducer[string])
	b.AddNode("a", noopNode)
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
	b.AddNode("a", noopNode)
	b.AddNode("b", noopNode)
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
		AddNode("a", noopNode).
		AddNode("b", noopNode).
		AddEdge("a", "b").
		SetEntryPoint("a").
		SetFinishPoint("b")
	graph, err := b.Compile()
	require.NoError(t, err)
	require.NotNil(t, graph)
}
