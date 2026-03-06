package flowy

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestExportMermaid_Simple(t *testing.T) {
	b := NewGraph[string](idReducer[string])
	b.AddNode("a", noopNode)
	b.AddNode("b", noopNode)
	b.AddEdge("a", "b")
	b.SetEntryPoint("a")
	b.SetFinishPoint("b")
	graph, err := b.Compile()
	require.NoError(t, err)

	out := graph.ExportMermaid()
	assert.True(t, strings.HasPrefix(out, "flowchart TD"))
	assert.Contains(t, out, "a --> b")
}

func TestExportMermaid_FanOut(t *testing.T) {
	b := NewGraph[string](idReducer[string])
	b.AddNode("db", noopNode)
	b.AddNode("web", noopNode)
	b.AddNode("merge", noopNode)
	b.AddFanOut("start", []string{"db", "web"}, "merge")
	b.SetEntryPoint("start")
	b.SetFinishPoint("merge")
	graph, err := b.Compile()
	require.NoError(t, err)

	out := graph.ExportMermaid()
	assert.Contains(t, out, "start --> db")
	assert.Contains(t, out, "start --> web")
	assert.Contains(t, out, "db --> merge")
	assert.Contains(t, out, "web --> merge")
}

func TestExportMermaid_Conditional(t *testing.T) {
	b := NewGraph[string](idReducer[string])
	b.AddNode("a", noopNode)
	b.AddNode("b", noopNode)
	b.AddNode("c", noopNode)
	b.AddConditionalEdge("a", func(_ context.Context, _ string) (string, error) { return "b", nil })
	b.AddEdge("b", "c")
	b.SetEntryPoint("a")
	b.SetFinishPoint("c")
	graph, err := b.Compile()
	require.NoError(t, err)

	out := graph.ExportMermaid()
	assert.Contains(t, out, "a -->|conditional| __cond_a")
}

func TestExportMermaid_MultipleConditionalEdges(t *testing.T) {
	// Each conditional edge gets a unique placeholder so the diagram does not collapse branches.
	b := NewGraph[string](idReducer[string])
	b.AddNode("a", noopNode)
	b.AddNode("b", noopNode)
	b.AddNode("c", noopNode)
	b.AddNode("d", noopNode)
	b.AddConditionalEdge("a", func(_ context.Context, _ string) (string, error) { return "b", nil })
	b.AddConditionalEdge("b", func(_ context.Context, _ string) (string, error) { return "c", nil })
	b.AddEdge("c", "d")
	b.SetEntryPoint("a")
	b.SetFinishPoint("d")
	graph, err := b.Compile()
	require.NoError(t, err)

	out := graph.ExportMermaid()
	assert.Contains(t, out, "a -->|conditional| __cond_a")
	assert.Contains(t, out, "b -->|conditional| __cond_b")
	// Placeholders must be distinct (no single __dynamic__ for all).
	assert.NotContains(t, out, "__dynamic__")
}

func TestExportMermaid_DynamicFanOut(t *testing.T) {
	b := NewGraph[string](idReducer[string])
	b.AddNode("db", noopNode)
	b.AddNode("web", noopNode)
	b.AddNode("merge", noopNode)
	b.AddDynamicFanOut("route", func(_ context.Context, _ string) ([]string, error) { return []string{"db", "web"}, nil }, "merge")
	b.AddEdge("entry", "route")
	b.AddNode("entry", noopNode)
	b.SetEntryPoint("entry")
	b.SetFinishPoint("merge")
	graph, err := b.Compile()
	require.NoError(t, err)

	out := graph.ExportMermaid()
	assert.Contains(t, out, "route -->|dynamic fan-out| __dyn_route")
	assert.Contains(t, out, "__dyn_route --> merge")
	// No dangling edges: entry -> route and dynamic block -> merge are present
	assert.Contains(t, out, "entry --> route")
}

func TestExportMermaid_SpecialCharNames(t *testing.T) {
	// Node name that normalizes to empty (all special chars) gets a fallback ID.
	b := NewGraph[string](idReducer[string])
	b.AddNode("!!!", noopNode)
	b.AddNode("b", noopNode)
	b.AddEdge("!!!", "b")
	b.SetEntryPoint("!!!")
	b.SetFinishPoint("b")
	graph, err := b.Compile()
	require.NoError(t, err)

	out := graph.ExportMermaid()
	assert.True(t, strings.HasPrefix(out, "flowchart TD"))
	// Fallback ID must be valid Mermaid (alphanumeric/underscore); no empty node IDs.
	assert.NotContains(t, out, "  --> ")
	assert.Contains(t, out, " --> b")
	// Fallback for "!!!" is _n<hex>
	assert.Regexp(t, `_n[0-9a-f]+ --> b`, out)
}

func TestExportMermaid_CollidingNames(t *testing.T) {
	// Different node names that sanitize to the same ID must get unique Mermaid IDs (no diagram collision).
	b := NewGraph[string](idReducer[string])
	b.AddNode("a-b", noopNode)
	b.AddNode("a_b", noopNode)
	b.AddNode("a b", noopNode)
	b.AddEdge("a-b", "a_b")
	b.AddEdge("a_b", "a b")
	b.SetEntryPoint("a-b")
	b.SetFinishPoint("a b")
	graph, err := b.Compile()
	require.NoError(t, err)

	out := graph.ExportMermaid()
	// All three nodes must appear; colliding base "a_b" gets suffixes so we have a_b, a_b_0, a_b_1.
	assert.Contains(t, out, "-->")
	assert.Regexp(t, `a_b_\d+`, out, "colliding names must get numeric suffix")
	// Exactly two edges in this graph
	assert.Equal(t, 2, strings.Count(out, "-->"))
}

// TestExportMermaid_SimpleLinear verifies Mermaid export for a linear graph (smoke).
func TestExportMermaid_SimpleLinear(t *testing.T) {
	b := NewGraph[string](idReducer[string])
	b.AddNode("a", noopNode)
	b.AddNode("b", noopNode)
	b.AddNode("c", noopNode)
	b.AddEdge("a", "b")
	b.AddEdge("b", "c")
	b.SetEntryPoint("a")
	b.SetFinishPoint("c")
	graph, err := b.Compile()
	require.NoError(t, err)
	out := graph.ExportMermaid()
	assert.True(t, strings.HasPrefix(out, "flowchart TD"))
	assert.Contains(t, out, "a --> b")
	assert.Contains(t, out, "b --> c")
}
