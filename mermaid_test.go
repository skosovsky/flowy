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
	b.AddNode("a", noopStringNode)
	b.AddNode("b", noopStringNode)
	b.AddEdge("a", "b")
	b.SetEntryPoint("a")
	b.SetFinishPoint("b")
	graph, err := b.Compile()
	require.NoError(t, err)

	out := graph.ExportMermaid()
	assert.True(t, strings.HasPrefix(out, "flowchart TD"))
	assert.Contains(t, out, "a --> b")
}

func TestExportMermaid_ParallelStepLinearEdges(t *testing.T) {
	concat := func(current, update string) string { return current + update }
	b := NewGraph[string](concat)
	b.AddNode("db", noopStringNode)
	b.AddNode("web", noopStringNode)
	b.AddNode("merge", noopStringNode)
	var g *Graph[string]
	b.AddNode("start", Parallel(&g, "start", concat, "db", "web"))
	b.AddEdge("start", "merge")
	b.SetEntryPoint("start")
	b.SetFinishPoint("merge")
	g, err := b.Compile()
	require.NoError(t, err)

	out := g.ExportMermaid()
	// Topology is linear: parallel work is inside the "start" node; diagram shows the edge to merge.
	assert.Contains(t, out, "start --> merge")
}

func TestExportMermaid_Choice(t *testing.T) {
	b := NewGraph[string](idReducer[string])
	b.AddNode("a", noopStringNode)
	b.AddNode("b", noopStringNode)
	b.AddNode("c", noopStringNode)
	b.AddChoice("a", func(_ context.Context, _ string) (string, error) { return "b", nil })
	b.AddEdge("b", "c")
	b.SetEntryPoint("a")
	b.SetFinishPoint("c")
	graph, err := b.Compile()
	require.NoError(t, err)

	out := graph.ExportMermaid()
	assert.Contains(t, out, "a -->|choice| __choice_a")
}

func TestExportMermaid_MultipleChoices(t *testing.T) {
	// Each choice gets a unique placeholder so the diagram does not collapse branches.
	b := NewGraph[string](idReducer[string])
	b.AddNode("a", noopStringNode)
	b.AddNode("b", noopStringNode)
	b.AddNode("c", noopStringNode)
	b.AddNode("d", noopStringNode)
	b.AddChoice("a", func(_ context.Context, _ string) (string, error) { return "b", nil })
	b.AddChoice("b", func(_ context.Context, _ string) (string, error) { return "c", nil })
	b.AddEdge("c", "d")
	b.SetEntryPoint("a")
	b.SetFinishPoint("d")
	graph, err := b.Compile()
	require.NoError(t, err)

	out := graph.ExportMermaid()
	assert.Contains(t, out, "a -->|choice| __choice_a")
	assert.Contains(t, out, "b -->|choice| __choice_b")
}

func TestExportMermaid_EntryToRouteToMerge(t *testing.T) {
	concat := func(current, update string) string { return current + update }
	b := NewGraph[string](concat)
	b.AddNode("db", noopStringNode)
	b.AddNode("web", noopStringNode)
	b.AddNode("merge", noopStringNode)
	b.AddNode("entry", noopStringNode)
	var g *Graph[string]
	b.AddNode(
		"route",
		ParallelDynamic(&g, "route", concat, func(_ context.Context, _ string) ([]string, error) {
			return []string{"db", "web"}, nil
		}),
	)
	b.AddEdge("entry", "route")
	b.AddEdge("route", "merge")
	b.SetEntryPoint("entry")
	b.SetFinishPoint("merge")
	g, err := b.Compile()
	require.NoError(t, err)

	out := g.ExportMermaid()
	assert.Contains(t, out, "entry --> route")
	assert.Contains(t, out, "route --> merge")
}

func TestExportMermaid_SpecialCharNames(t *testing.T) {
	// Node name that normalizes to empty (all special chars) gets a fallback ID.
	b := NewGraph[string](idReducer[string])
	b.AddNode("!!!", noopStringNode)
	b.AddNode("b", noopStringNode)
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
	b.AddNode("a-b", noopStringNode)
	b.AddNode("a_b", noopStringNode)
	b.AddNode("a b", noopStringNode)
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
	b.AddNode("a", noopStringNode)
	b.AddNode("b", noopStringNode)
	b.AddNode("c", noopStringNode)
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
