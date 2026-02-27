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
	assert.Contains(t, out, "a -->|conditional| __dynamic__")
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
