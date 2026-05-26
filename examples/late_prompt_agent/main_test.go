package main

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLatePromptGraph_RemovedToolDoesNotAppearInRenderedSystemPrompt(t *testing.T) {
	// Arrange.
	renderer := &spyRenderer{inner: SalesRenderer{}}
	client := &spyClient{}
	graph, err := buildGraph(renderer, client)
	require.NoError(t, err)

	// Act.
	final, err := graph.Invoke(context.Background(), initialState())

	// Assert.
	require.NoError(t, err)
	require.Len(t, renderer.lastRendered, 1)

	systemPrompt := renderer.lastRendered[0].Content
	assert.Equal(t, 1, renderer.calls)
	assert.Equal(t, 1, client.calls)
	assert.Equal(t, "personas/sales", renderer.lastPrompt)
	assert.NotContains(t, systemPrompt, blockedToolName)
	assert.NotContains(t, systemPrompt, "Read prior appointments")
	assert.Contains(t, systemPrompt, "book_slot")
	assert.Contains(t, systemPrompt, "Book a calendar slot")
	assert.Equal(t, []string{"book_slot"}, toolNames(client.lastTools))
	require.Len(t, final.History, 2)
	assert.Equal(t, "ok", final.History[len(final.History)-1].Content)
}

func TestLatePromptGraph_LLMNodeUsesCurrentToolsInsteadOfStaleRenderedInput(t *testing.T) {
	// Arrange.
	renderer := &spyRenderer{inner: SalesRenderer{}}
	client := &spyClient{}
	graph, err := buildGraph(renderer, client)
	require.NoError(t, err)

	state := initialState()
	state.RenderContext.Input.AllowedTools = []AllowedTool{
		{Name: "ghost_tool", Description: "Ghost description"},
	}

	// Act.
	_, err = graph.Invoke(context.Background(), state)

	// Assert.
	require.NoError(t, err)
	assert.Equal(t, 1, renderer.calls)
	assert.Equal(t, []AllowedTool{
		{Name: "book_slot", Description: "Book a calendar slot"},
	}, renderer.lastAllowed)
	assert.Equal(t, []string{"book_slot"}, toolNames(client.lastTools))
	require.Len(t, renderer.lastRendered, 1)
	assert.NotContains(t, renderer.lastRendered[0].Content, "ghost_tool")
	assert.NotContains(t, renderer.lastRendered[0].Content, "Ghost description")
}

func TestLatePromptGraph_UsesExportedPromptRenderContext(t *testing.T) {
	// Arrange.
	state := initialState()

	// Act.
	var exported = state.RenderContext

	// Assert.
	assert.Equal(t, "personas/sales", exported.PromptID)
	require.NotNil(t, exported.Input)
	assert.Equal(t, "Alice", exported.Input.CustomerName)
}
