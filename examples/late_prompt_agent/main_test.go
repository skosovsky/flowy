package main

import (
	"context"
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type spyRenderer struct {
	calls       int
	lastPrompt  string
	lastAllowed []AllowedTool
	inner       PromptRenderer[*SalesPromptInput]
}

func (s *spyRenderer) Render(ctx context.Context, promptID string, input *SalesPromptInput) ([]Message, error) {
	s.calls++
	s.lastPrompt = promptID
	s.lastAllowed = slices.Clone(input.AllowedTools)
	return s.inner.Render(ctx, promptID, input)
}

type spyClient struct {
	calls        int
	lastMessages []Message
	lastTools    []Tool
}

func (s *spyClient) Generate(_ context.Context, messages []Message, tools []Tool) (Message, error) {
	s.calls++
	s.lastMessages = slices.Clone(messages)
	s.lastTools = slices.Clone(tools)
	return Message{Role: "assistant", Content: "ok"}, nil
}

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
	require.Len(t, final.LastRendered, 1)

	systemPrompt := final.LastRendered[0].Content
	assert.Equal(t, 1, final.RenderCount)
	assert.Equal(t, 1, renderer.calls)
	assert.Equal(t, 1, client.calls)
	assert.Equal(t, "personas/sales", renderer.lastPrompt)
	assert.NotContains(t, systemPrompt, blockedToolName)
	assert.NotContains(t, systemPrompt, "Read prior appointments")
	assert.Contains(t, systemPrompt, "book_slot")
	assert.Contains(t, systemPrompt, "Book a calendar slot")
	assert.Equal(t, []string{"book_slot"}, toolNames(client.lastTools))
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
	final, err := graph.Invoke(context.Background(), state)

	// Assert.
	require.NoError(t, err)
	assert.Equal(t, 1, renderer.calls)
	assert.Equal(t, []AllowedTool{
		{Name: "book_slot", Description: "Book a calendar slot"},
	}, renderer.lastAllowed)
	assert.Equal(t, []string{"book_slot"}, toolNames(client.lastTools))
	assert.NotContains(t, final.LastRendered[0].Content, "ghost_tool")
	assert.NotContains(t, final.LastRendered[0].Content, "Ghost description")
}
