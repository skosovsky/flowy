// Package main demonstrates Late Prompt Rendering with flowy-only primitives:
// typed state flows through the graph, middleware filters tools, and the final
// LLM node renders messages once immediately before calling the client.
package main

import (
	"context"
	"fmt"
	"log"
	"slices"
	"strings"

	"github.com/skosovsky/flowy"
)

const blockedToolName = "get_history"

type Message struct {
	Role    string
	Content string
}

type Tool struct {
	Name        string
	Description string
}

type AllowedTool struct {
	Name        string
	Description string
}

type ToolAwareInput interface {
	SetAllowedTools([]AllowedTool)
}

type PromptRenderContext[T ToolAwareInput] struct {
	PromptID string
	Input    T
}

type AgentRunState[T ToolAwareInput] struct {
	RenderContext PromptRenderContext[T]
	Tools         []Tool
	History       []Message
	LastRendered  []Message
	LastReply     Message
	RenderCount   int
}

type PromptRenderer[T ToolAwareInput] interface {
	Render(ctx context.Context, promptID string, input T) ([]Message, error)
}

type LLMClient interface {
	Generate(ctx context.Context, messages []Message, tools []Tool) (Message, error)
}

type LLMNode[T ToolAwareInput] struct {
	renderer PromptRenderer[T]
	client   LLMClient
}

func (n LLMNode[T]) Run(ctx context.Context, state AgentRunState[T]) (AgentRunState[T], error) {
	allowedTools := make([]AllowedTool, 0, len(state.Tools))
	for _, tool := range state.Tools {
		allowedTools = append(allowedTools, AllowedTool(tool))
	}

	state.RenderContext.Input.SetAllowedTools(allowedTools)

	rendered, err := n.renderer.Render(ctx, state.RenderContext.PromptID, state.RenderContext.Input)
	if err != nil {
		return state, err
	}

	state.RenderCount++
	state.LastRendered = slices.Clone(rendered)

	requestMessages := append(slices.Clone(rendered), state.History...)
	reply, err := n.client.Generate(ctx, requestMessages, slices.Clone(state.Tools))
	if err != nil {
		return state, err
	}

	state.LastReply = reply
	state.History = append(state.History, reply)
	return state, nil
}

type SalesPromptInput struct {
	CustomerName string
	Goal         string
	AllowedTools []AllowedTool
}

func (in *SalesPromptInput) SetAllowedTools(tools []AllowedTool) {
	in.AllowedTools = slices.Clone(tools)
}

type SalesRenderer struct{}

func (SalesRenderer) Render(_ context.Context, promptID string, input *SalesPromptInput) ([]Message, error) {
	var builder strings.Builder
	builder.WriteString("Prompt: ")
	builder.WriteString(promptID)
	builder.WriteString("\nCustomer: ")
	builder.WriteString(input.CustomerName)
	builder.WriteString("\nGoal: ")
	builder.WriteString(input.Goal)
	builder.WriteString("\nAllowed tools:")

	if len(input.AllowedTools) == 0 {
		builder.WriteString("\n- none")
	} else {
		for _, tool := range input.AllowedTools {
			builder.WriteString("\n- ")
			builder.WriteString(tool.Name)
			builder.WriteString(": ")
			builder.WriteString(tool.Description)
		}
	}

	return []Message{{Role: "system", Content: builder.String()}}, nil
}

type EchoLLMClient struct{}

func (EchoLLMClient) Generate(_ context.Context, _ []Message, tools []Tool) (Message, error) {
	return Message{
		Role:    "assistant",
		Content: "assistant sees tools: " + strings.Join(toolNames(tools), ", "),
	}, nil
}

func FilterToolsMiddleware[T ToolAwareInput](blockedNames ...string) flowy.Middleware[AgentRunState[T]] {
	blocked := make(map[string]struct{}, len(blockedNames))
	for _, name := range blockedNames {
		blocked[name] = struct{}{}
	}

	return func(ctx context.Context, state AgentRunState[T], chain *flowy.ExecutionChain[AgentRunState[T]]) (AgentRunState[T], error) {
		out, err := chain.Next(ctx, state)
		if err != nil {
			return out, err
		}

		filtered := make([]Tool, 0, len(out.Tools))
		for _, tool := range out.Tools {
			if _, denied := blocked[tool.Name]; denied {
				continue
			}
			filtered = append(filtered, tool)
		}

		out.Tools = filtered
		return out, nil
	}
}

func passStateNode[T ToolAwareInput](_ context.Context, state AgentRunState[T]) (AgentRunState[T], error) {
	return state, nil
}

func buildGraph(
	renderer PromptRenderer[*SalesPromptInput],
	client LLMClient,
) (*flowy.Graph[AgentRunState[*SalesPromptInput]], error) {
	reducer := func(_ AgentRunState[*SalesPromptInput], update AgentRunState[*SalesPromptInput]) AgentRunState[*SalesPromptInput] {
		return update
	}

	builder := flowy.NewGraph[AgentRunState[*SalesPromptInput]](reducer)
	builder.AddNode(
		"policy",
		passStateNode[*SalesPromptInput],
		FilterToolsMiddleware[*SalesPromptInput](blockedToolName),
	)
	builder.AddNode("llm", LLMNode[*SalesPromptInput]{
		renderer: renderer,
		client:   client,
	}.Run)
	builder.AddEdge("policy", "llm")
	builder.SetEntryPoint("policy")
	builder.SetFinishPoint("llm")
	return builder.Compile()
}

func initialState() AgentRunState[*SalesPromptInput] {
	return AgentRunState[*SalesPromptInput]{
		RenderContext: PromptRenderContext[*SalesPromptInput]{
			PromptID: "personas/sales",
			Input: &SalesPromptInput{
				CustomerName: "Alice",
				Goal:         "Book a follow-up call",
			},
		},
		Tools: []Tool{
			{Name: "book_slot", Description: "Book a calendar slot"},
			{Name: blockedToolName, Description: "Read prior appointments"},
		},
		History: []Message{
			{Role: "user", Content: "Please find me time next week."},
		},
	}
}

func toolNames(tools []Tool) []string {
	names := make([]string, 0, len(tools))
	for _, tool := range tools {
		names = append(names, tool.Name)
	}
	return names
}

func main() {
	graph, err := buildGraph(SalesRenderer{}, EchoLLMClient{})
	if err != nil {
		log.Fatal(err)
	}

	final, err := graph.Invoke(context.Background(), initialState())
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("Render count:", final.RenderCount)
	fmt.Println("System prompt:")
	fmt.Println(final.LastRendered[0].Content)
	fmt.Println("Assistant reply:", final.LastReply.Content)
}
