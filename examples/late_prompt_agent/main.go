// Package main demonstrates late prompt rendering with standard flowy APIs:
// typed state, graph edges, and a policy node that filters tools before the LLM node.
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

type AgentRunState struct {
	PromptID string
	Input    *SalesPromptInput
	Tools    []Tool
	History  []Message
}

type PromptRenderer interface {
	Render(ctx context.Context, promptID string, input *SalesPromptInput) ([]Message, error)
}

type LLMClient interface {
	Generate(ctx context.Context, messages []Message, tools []Tool) (Message, error)
}

type LLMNode struct {
	renderer           PromptRenderer
	client             LLMClient
	injectAllowedTools func(input *SalesPromptInput, tools []AllowedTool) *SalesPromptInput
}

func (n LLMNode) asNode() flowy.Node[AgentRunState, flowy.NoEffect] {
	return func(ctx context.Context, state AgentRunState) (AgentRunState, flowy.Directive, error) {
		allowedTools := make([]AllowedTool, 0, len(state.Tools))
		for _, tool := range state.Tools {
			allowedTools = append(allowedTools, AllowedTool(tool))
		}

		state.Input = n.injectAllowedTools(state.Input, allowedTools)

		rendered, err := n.renderer.Render(ctx, state.PromptID, state.Input)
		if err != nil {
			return state, flowy.Fail("render failed"), err
		}

		requestMessages := append(slices.Clone(rendered), state.History...)
		reply, err := n.client.Generate(ctx, requestMessages, slices.Clone(state.Tools))
		if err != nil {
			return state, flowy.Fail("llm failed"), err
		}

		state.History = append(state.History, reply)
		return state, flowy.End(), nil
	}
}

type SalesPromptInput struct {
	CustomerName string
	Goal         string
	AllowedTools []AllowedTool
}

func injectSalesAllowedTools(in *SalesPromptInput, tools []AllowedTool) *SalesPromptInput {
	in.AllowedTools = slices.Clone(tools)
	return in
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

type spyRenderer struct {
	calls        int
	lastPrompt   string
	lastAllowed  []AllowedTool
	lastRendered []Message
	inner        PromptRenderer
}

func (s *spyRenderer) Render(ctx context.Context, promptID string, input *SalesPromptInput) ([]Message, error) {
	s.calls++
	s.lastPrompt = promptID
	s.lastAllowed = slices.Clone(input.AllowedTools)
	rendered, err := s.inner.Render(ctx, promptID, input)
	if err != nil {
		return nil, err
	}
	s.lastRendered = slices.Clone(rendered)
	return rendered, nil
}

type spyClient struct {
	calls        int
	lastMessages []Message
	lastTools    []Tool
	inner        LLMClient
}

func (s *spyClient) Generate(ctx context.Context, messages []Message, tools []Tool) (Message, error) {
	s.calls++
	s.lastMessages = slices.Clone(messages)
	s.lastTools = slices.Clone(tools)
	if s.inner != nil {
		return s.inner.Generate(ctx, messages, tools)
	}
	return Message{Role: "assistant", Content: "ok"}, nil
}

func filterTools(tools []Tool, blocked ...string) []Tool {
	blockedSet := make(map[string]struct{}, len(blocked))
	for _, name := range blocked {
		blockedSet[name] = struct{}{}
	}
	filtered := make([]Tool, 0, len(tools))
	for _, tool := range tools {
		if _, denied := blockedSet[tool.Name]; denied {
			continue
		}
		filtered = append(filtered, tool)
	}
	return filtered
}

func buildGraph(
	renderer PromptRenderer,
	client LLMClient,
) (*flowy.Graph[AgentRunState, flowy.NoEffect], error) {
	reducer := func(_ AgentRunState, update AgentRunState) AgentRunState { return update }

	builder := flowy.NewGraph[AgentRunState, flowy.NoEffect](reducer)
	builder.AddNode("policy", func(_ context.Context, state AgentRunState) (AgentRunState, flowy.Directive, error) {
		state.Tools = filterTools(state.Tools, blockedToolName)
		return state, flowy.Completed(), nil
	})
	builder.AddNode("llm", LLMNode{
		renderer:           renderer,
		client:             client,
		injectAllowedTools: injectSalesAllowedTools,
	}.asNode())
	builder.AddEdge("policy", "llm")
	builder.AllowNoOutgoingRoute("llm")
	builder.SetEntryPoint("policy")
	return builder.Compile()
}

func runGraph(
	ctx context.Context,
	graph *flowy.Graph[AgentRunState, flowy.NoEffect],
	state AgentRunState,
) (AgentRunState, error) {
	result, err := graph.NewRunner(nil).Start(ctx, "late-prompt", state)
	if err != nil {
		return AgentRunState{}, err
	}
	return result.State, nil
}

func initialState() AgentRunState {
	return AgentRunState{
		PromptID: "personas/sales",
		Input: &SalesPromptInput{
			CustomerName: "Alice",
			Goal:         "Book a follow-up call",
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
	renderer := &spyRenderer{inner: SalesRenderer{}}
	client := &spyClient{inner: EchoLLMClient{}}
	graph, err := buildGraph(renderer, client)
	if err != nil {
		log.Fatal(err)
	}

	final, err := runGraph(context.Background(), graph, initialState())
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("Render count:", renderer.calls)
	fmt.Println("System prompt:")
	fmt.Println(renderer.lastRendered[0].Content)
	fmt.Println("Assistant reply:", final.History[len(final.History)-1].Content)
}
