package flowy

import (
	"context"
	"fmt"
	"strings"
	"time"
)

// Example_linearGraph demonstrates minimal graph construction: reducer, two nodes,
// one edge, entry and finish points, and Compile.
func Example_linearGraph() {
	reducer := func(_, update string) string { return update }
	b := NewGraph[string](reducer)
	b.AddNode("a", func(_ context.Context, s string) (string, error) { return s + "a", nil })
	b.AddNode("b", func(_ context.Context, s string) (string, error) { return s + "b", nil })
	b.AddEdge("a", "b")
	b.SetEntryPoint("a")
	b.SetFinishPoint("b")

	graph, err := b.Compile()
	if err != nil {
		fmt.Println("compile error:", err)
		return
	}
	out, err := graph.Invoke(context.Background(), "")
	if err != nil {
		fmt.Println("invoke error:", err)
		return
	}
	fmt.Println(out)
	// Output: ab
}

// Example_conditionalEdges shows a graph with a choice: the router
// chooses the next node from state. Two Invoke calls with different state demonstrate different paths.
func Example_conditionalEdges() {
	reducer := func(_, update string) string { return update }
	b := NewGraph[string](reducer)
	b.AddNode("start", func(_ context.Context, s string) (string, error) { return s + "[start]", nil })
	b.AddNode("left", func(_ context.Context, s string) (string, error) { return s + "[left]", nil })
	b.AddNode("right", func(_ context.Context, s string) (string, error) { return s + "[right]", nil })
	b.AddChoice("start", func(_ context.Context, s string) (string, error) {
		if len(s) > 0 && s[0] == 'R' {
			return "right", nil
		}
		return "left", nil
	})
	b.SetEntryPoint("start")
	b.SetFinishPoint("left")
	b.SetFinishPoint("right")

	graph, err := b.Compile()
	if err != nil {
		fmt.Println("compile error:", err)
		return
	}
	ctx := context.Background()
	out1, _ := graph.Invoke(ctx, "")
	out2, _ := graph.Invoke(ctx, "R")
	fmt.Println(out1)
	fmt.Println(out2)
	// Output:
	// [start][left]
	// R[start][right]
}

// Example_mermaidExport builds a small graph and exports it to Mermaid flowchart syntax.
// Use this to log or inspect the graph structure before running it.
func Example_mermaidExport() {
	reducer := func(_, update string) string { return update }
	b := NewGraph[string](reducer)
	b.AddNode("a", func(_ context.Context, s string) (string, error) { return s, nil })
	b.AddNode("b", func(_ context.Context, s string) (string, error) { return s, nil })
	b.AddEdge("a", "b")
	b.SetEntryPoint("a")
	b.SetFinishPoint("b")

	graph, err := b.Compile()
	if err != nil {
		fmt.Println("compile error:", err)
		return
	}
	mermaid := graph.ExportMermaid()
	fmt.Print(mermaid)
	// Output:
	// flowchart TD
	//   a --> b
}

// Example_nodeMicroResilience shows applying a timeout inside one node while the graph
// passes through the parent context unchanged (stdlib only; no engine BuildOption).
func Example_nodeMicroResilience() {
	reducer := func(_, update string) string { return update }
	b := NewGraph[string](reducer)
	b.AddNode("risky", func(ctx context.Context, s string) (string, error) {
		nodeCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		defer cancel()
		// Production code would pass nodeCtx to HTTP/RPC clients, etc.
		_ = nodeCtx
		return s + "_ok", nil
	})
	b.SetEntryPoint("risky")
	b.SetFinishPoint("risky")

	graph, err := b.Compile()
	if err != nil {
		fmt.Println("compile error:", err)
		return
	}
	out, err := graph.Invoke(context.Background(), "x")
	if err != nil {
		fmt.Println("invoke error:", err)
		return
	}
	fmt.Println(out)
	// Output:
	// x_ok
}

// Example_latePromptRenderContext shows the canonical LLM-style contract: the graph
// carries [PromptRenderContext] in typed state, middleware filters tools, and the
// final node renders once from the current state instead of patching old messages.
func Example_latePromptRenderContext() {
	type promptInput struct {
		CustomerName string
		AllowedTools []string
	}
	type state struct {
		RenderContext PromptRenderContext[*promptInput]
		Tools         []string
	}
	var renderedPrompt string

	filterTools := func(blocked string) Middleware[state] {
		return func(ctx context.Context, s state, chain *ExecutionChain[state]) (state, error) {
			out, err := chain.Next(ctx, s)
			if err != nil {
				return out, err
			}

			filtered := make([]string, 0, len(out.Tools))
			for _, tool := range out.Tools {
				if tool == blocked {
					continue
				}
				filtered = append(filtered, tool)
			}
			out.Tools = filtered
			return out, nil
		}
	}

	reducer := func(_ state, update state) state { return update }
	b := NewGraph[state](reducer)
	b.AddNode("policy", func(_ context.Context, s state) (state, error) { return s, nil }, filterTools("get_history"))
	b.AddNode("llm", func(_ context.Context, s state) (state, error) {
		s.RenderContext.Input.AllowedTools = append([]string(nil), s.Tools...)
		renderedPrompt = fmt.Sprintf(
			"prompt=%s customer=%s tools=%s",
			s.RenderContext.PromptID,
			s.RenderContext.Input.CustomerName,
			strings.Join(s.RenderContext.Input.AllowedTools, ","),
		)
		return s, nil
	})
	b.AddEdge("policy", "llm")
	b.SetEntryPoint("policy")
	b.SetFinishPoint("llm")

	graph, err := b.Compile()
	if err != nil {
		fmt.Println("compile error:", err)
		return
	}

	initial := state{
		RenderContext: PromptRenderContext[*promptInput]{
			PromptID: "personas/sales",
			Input: &promptInput{
				CustomerName: "Alice",
			},
		},
		Tools: []string{"book_slot", "get_history"},
	}
	final, err := graph.Invoke(context.Background(), initial)
	if err != nil {
		fmt.Println("invoke error:", err)
		return
	}

	_ = final
	fmt.Println(renderedPrompt)
	// Output:
	// prompt=personas/sales customer=Alice tools=book_slot
}
