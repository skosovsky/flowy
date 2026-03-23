package flowy

import (
	"context"
	"fmt"
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
