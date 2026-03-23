package flowy

import (
	"context"
	"testing"
)

func BenchmarkExecuteNode_NoMiddleware(b *testing.B) {
	reducer := func(_, u string) string { return u }
	gb := NewGraph[string](reducer)
	// Node must not allocate (e.g. avoid string concat) so the benchmark measures executeNode only.
	gb.AddNode("n", func(_ context.Context, s string) (string, error) { return s, nil })
	gb.SetEntryPoint("n")
	gb.SetFinishPoint("n")
	g, err := gb.Compile()
	if err != nil {
		b.Fatal(err)
	}
	ctx := context.Background()
	cfg := &g.defaults
	for range 3 {
		_, err := g.executeNode(ctx, "seed", "n", "n", cfg)
		if err != nil {
			b.Fatal(err)
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		_, err := g.executeNode(ctx, "seed", "n", "n", cfg)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// passthroughBenchMw is a single shared function value used five times in BenchmarkExecuteNode_With5Middlewares.
func passthroughBenchMw(ctx context.Context, state string, chain *ExecutionChain[string]) (string, error) {
	return chain.Next(ctx, state)
}

func BenchmarkExecuteNode_With5Middlewares(b *testing.B) {
	reducer := func(_, u string) string { return u }
	gb := NewGraph[string](reducer)
	gb.AddNode("n", func(_ context.Context, s string) (string, error) { return s, nil })
	gb.SetEntryPoint("n")
	gb.SetFinishPoint("n")
	gb.Use(
		passthroughBenchMw,
		passthroughBenchMw,
		passthroughBenchMw,
		passthroughBenchMw,
		passthroughBenchMw,
	)
	g, err := gb.Compile()
	if err != nil {
		b.Fatal(err)
	}
	ctx := context.Background()
	cfg := &g.defaults
	for range 3 {
		_, err := g.executeNode(ctx, "seed", "n", "n", cfg)
		if err != nil {
			b.Fatal(err)
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		_, err := g.executeNode(ctx, "seed", "n", "n", cfg)
		if err != nil {
			b.Fatal(err)
		}
	}
}
