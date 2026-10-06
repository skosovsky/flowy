package flowy

import (
	"context"
	"testing"
)

func benchmarkGraph(tb testing.TB, middlewareCount int) *Graph[int, NoEffect] {
	tb.Helper()
	b := NewGraph[int, NoEffect](func(_, update int) int { return update })
	b.AddNode("hot", func(_ context.Context, state int) (int, Directive, error) {
		return state + 1, End(), nil
	}).AllowNoOutgoingRoute("hot").SetEntryPoint("hot")
	for range middlewareCount {
		b.Use(func(next Node[int, NoEffect]) Node[int, NoEffect] {
			return func(ctx context.Context, s int) (int, Directive, error) { return next(ctx, s) }
		})
	}
	graph, err := b.Compile()
	if err != nil {
		tb.Fatal(err)
	}
	return graph
}

func benchmarkNode(b *testing.B, middlewares int) {
	runner := benchmarkGraph(b, middlewares).NewRunner(nil).(*graphRunner[int, NoEffect])
	ctx := context.Background()
	nodeCtx := withNodeName(ctx, "hot")
	var meta RunMetadata
	var invocation runInvocationOptions[int, NoEffect]
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		outcome, err := runner.runNodeStep(nodeCtx, ctx, "hot", 0, meta, nil, nil, invocation)
		if err != nil || outcome.state != 1 || outcome.meta.StepCount != 1 || outcome.base.Type() != "end" {
			b.Fatalf("node did not execute: %+v %v", outcome, err)
		}
	}
}

func BenchmarkExecuteNode_NoMiddleware(b *testing.B)     { benchmarkNode(b, 0) }
func BenchmarkExecuteNode_With5Middlewares(b *testing.B) { benchmarkNode(b, 5) }

func BenchmarkOrdinaryStreaming(b *testing.B) {
	runner := benchmarkGraph(b, 0).NewRunner(nil)
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		handle, err := runner.Stream(ctx, "stream-benchmark", 0)
		if err != nil {
			b.Fatal(err)
		}
		completed := 0
		for event := range handle.Events() {
			if event.Type == EventCompleted {
				completed++
			}
		}
		result, err := handle.WaitResult()
		if err != nil || result.State != 1 || result.Status != RunStatusCompleted || completed != 1 ||
			result.ResumeToken.SnapshotRevision != 0 {
			b.Fatalf("stream contract: result=%+v completed=%d err=%v", result, completed, err)
		}
	}
}
