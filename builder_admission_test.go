package flowy

import (
	"context"
	"strings"
	"testing"
)

func TestCompileRejectsReservedTerminalNode(t *testing.T) {
	t.Parallel()
	// Arrange: routing sentinel registration must not silently shadow a handler.
	builder := NewGraph[int, NoEffect](func(_, update int) int { return update })
	builder.AddNode(EndNode, func(context.Context, int) (int, Directive, error) { return 1, End(), nil })
	builder.SetEntryPoint(EndNode).AllowNoOutgoingRoute(EndNode)
	// Act.
	graph, err := builder.Compile()
	// Assert.
	if graph != nil || err == nil || !strings.Contains(err.Error(), "reserved for terminal routing") {
		t.Fatalf("graph=%v error=%v", graph, err)
	}
}

func TestCompileDiagnosticsAreStableAcrossRegistrationOrder(t *testing.T) {
	t.Parallel()
	var baseline string
	for iteration := range 50 {
		// Arrange: reverse registration while maps independently choose traversal order.
		builder := NewGraph[int, NoEffect](func(_, update int) int { return update })
		names := []string{"charlie", "alpha", "bravo"}
		if iteration%2 == 0 {
			names = []string{"bravo", "alpha", "charlie"}
		}
		for _, name := range names {
			builder.AddNode(name, nil).AddEdge(name, "missing-"+name)
			builder.AddConditionalEdge(name, nil, "unknown-"+name)
		}
		builder.SetEntryPoint("absent")
		// Act.
		_, err := builder.Compile()
		// Assert: every diagnostic survives and full output is stable.
		if err == nil {
			t.Fatal("invalid graph compiled")
		}
		if iteration == 0 {
			baseline = err.Error()
		}
		if err.Error() != baseline {
			t.Fatalf("diagnostics changed:\n%s\nwant:\n%s", err, baseline)
		}
		for _, name := range names {
			if !strings.Contains(err.Error(), "missing-"+name) || !strings.Contains(err.Error(), "unknown-"+name) ||
				!strings.Contains(err.Error(), "node \""+name+"\" has nil handler") {
				t.Fatalf("missing diagnostic for %s: %v", name, err)
			}
		}
	}
}
