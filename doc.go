// Package flowy provides a type-safe directed graph engine for orchestrating
// AI agents with support for conditional routing, parallel execution (fan-out/fan-in),
// and middleware-based cross-cutting concerns. Execution is stateless: persistence and
// human-in-the-loop resume are implemented by the caller using [Graph.Stream] steps and
// types from the sibling persistence module (see repository README).
//
// State updates can be full replace (simple types) or merge/delta (complex state);
// see the README section "State Management Patterns" for the recommended approach.
//
// Example:
//
//	ctx := context.Background()
//	b := flowy.NewGraph[string](func(_, u string) string { return u })
//	b.AddNode("greet", func(ctx context.Context, s string) (string, error) { return "hello " + s, nil })
//	b.SetEntryPoint("greet")
//	b.SetFinishPoint("greet")
//	graph, _ := b.Compile()
//	result, err := graph.Invoke(ctx, "world")
//	if err != nil {
//		// handle error
//	}
package flowy
