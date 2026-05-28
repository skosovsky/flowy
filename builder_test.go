package flowy

import (
	"context"
	"strings"
	"testing"
)

func TestCompileRejectsMissingOutgoingRoute(t *testing.T) {
	t.Parallel()

	b := NewGraph[struct{}, NoEffect](func(_, b struct{}) struct{} { return b })
	b.AddNode("orphan", func(_ context.Context, s struct{}) (struct{}, Directive, error) {
		return s, Completed(), nil
	})
	b.SetEntryPoint("orphan")

	_, err := b.Compile()
	if err == nil || !strings.Contains(err.Error(), "no outgoing edge") {
		t.Fatalf("expected missing route compile error, got %v", err)
	}
}

func TestCompileRejectsUnknownEdgeTarget(t *testing.T) {
	t.Parallel()

	b := NewGraph[struct{}, NoEffect](func(_, b struct{}) struct{} { return b })
	b.AddNode("a", func(_ context.Context, s struct{}) (struct{}, Directive, error) {
		return s, Completed(), nil
	})
	b.AddEdge("a", "missing")
	b.SetEntryPoint("a")

	_, err := b.Compile()
	if err == nil || !strings.Contains(err.Error(), "not registered") {
		t.Fatalf("expected unknown target compile error, got %v", err)
	}
}

func TestCompileRejectsConditionalAllowedUnknownTarget(t *testing.T) {
	t.Parallel()

	b := NewGraph[struct{}, NoEffect](func(_, b struct{}) struct{} { return b })
	b.AddNode("router", func(_ context.Context, s struct{}) (struct{}, Directive, error) {
		return s, Completed(), nil
	})
	b.AddConditionalEdge("router", func(_ context.Context, _ struct{}) (string, error) {
		return "ghost", nil
	}, "ghost")
	b.SetEntryPoint("router")

	_, err := b.Compile()
	if err == nil || !strings.Contains(err.Error(), "allows unknown target") {
		t.Fatalf("expected allowed target compile error, got %v", err)
	}
}

func TestCompileRejectsRetryRouteUnknownFallback(t *testing.T) {
	t.Parallel()

	b := NewGraph[struct{}, NoEffect](func(_, b struct{}) struct{} { return b })
	b.AddNode("worker", func(_ context.Context, s struct{}) (struct{}, Directive, error) {
		return s, Retry(1), nil
	})
	b.AllowNoOutgoingRoute("worker")
	b.AddRetryRoute("worker", "missing")
	b.SetEntryPoint("worker")

	_, err := b.Compile()
	if err == nil || !strings.Contains(err.Error(), "unknown fallback") {
		t.Fatalf("expected retry route compile error, got %v", err)
	}
}

func TestCompileRejectsEdgeAndConditionalConflict(t *testing.T) {
	t.Parallel()

	b := NewGraph[struct{}, NoEffect](func(_, b struct{}) struct{} { return b })
	b.AddNode("a", func(_ context.Context, s struct{}) (struct{}, Directive, error) {
		return s, Completed(), nil
	})
	b.AddEdge("a", EndNode)
	b.AddConditionalEdge("a", func(_ context.Context, _ struct{}) (string, error) {
		return EndNode, nil
	}, EndNode)
	b.SetEntryPoint("a")

	_, err := b.Compile()
	if err == nil || !strings.Contains(err.Error(), "both edge and conditional") {
		t.Fatalf("expected routing conflict compile error, got %v", err)
	}
}

func TestCompileRejectsConditionalWithoutAllowedTargets(t *testing.T) {
	t.Parallel()

	b := NewGraph[struct{}, NoEffect](func(_, b struct{}) struct{} { return b })
	b.AddNode("router", func(_ context.Context, s struct{}) (struct{}, Directive, error) {
		return s, Completed(), nil
	})
	b.AddConditionalEdge("router", func(_ context.Context, _ struct{}) (string, error) {
		return EndNode, nil
	})
	b.SetEntryPoint("router")

	_, err := b.Compile()
	if err == nil || !strings.Contains(err.Error(), "requires allowedTargets") {
		t.Fatalf("expected missing allowed targets compile error, got %v", err)
	}
}

func TestCompileRejectsNoOutgoingRouteWithEdge(t *testing.T) {
	t.Parallel()

	b := NewGraph[struct{}, NoEffect](func(_, b struct{}) struct{} { return b })
	b.AddNode("a", func(_ context.Context, s struct{}) (struct{}, Directive, error) {
		return s, End(), nil
	})
	b.AddEdge("a", EndNode)
	b.AllowNoOutgoingRoute("a")
	b.SetEntryPoint("a")

	_, err := b.Compile()
	if err == nil || !strings.Contains(err.Error(), "AllowNoOutgoingRoute but also has a static edge") {
		t.Fatalf("expected no-outgoing misuse error, got %v", err)
	}
}

func TestCompileRejectsAllowNoOutgoingRouteUnknownNode(t *testing.T) {
	t.Parallel()

	b := NewGraph[struct{}, NoEffect](func(_, b struct{}) struct{} { return b })
	b.AddNode("a", func(_ context.Context, s struct{}) (struct{}, Directive, error) {
		return s, End(), nil
	})
	b.AllowNoOutgoingRoute("ghost")
	b.SetEntryPoint("a")

	_, err := b.Compile()
	if err == nil || !strings.Contains(err.Error(), "unknown node") {
		t.Fatalf("expected unknown exempt node error, got %v", err)
	}
}

func TestCompileRejectsRetryRouteWithoutExemption(t *testing.T) {
	t.Parallel()

	b := NewGraph[struct{}, NoEffect](func(_, b struct{}) struct{} { return b })
	b.AddNode("worker", func(_ context.Context, s struct{}) (struct{}, Directive, error) {
		return s, Retry(1), nil
	})
	b.AddNode("fallback", func(_ context.Context, s struct{}) (struct{}, Directive, error) {
		return s, End(), nil
	})
	b.AddRetryRoute("worker", "fallback")
	b.AddEdge("worker", "fallback")
	b.SetEntryPoint("worker")

	_, err := b.Compile()
	if err == nil || !strings.Contains(err.Error(), "not exempt via AllowNoOutgoingRoute") {
		t.Fatalf("expected retry exemption error, got %v", err)
	}
}

func TestCompileRejectsNoOutgoingRouteWithConditional(t *testing.T) {
	t.Parallel()

	b := NewGraph[struct{}, NoEffect](func(_, b struct{}) struct{} { return b })
	b.AddNode("a", func(_ context.Context, s struct{}) (struct{}, Directive, error) {
		return s, Completed(), nil
	})
	b.AddConditionalEdge("a", func(_ context.Context, _ struct{}) (string, error) {
		return EndNode, nil
	}, EndNode)
	b.AllowNoOutgoingRoute("a")
	b.SetEntryPoint("a")

	_, err := b.Compile()
	if err == nil || !strings.Contains(err.Error(), "conditional edge") {
		t.Fatalf("expected conditional misuse error, got %v", err)
	}
}

func TestCompileRejectsDuplicateAddNode(t *testing.T) {
	t.Parallel()

	b := NewGraph[struct{}, NoEffect](func(_, b struct{}) struct{} { return b })
	b.AddNode("a", func(_ context.Context, s struct{}) (struct{}, Directive, error) {
		return s, End(), nil
	})
	b.AddNode("a", func(_ context.Context, s struct{}) (struct{}, Directive, error) {
		return s, End(), nil
	})
	b.AllowNoOutgoingRoute("a")
	b.SetEntryPoint("a")

	_, err := b.Compile()
	if err == nil || !strings.Contains(err.Error(), "registered more than once") {
		t.Fatalf("expected duplicate node error, got %v", err)
	}
}

func TestCompileRejectsTerminalNodeWithoutExemption(t *testing.T) {
	t.Parallel()

	b := NewGraph[struct{}, NoEffect](func(_, b struct{}) struct{} { return b })
	b.AddNode("done", func(_ context.Context, s struct{}) (struct{}, Directive, error) {
		return s, End(), nil
	})
	b.SetEntryPoint("done")

	_, err := b.Compile()
	if err == nil || !strings.Contains(err.Error(), "no outgoing edge") {
		t.Fatalf("expected missing route error, got %v", err)
	}
}

func TestCompileRejectsRetryRouteUnregisteredSource(t *testing.T) {
	t.Parallel()

	b := NewGraph[struct{}, NoEffect](func(_, b struct{}) struct{} { return b })
	b.AddNode("fallback", func(_ context.Context, s struct{}) (struct{}, Directive, error) {
		return s, End(), nil
	})
	b.AllowNoOutgoingRoute("fallback")
	b.AddRetryRoute("ghost", "fallback")
	b.SetEntryPoint("fallback")

	_, err := b.Compile()
	if err == nil || !strings.Contains(err.Error(), "not registered") {
		t.Fatalf("expected unregistered retry source error, got %v", err)
	}
}

func TestCompileRejectsDuplicateAddEdge(t *testing.T) {
	t.Parallel()

	b := NewGraph[struct{}, NoEffect](func(_, b struct{}) struct{} { return b })
	b.AddNode("a", func(_ context.Context, s struct{}) (struct{}, Directive, error) {
		return s, Completed(), nil
	})
	b.AddNode("b", func(_ context.Context, s struct{}) (struct{}, Directive, error) {
		return s, End(), nil
	})
	b.AddEdge("a", "b")
	b.AddEdge("a", EndNode)
	b.AllowNoOutgoingRoute("b")
	b.SetEntryPoint("a")

	_, err := b.Compile()
	if err == nil || !strings.Contains(err.Error(), "edge from \"a\" registered more than once") {
		t.Fatalf("expected duplicate edge error, got %v", err)
	}
}

func TestCompileRejectsDuplicateAddConditionalEdge(t *testing.T) {
	t.Parallel()

	b := NewGraph[struct{}, NoEffect](func(_, b struct{}) struct{} { return b })
	b.AddNode("router", func(_ context.Context, s struct{}) (struct{}, Directive, error) {
		return s, Completed(), nil
	})
	b.AddConditionalEdge("router", func(_ context.Context, _ struct{}) (string, error) {
		return EndNode, nil
	}, EndNode)
	b.AddConditionalEdge("router", func(_ context.Context, _ struct{}) (string, error) {
		return EndNode, nil
	}, EndNode)
	b.SetEntryPoint("router")

	_, err := b.Compile()
	if err == nil || !strings.Contains(err.Error(), "conditional edge from \"router\" registered more than once") {
		t.Fatalf("expected duplicate conditional edge error, got %v", err)
	}
}

func TestCompileRejectsDuplicateAddRetryRoute(t *testing.T) {
	t.Parallel()

	b := NewGraph[struct{}, NoEffect](func(_, b struct{}) struct{} { return b })
	b.AddNode("worker", func(_ context.Context, s struct{}) (struct{}, Directive, error) {
		return s, Retry(1), nil
	})
	b.AddNode("fallback", func(_ context.Context, s struct{}) (struct{}, Directive, error) {
		return s, End(), nil
	})
	b.AllowNoOutgoingRoute("worker")
	b.AllowNoOutgoingRoute("fallback")
	b.AddRetryRoute("worker", "fallback")
	b.AddRetryRoute("worker", "fallback")
	b.SetEntryPoint("worker")

	_, err := b.Compile()
	if err == nil || !strings.Contains(err.Error(), "retry route from \"worker\" registered more than once") {
		t.Fatalf("expected duplicate retry route error, got %v", err)
	}
}

func TestCompileRejectsEmptyRetryFallback(t *testing.T) {
	t.Parallel()

	b := NewGraph[struct{}, NoEffect](func(_, b struct{}) struct{} { return b })
	b.AddNode("worker", func(_ context.Context, s struct{}) (struct{}, Directive, error) {
		return s, Retry(1), nil
	})
	b.AllowNoOutgoingRoute("worker")
	b.AddRetryRoute("worker", "")
	b.SetEntryPoint("worker")

	_, err := b.Compile()
	if err == nil || !strings.Contains(err.Error(), "empty fallback") {
		t.Fatalf("expected empty fallback error, got %v", err)
	}
}

func TestCompileJoinsMultipleErrors(t *testing.T) {
	t.Parallel()

	b := NewGraph[struct{}, NoEffect](nil)
	_, err := b.Compile()
	if err == nil {
		t.Fatal("expected compile errors")
	}
	if !strings.Contains(err.Error(), "reducer") || !strings.Contains(err.Error(), "entry point") {
		t.Fatalf("expected joined compile errors, got %v", err)
	}
}
