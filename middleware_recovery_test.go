package flowy

import (
	"context"
	"errors"
	"strings"
	"testing"
)

func TestRecoverMiddlewarePreservesInputAndPanicCause(t *testing.T) {
	t.Parallel()
	cause := errors.New("sentinel panic")
	cases := []struct {
		name  string
		value any
		cause error
		text  string
	}{
		{"error", cause, cause, "sentinel panic"},
		{"string", "string panic", nil, "string panic"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			// Arrange.
			node := RecoverMiddleware[int, NoEffect]()(
				func(context.Context, int) (int, Directive, error) { panic(tc.value) },
			)
			// Act.
			state, directive, err := node(context.Background(), 42)
			// Assert: error and input are direct wrapper results, independent of graph validation.
			if state != 42 || err == nil || directive.kind != directiveFail {
				t.Fatalf("state=%d directive=%+v err=%v", state, directive, err)
			}
			if tc.cause != nil && !errors.Is(err, tc.cause) {
				t.Fatalf("panic cause lost: %v", err)
			}
			if !strings.Contains(err.Error(), tc.text) {
				t.Fatalf("panic text lost: %v", err)
			}
		})
	}
}

func TestRecoverMiddlewareGraphRejectsPanicBeforeReducerAndSave(t *testing.T) {
	t.Parallel()
	// Arrange.
	cause := errors.New("node panic")
	reducerCalls := 0
	builder := NewGraph[int, NoEffect](func(_, update int) int { reducerCalls++; return update })
	builder.Use(RecoverMiddleware[int, NoEffect]())
	builder.AddNode("panic", func(context.Context, int) (int, Directive, error) { panic(cause) })
	builder.SetEntryPoint("panic").AllowNoOutgoingRoute("panic")
	graph, err := builder.Compile()
	if err != nil {
		t.Fatal(err)
	}
	cp := newMemoryCP[int, NoEffect]()
	// Act.
	result, err := graph.NewRunner(cp).Start(context.Background(), "panic-state", 42)
	// Assert.
	if !errors.Is(err, cause) || result == nil || result.State != 42 || result.Status != RunStatusFailed {
		t.Fatalf("result=%+v err=%v", result, err)
	}
	if reducerCalls != 0 || len(cp.hist) != 0 || len(result.Effects) != 0 || result.ResumeToken.ThreadID != "" {
		t.Fatal("panic was treated as successful effect/checkpoint execution")
	}
}

func TestRecoverMiddlewareDoesNotRollbackSharedMutableInput(t *testing.T) {
	t.Parallel()
	// Arrange: BYOT slice storage belongs to the host.
	input := []int{42}
	node := RecoverMiddleware[[]int, NoEffect]()(func(_ context.Context, state []int) ([]int, Directive, error) {
		state[0] = 7
		panic("after mutation")
	})
	// Act.
	state, _, err := node(context.Background(), input)
	// Assert: preserve the supplied reference, without claiming rollback of host changes.
	if err == nil || input[0] != 7 || state[0] != 7 {
		t.Fatalf("state=%v input=%v err=%v", state, input, err)
	}
}

func TestRecoverMiddlewareHonoursOuterBoundary(t *testing.T) {
	t.Parallel()
	cause := errors.New("outer middleware panic")
	panicking := func(_ Node[int, NoEffect]) Node[int, NoEffect] {
		return func(context.Context, int) (int, Directive, error) { panic(cause) }
	}
	node := func(_ context.Context, state int) (int, Directive, error) { return state, End(), nil }
	// Arrange/Act: first registered Recover is outermost and catches inner middleware.
	covered := wrapNodeWithMiddlewares(
		node,
		[]NodeMiddleware[int, NoEffect]{RecoverMiddleware[int, NoEffect](), panicking},
	)
	state, _, err := covered(context.Background(), 42)
	// Assert.
	if state != 42 || !errors.Is(err, cause) {
		t.Fatalf("state=%d err=%v", state, err)
	}
	// Arrange/Act/Assert: middleware registered outside Recover still propagates panic.
	uncovered := wrapNodeWithMiddlewares(
		node,
		[]NodeMiddleware[int, NoEffect]{panicking, RecoverMiddleware[int, NoEffect]()},
	)
	defer func() {
		recovered, ok := recover().(error)
		if !ok || !errors.Is(recovered, cause) {
			t.Fatalf("outer panic cause=%v", recovered)
		}
	}()
	_, _, _ = uncovered(context.Background(), 42)
	t.Fatal("outer middleware panic was swallowed")
}
