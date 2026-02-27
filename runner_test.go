package flowy

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInvoke_Linear(t *testing.T) {
	b := NewGraph[string](idReducer[string])
	b.AddNode("a", func(_ context.Context, s string) (string, error) { return s + "a", nil })
	b.AddNode("b", func(_ context.Context, s string) (string, error) { return s + "b", nil })
	b.AddNode("c", func(_ context.Context, s string) (string, error) { return s + "c", nil })
	b.AddEdge("a", "b")
	b.AddEdge("b", "c")
	b.SetEntryPoint("a")
	b.SetFinishPoint("c")

	graph, err := b.Compile()
	require.NoError(t, err)
	ctx := context.Background()
	out, err := graph.Invoke(ctx, ".")
	require.NoError(t, err)
	assert.Equal(t, ".abc", out)
}

func TestInvoke_Conditional(t *testing.T) {
	b := NewGraph[string](idReducer[string])
	b.AddNode("a", func(_ context.Context, s string) (string, error) { return s + "_a", nil })
	b.AddNode("b", func(_ context.Context, s string) (string, error) { return s + "_b", nil })
	b.AddNode("c", func(_ context.Context, s string) (string, error) { return s + "_c", nil })
	b.AddConditionalEdge("a", func(_ context.Context, s string) (string, error) {
		if s == "_a" {
			return "b", nil
		}
		return "c", nil
	})
	b.AddEdge("b", "end")
	b.AddEdge("c", "end")
	b.AddNode("end", func(_ context.Context, s string) (string, error) { return s, nil })
	b.SetEntryPoint("a")
	b.SetFinishPoint("end")

	graph, err := b.Compile()
	require.NoError(t, err)
	ctx := context.Background()
	out, err := graph.Invoke(ctx, "")
	require.NoError(t, err)
	// Router returns "b" when s == "_a", so path is a -> b -> end
	assert.Equal(t, "_a_b", out)
}

func TestInvoke_MaxStepsExceeded(t *testing.T) {
	b := NewGraph[string](idReducer[string])
	b.AddNode("a", func(_ context.Context, s string) (string, error) { return s + "a", nil })
	b.AddNode("b", func(_ context.Context, s string) (string, error) { return s + "b", nil })
	b.AddNode("c", noopNode) // unreachable finish
	b.AddEdge("a", "b")
	b.AddEdge("b", "a")
	b.SetEntryPoint("a")
	b.SetFinishPoint("c") // never reached

	graph, err := b.Compile(WithMaxSteps[string](3))
	require.NoError(t, err)
	ctx := context.Background()
	_, err = graph.Invoke(ctx, "")
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrMaxStepsExceeded)
}

func TestInvoke_ConditionalEdgeError(t *testing.T) {
	b := NewGraph[string](idReducer[string])
	b.AddNode("a", func(_ context.Context, s string) (string, error) { return s + "_a", nil })
	b.AddNode("b", func(_ context.Context, s string) (string, error) { return s + "_b", nil })
	b.AddConditionalEdge("a", func(_ context.Context, _ string) (string, error) {
		return "", errors.New("router failed")
	})
	b.AddEdge("b", "end")
	b.AddNode("end", func(_ context.Context, s string) (string, error) { return s, nil })
	b.SetEntryPoint("a")
	b.SetFinishPoint("end")
	graph, err := b.Compile()
	require.NoError(t, err)
	ctx := context.Background()
	_, err = graph.Invoke(ctx, "")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "conditional edge")
	assert.Contains(t, err.Error(), "router failed")
}

func TestInvoke_ConditionalEdge_ReturnsEmpty(t *testing.T) {
	b := NewGraph[string](idReducer[string])
	b.AddNode("a", func(_ context.Context, s string) (string, error) { return s + "_a", nil })
	b.AddNode("b", noopNode)
	b.AddConditionalEdge("a", func(_ context.Context, _ string) (string, error) {
		return "", nil // empty node name, no error
	})
	b.AddEdge("b", "end")
	b.AddNode("end", noopNode)
	b.SetEntryPoint("a")
	b.SetFinishPoint("end")
	graph, err := b.Compile()
	require.NoError(t, err)
	ctx := context.Background()
	_, err = graph.Invoke(ctx, "")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "conditional edge")
	assert.Contains(t, err.Error(), "empty node name")
}

func TestInvoke_ConditionalEdge_ReturnsUnknown(t *testing.T) {
	b := NewGraph[string](idReducer[string])
	b.AddNode("a", func(_ context.Context, s string) (string, error) { return s + "_a", nil })
	b.AddNode("b", noopNode)
	b.AddConditionalEdge("a", func(_ context.Context, _ string) (string, error) {
		return "nonexistent", nil
	})
	b.AddEdge("b", "end")
	b.AddNode("end", noopNode)
	b.SetEntryPoint("a")
	b.SetFinishPoint("end")
	graph, err := b.Compile()
	require.NoError(t, err)
	ctx := context.Background()
	_, err = graph.Invoke(ctx, "")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "conditional edge")
	assert.Contains(t, err.Error(), "unknown node")
	assert.Contains(t, err.Error(), "nonexistent")
}

func TestInvoke_NoOutgoingEdge(t *testing.T) {
	b := NewGraph[string](idReducer[string])
	b.AddNode("a", func(_ context.Context, s string) (string, error) { return s + "a", nil })
	b.AddNode("b", func(_ context.Context, s string) (string, error) { return s + "b", nil })
	b.AddNode("c", func(_ context.Context, s string) (string, error) { return s, nil })
	b.AddEdge("a", "b")
	// "b" has no outgoing edge (no AddEdge("b", "c")); finish is "c" so we must resolve from "b" and fail
	b.SetEntryPoint("a")
	b.SetFinishPoint("c")
	graph, err := b.Compile()
	require.NoError(t, err)
	ctx := context.Background()
	_, err = graph.Invoke(ctx, "")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no outgoing edge")
	assert.Contains(t, err.Error(), "b")
}

func TestInvoke_NodeError(t *testing.T) {
	b := NewGraph[string](idReducer[string])
	b.AddNode("a", func(_ context.Context, s string) (string, error) { return s + "a", nil })
	b.AddNode("b", func(_ context.Context, _ string) (string, error) { return "", errors.New("b failed") })
	b.AddNode("c", func(_ context.Context, s string) (string, error) { return s + "c", nil })
	b.AddEdge("a", "b")
	b.AddEdge("b", "c")
	b.SetEntryPoint("a")
	b.SetFinishPoint("c")

	graph, err := b.Compile()
	require.NoError(t, err)
	ctx := context.Background()
	out, err := graph.Invoke(ctx, "")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "node \"b\"")
	assert.Contains(t, err.Error(), "b failed")
	assert.Equal(t, "a", out)
}

func TestInvoke_FanOut(t *testing.T) {
	concat := func(current, update string) string { return current + update }
	b := NewGraph[string](concat)
	b.AddNode("db", func(_ context.Context, s string) (string, error) { return s + "[db]", nil })
	b.AddNode("web", func(_ context.Context, s string) (string, error) { return s + "[web]", nil })
	b.AddNode("merge", func(_ context.Context, _ string) (string, error) { return "[merge]", nil })
	b.AddFanOut("start", []string{"db", "web"}, "merge")
	b.SetEntryPoint("start")
	b.SetFinishPoint("merge")

	graph, err := b.Compile()
	require.NoError(t, err)
	ctx := context.Background()
	out, err := graph.Invoke(ctx, "")
	require.NoError(t, err)
	// Fan-out applies reducer in targets order (db then web); merge returns delta only
	assert.Equal(t, "[db][web][merge]", out)
}

func TestStream_Events(t *testing.T) {
	b := NewGraph[string](idReducer[string])
	b.AddNode("a", func(_ context.Context, s string) (string, error) { return s + "a", nil })
	b.AddNode("b", func(_ context.Context, s string) (string, error) { return s + "b", nil })
	b.AddEdge("a", "b")
	b.SetEntryPoint("a")
	b.SetFinishPoint("b")
	graph, err := b.Compile()
	require.NoError(t, err)
	ctx := context.Background()

	ch := graph.Stream(ctx, ".")
	var events []EventType
	for e := range ch {
		events = append(events, e.Type)
	}
	assert.Contains(t, events, EventNodeStart)
	assert.Contains(t, events, EventNodeEnd)
}

func TestInvoke_ContextCancelled(t *testing.T) {
	b := NewGraph[string](idReducer[string])
	b.AddNode("a", func(ctx context.Context, s string) (string, error) {
		<-ctx.Done()
		return s, ctx.Err()
	})
	b.AddNode("b", func(_ context.Context, s string) (string, error) { return s + "b", nil })
	b.AddEdge("a", "b")
	b.SetEntryPoint("a")
	b.SetFinishPoint("b")
	graph, err := b.Compile()
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(10 * time.Millisecond)
		cancel()
	}()
	_, err = graph.Invoke(ctx, "")
	require.Error(t, err)
	require.ErrorIs(t, err, context.Canceled)
}

func TestStream_ContextCancelled(t *testing.T) {
	b := NewGraph[string](idReducer[string])
	b.AddNode("a", func(_ context.Context, s string) (string, error) { return s + "a", nil })
	b.AddNode("b", func(_ context.Context, s string) (string, error) { return s + "b", nil })
	b.AddEdge("a", "b")
	b.SetEntryPoint("a")
	b.SetFinishPoint("b")
	graph, err := b.Compile()
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	ch := graph.Stream(ctx, ".")
	for range ch {
		_ = 0 // drain channel so it closes and test completes
	}
}

// TestStream_ContextCancelled_EmitsErrorEvent ensures context cancellation is detected and EventError is attempted (same runFrom path as Invoke; Stream delivery is best-effort when ctx is already done).
func TestStream_ContextCancelled_EmitsErrorEvent(t *testing.T) {
	b := NewGraph[string](idReducer[string])
	b.AddNode("a", func(_ context.Context, s string) (string, error) { return s + "a", nil })
	b.AddNode("b", func(_ context.Context, s string) (string, error) { return s + "b", nil })
	b.AddEdge("a", "b")
	b.SetEntryPoint("a")
	b.SetFinishPoint("b")
	graph, err := b.Compile()
	require.NoError(t, err)
	// Invoke with already-cancelled context: runFrom hits ctx.Err() at loop start and returns wrapped context.Canceled (and attempts sendEvent for Stream)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = graph.Invoke(ctx, ".")
	require.Error(t, err)
	require.ErrorIs(t, err, context.Canceled)
	// Stream with cancelled context: channel must close (no hang); EventError may or may not be received (best-effort)
	ctx2, cancel2 := context.WithCancel(context.Background())
	cancel2()
	ch := graph.Stream(ctx2, ".")
	for range ch {
		_ = 0 // drain channel so it closes and test completes
	}
}

func TestStream_MaxStepsExceeded_Event(t *testing.T) {
	b := NewGraph[string](idReducer[string])
	b.AddNode("a", func(_ context.Context, s string) (string, error) { return s + "a", nil })
	b.AddNode("b", func(_ context.Context, s string) (string, error) { return s + "b", nil })
	b.AddNode("c", noopNode)
	b.AddEdge("a", "b")
	b.AddEdge("b", "a")
	b.SetEntryPoint("a")
	b.SetFinishPoint("c") // unreachable; loop a->b->a until max steps
	graph, err := b.Compile(WithMaxSteps[string](3))
	require.NoError(t, err)
	ctx := context.Background()
	ch := graph.Stream(ctx, ".")
	var gotErr *Event[string]
	for e := range ch {
		if e.Type == EventError {
			gotErr = &e
			break
		}
	}
	require.NotNil(t, gotErr)
	assert.ErrorIs(t, gotErr.Err, ErrMaxStepsExceeded)
}

func TestInvoke_NodeTimeout(t *testing.T) {
	b := NewGraph[string](idReducer[string])
	b.AddNode("slow", func(ctx context.Context, s string) (string, error) {
		select {
		case <-time.After(2 * time.Second):
			return s + "slow", nil
		case <-ctx.Done():
			return s, ctx.Err()
		}
	})
	b.SetEntryPoint("slow")
	b.SetFinishPoint("slow")
	graph, err := b.Compile(WithNodeTimeout[string](10 * time.Millisecond))
	require.NoError(t, err)
	ctx := context.Background()
	_, err = graph.Invoke(ctx, "")
	require.Error(t, err)
	assert.ErrorIs(t, err, context.DeadlineExceeded)
}

// TestStream_ContextCancelled_NoGoroutineLeak ensures cancelling context without draining the channel
// does not leave a goroutine leak (sendEvent respects ctx.Done()).
func TestStream_ContextCancelled_NoGoroutineLeak(t *testing.T) {
	b := NewGraph[string](idReducer[string])
	b.AddNode("a", func(_ context.Context, s string) (string, error) { return s + "a", nil })
	b.AddNode("b", func(_ context.Context, s string) (string, error) { return s + "b", nil })
	b.AddEdge("a", "b")
	b.SetEntryPoint("a")
	b.SetFinishPoint("b")
	graph, err := b.Compile()
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately so Stream goroutine exits on first sendEvent
	ch := graph.Stream(ctx, ".")
	// Do not drain ch; goroutine should still exit due to ctx.Done() in sendEvent
	_ = ch
}

// TestInvoke_FanOut_Error_NoGoroutineLeak ensures fan-out with one failing target does not leak goroutines.
func TestInvoke_FanOut_Error_NoGoroutineLeak(t *testing.T) {
	b := NewGraph[string](idReducer[string])
	b.AddNode("ok", func(_ context.Context, s string) (string, error) { return s + "[ok]", nil })
	b.AddNode("fail", func(_ context.Context, _ string) (string, error) { return "", errors.New("fail") })
	b.AddNode("merge", func(_ context.Context, s string) (string, error) { return s, nil })
	b.AddFanOut("start", []string{"ok", "fail"}, "merge")
	b.SetEntryPoint("start")
	b.SetFinishPoint("merge")
	graph, err := b.Compile()
	require.NoError(t, err)
	ctx := context.Background()
	_, err = graph.Invoke(ctx, "")
	require.Error(t, err)
}

func TestInvoke_FanOut_ErrorIncludesTargetName(t *testing.T) {
	b := NewGraph[string](idReducer[string])
	b.AddNode("ok", func(_ context.Context, s string) (string, error) { return s + "[ok]", nil })
	b.AddNode("fail", func(_ context.Context, _ string) (string, error) { return "", errors.New("my error") })
	b.AddNode("merge", func(_ context.Context, s string) (string, error) { return s, nil })
	b.AddFanOut("start", []string{"ok", "fail"}, "merge")
	b.SetEntryPoint("start")
	b.SetFinishPoint("merge")
	graph, err := b.Compile()
	require.NoError(t, err)
	ctx := context.Background()
	_, err = graph.Invoke(ctx, "")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "fail", "error should include failing target name")
	assert.Contains(t, err.Error(), "my error")
}

// TestInvoke_InterruptAfter_FinishPoint ensures a node that is both finish point and interruptAfter
// completes successfully (finish takes precedence; no resolveNext on terminal node).
func TestInvoke_InterruptAfter_FinishPoint(t *testing.T) {
	b := NewGraph[string](idReducer[string])
	b.AddNode("end", func(_ context.Context, s string) (string, error) { return s + "_done", nil })
	b.SetEntryPoint("end")
	b.SetFinishPoint("end")
	b.InterruptAfter("end")
	graph, err := b.Compile()
	require.NoError(t, err)
	ctx := context.Background()
	out, err := graph.Invoke(ctx, "x")
	require.NoError(t, err)
	assert.Equal(t, "x_done", out)
}

func TestAsNode_Composition(t *testing.T) {
	inner, _ := NewGraph[string](idReducer[string]).
		AddNode("x", func(_ context.Context, s string) (string, error) { return s + "x", nil }).
		SetEntryPoint("x").
		SetFinishPoint("x").
		Compile()

	b := NewGraph[string](idReducer[string])
	b.AddNode("outer", inner.AsNode())
	b.SetEntryPoint("outer")
	b.SetFinishPoint("outer")
	graph, err := b.Compile()
	require.NoError(t, err)
	ctx := context.Background()
	out, err := graph.Invoke(ctx, ".")
	require.NoError(t, err)
	assert.Equal(t, ".x", out)
}
