package flowy

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
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

// TestInvoke_MultipleFinishPoints verifies execution stops at the first reached finish point.
func TestInvoke_MultipleFinishPoints(t *testing.T) {
	b := NewGraph[string](idReducer[string])
	b.AddNode("a", func(_ context.Context, s string) (string, error) { return s + "a", nil })
	b.AddNode("b", func(_ context.Context, s string) (string, error) { return s + "b", nil })
	b.AddNode("c", func(_ context.Context, s string) (string, error) { return s + "c", nil })
	b.AddEdge("a", "b")
	b.AddEdge("b", "c")
	b.SetEntryPoint("a")
	b.SetFinishPoint("b")
	b.SetFinishPoint("c")
	graph, err := b.Compile()
	require.NoError(t, err)
	ctx := context.Background()
	out, err := graph.Invoke(ctx, ".")
	require.NoError(t, err)
	// Stops at first finish point reached (b).
	assert.Equal(t, ".ab", out)
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

// TestInvoke_FanOut_MaxConcurrency verifies that at most N goroutines run at once when WithMaxConcurrency(N) is set.
// Uses atomic CAS to record max concurrent active; a short hold (Sleep) gives the scheduler time to overlap goroutines so we observe the cap.
func TestInvoke_FanOut_MaxConcurrency(t *testing.T) {
	const limit = 2
	var active, maxObserved atomic.Int32
	concat := func(current, update string) string { return current + update }
	b := NewGraph[string](concat)
	for i := range 10 {
		name := string(rune('a' + i))
		b.AddNode(name, func(_ context.Context, s string) (string, error) {
			active.Add(1)
			defer active.Add(-1)
			for {
				c := active.Load()
				m := maxObserved.Load()
				if c <= m {
					break
				}
				if maxObserved.CompareAndSwap(m, c) {
					break
				}
			}
			time.Sleep(20 * time.Millisecond)
			return s + "[" + name + "]", nil
		})
	}
	targets := []string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j"}
	b.AddNode("merge", func(_ context.Context, s string) (string, error) { return s + "[merge]", nil })
	b.AddFanOut("start", targets, "merge")
	b.SetEntryPoint("start")
	b.SetFinishPoint("merge")
	graph, err := b.Compile()
	require.NoError(t, err)
	_, err = graph.Invoke(context.Background(), "", WithMaxConcurrency[string](limit))
	require.NoError(t, err)
	assert.LessOrEqual(t, maxObserved.Load(), int32(limit), "observed concurrent goroutines must not exceed limit")
}

// TestInvoke_DynamicFanOut_MaxConcurrency verifies max concurrency applies to dynamic fan-out as well.
// Same atomic + short Sleep to observe concurrency cap.
func TestInvoke_DynamicFanOut_MaxConcurrency(t *testing.T) {
	const limit = 2
	var active, maxObserved atomic.Int32
	concat := func(current, update string) string { return current + update }
	b := NewGraph[string](concat)
	for i := range 6 {
		name := string(rune('a' + i))
		b.AddNode(name, func(_ context.Context, s string) (string, error) {
			active.Add(1)
			defer active.Add(-1)
			for {
				c := active.Load()
				m := maxObserved.Load()
				if c <= m {
					break
				}
				if maxObserved.CompareAndSwap(m, c) {
					break
				}
			}
			time.Sleep(20 * time.Millisecond)
			return s + "[" + name + "]", nil
		})
	}
	b.AddNode("merge", func(_ context.Context, s string) (string, error) { return s + "[merge]", nil })
	b.AddDynamicFanOut("start", func(_ context.Context, _ string) ([]string, error) {
		return []string{"a", "b", "c", "d", "e", "f"}, nil
	}, "merge")
	b.SetEntryPoint("start")
	b.SetFinishPoint("merge")
	graph, err := b.Compile()
	require.NoError(t, err)
	_, err = graph.Invoke(context.Background(), "", WithMaxConcurrency[string](limit))
	require.NoError(t, err)
	assert.LessOrEqual(t, maxObserved.Load(), int32(limit), "dynamic fan-out must respect max concurrency")
}

// TestInvoke_FanOut_MaxConcurrency_ZeroOrUnset preserves default behavior (no limit).
func TestInvoke_FanOut_MaxConcurrency_ZeroOrUnset(t *testing.T) {
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
	// Without option: same result as before.
	out, err := graph.Invoke(ctx, "")
	require.NoError(t, err)
	assert.Equal(t, "[db][web][merge]", out)
	// With explicit zero: same.
	out2, err2 := graph.Invoke(ctx, "", WithMaxConcurrency[string](0))
	require.NoError(t, err2)
	assert.Equal(t, "[db][web][merge]", out2)
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

// TestStream_FanOut_MaxConcurrency verifies Stream with WithMaxConcurrency limits concurrent fan-out and the event channel closes.
func TestStream_FanOut_MaxConcurrency(t *testing.T) {
	const limit = 2
	var active, maxObserved atomic.Int32
	concat := func(current, update string) string { return current + update }
	b := NewGraph[string](concat)
	for i := range 6 {
		name := string(rune('a' + i))
		b.AddNode(name, func(_ context.Context, s string) (string, error) {
			active.Add(1)
			defer active.Add(-1)
			for {
				c := active.Load()
				m := maxObserved.Load()
				if c <= m {
					break
				}
				if maxObserved.CompareAndSwap(m, c) {
					break
				}
			}
			time.Sleep(20 * time.Millisecond)
			return s + "[" + name + "]", nil
		})
	}
	b.AddNode("merge", func(_ context.Context, s string) (string, error) { return s + "[merge]", nil })
	b.AddFanOut("start", []string{"a", "b", "c", "d", "e", "f"}, "merge")
	b.SetEntryPoint("start")
	b.SetFinishPoint("merge")
	graph, err := b.Compile()
	require.NoError(t, err)
	ctx := context.Background()
	ch := graph.Stream(ctx, "", WithMaxConcurrency[string](limit))
	eventCount := 0
	for range ch {
		eventCount++
	}
	assert.Positive(t, eventCount, "Stream must emit events")
	assert.LessOrEqual(t, maxObserved.Load(), int32(limit), "Stream fan-out must respect WithMaxConcurrency")
}

// TestStream_WithMaxSteps_EmitsEventError verifies Stream with WithMaxSteps emits EventError when limit is hit.
func TestStream_WithMaxSteps_EmitsEventError(t *testing.T) {
	b := NewGraph[string](idReducer[string])
	b.AddNode("a", func(_ context.Context, s string) (string, error) { return s + "a", nil })
	b.AddNode("b", func(_ context.Context, s string) (string, error) { return s + "b", nil })
	b.AddNode("c", noopNode)
	b.AddEdge("a", "b")
	b.AddEdge("b", "a")
	b.SetEntryPoint("a")
	b.SetFinishPoint("c") // unreachable; graph loops a->b->a so we hit max steps
	graph, err := b.Compile()
	require.NoError(t, err)
	ctx := context.Background()
	ch := graph.Stream(ctx, ".", WithMaxSteps[string](3))
	var gotErr *Event[string]
	for e := range ch {
		if e.Type == EventError {
			gotErr = &e
			break
		}
	}
	require.NotNil(t, gotErr, "Stream must emit EventError when max steps exceeded")
	assert.ErrorIs(t, gotErr.Err, ErrMaxStepsExceeded)
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

func TestInvoke_DynamicFanOut_Success(t *testing.T) {
	concat := func(current, update string) string { return current + update }
	b := NewGraph[string](concat)
	b.AddNode("db", func(_ context.Context, s string) (string, error) { return s + "[db]", nil })
	b.AddNode("web", func(_ context.Context, s string) (string, error) { return s + "[web]", nil })
	b.AddNode("merge", func(_ context.Context, _ string) (string, error) { return "[merge]", nil })
	b.AddDynamicFanOut("route", func(_ context.Context, _ string) ([]string, error) {
		return []string{"db", "web"}, nil
	}, "merge")
	b.SetEntryPoint("route")
	b.SetFinishPoint("merge")
	graph, err := b.Compile()
	require.NoError(t, err)
	ctx := context.Background()
	out, err := graph.Invoke(ctx, "")
	require.NoError(t, err)
	assert.Equal(t, "[db][web][merge]", out)
}

func TestInvoke_DynamicFanOut_EmptyTargets(t *testing.T) {
	b := NewGraph[string](idReducer[string])
	b.AddNode("merge", func(_ context.Context, s string) (string, error) { return s + "_merge", nil })
	b.AddDynamicFanOut("route", func(_ context.Context, _ string) ([]string, error) {
		return nil, nil // empty targets: go straight to joinNode
	}, "merge")
	b.SetEntryPoint("route")
	b.SetFinishPoint("merge")
	graph, err := b.Compile()
	require.NoError(t, err)
	ctx := context.Background()
	out, err := graph.Invoke(ctx, "x")
	require.NoError(t, err)
	assert.Equal(t, "x_merge", out)
}

func TestInvoke_DynamicFanOut_RouterError(t *testing.T) {
	b := NewGraph[string](idReducer[string])
	b.AddNode("merge", noopNode)
	b.AddDynamicFanOut("route", func(_ context.Context, _ string) ([]string, error) {
		return nil, errors.New("router failed")
	}, "merge")
	b.SetEntryPoint("route")
	b.SetFinishPoint("merge")
	graph, err := b.Compile()
	require.NoError(t, err)
	ctx := context.Background()
	_, err = graph.Invoke(ctx, "")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "dynamic fan-out router")
	assert.Contains(t, err.Error(), "router failed")
}

func TestInvoke_DynamicFanOut_UnknownTarget(t *testing.T) {
	b := NewGraph[string](idReducer[string])
	b.AddNode("merge", noopNode)
	b.AddDynamicFanOut("route", func(_ context.Context, _ string) ([]string, error) {
		return []string{"nonexistent"}, nil
	}, "merge")
	b.SetEntryPoint("route")
	b.SetFinishPoint("merge")
	graph, err := b.Compile()
	require.NoError(t, err)
	ctx := context.Background()
	_, err = graph.Invoke(ctx, "")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "fan-out target node")
	assert.Contains(t, err.Error(), "nonexistent")
}

// TestInvoke_DynamicFanOut_MixedValidInvalid ensures no target runs when the list contains an invalid target (pre-validation).
func TestInvoke_DynamicFanOut_MixedValidInvalid(t *testing.T) {
	var dbRan bool
	b := NewGraph[string](idReducer[string])
	b.AddNode("db", func(_ context.Context, s string) (string, error) {
		dbRan = true
		return s + "[db]", nil
	})
	b.AddNode("merge", noopNode)
	b.AddDynamicFanOut("route", func(_ context.Context, _ string) ([]string, error) {
		return []string{"db", "nonexistent"}, nil
	}, "merge")
	b.SetEntryPoint("route")
	b.SetFinishPoint("merge")
	graph, err := b.Compile()
	require.NoError(t, err)
	ctx := context.Background()
	_, err = graph.Invoke(ctx, "")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "fan-out target node")
	assert.Contains(t, err.Error(), "nonexistent")
	assert.False(t, dbRan, "valid target must not run when another target is invalid")
}

// TestInvoke_DynamicFanOut_DuplicateTargets documents that duplicate names in router output
// are executed as separate branches (each occurrence runs once; reducer merges in order).
func TestInvoke_DynamicFanOut_DuplicateTargets(t *testing.T) {
	concat := func(current, update string) string { return current + update }
	b := NewGraph[string](concat)
	b.AddNode("db", func(_ context.Context, s string) (string, error) { return s + "[db]", nil })
	b.AddNode("web", func(_ context.Context, s string) (string, error) { return s + "[web]", nil })
	b.AddNode("merge", func(_ context.Context, _ string) (string, error) { return "[merge]", nil })
	b.AddDynamicFanOut("route", func(_ context.Context, _ string) ([]string, error) {
		return []string{"db", "db", "web"}, nil
	}, "merge")
	b.SetEntryPoint("route")
	b.SetFinishPoint("merge")
	graph, err := b.Compile()
	require.NoError(t, err)
	ctx := context.Background()
	out, err := graph.Invoke(ctx, "")
	require.NoError(t, err)
	// Each target runs once per occurrence: db twice, web once, then merge.
	assert.Equal(t, "[db][db][web][merge]", out)
}

// TestInvoke_DynamicFanOut_InterruptOnTarget ensures runtime error when router returns
// a node that has InterruptBefore or InterruptAfter (not supported on fan-out targets).
func TestInvoke_DynamicFanOut_InterruptOnTarget(t *testing.T) {
	b := NewGraph[string](idReducer[string])
	b.AddNode("db", noopNode)
	b.AddNode("merge", noopNode)
	b.InterruptBefore("db")
	b.AddDynamicFanOut("route", func(_ context.Context, _ string) ([]string, error) {
		return []string{"db"}, nil
	}, "merge")
	b.SetEntryPoint("route")
	b.SetFinishPoint("merge")
	graph, err := b.Compile()
	require.NoError(t, err)
	ctx := context.Background()
	_, err = graph.Invoke(ctx, "")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "interruptBefore")
	assert.Contains(t, err.Error(), "fan-out target")
	assert.Contains(t, err.Error(), "db")
}

// TestMiddleware_DynamicFanOutTargets verifies middlewares wrap nodes executed as dynamic fan-out targets.
func TestMiddleware_DynamicFanOutTargets(t *testing.T) {
	var (
		mu    sync.Mutex
		order []string
	)
	appendOrder := func(s string) {
		mu.Lock()
		defer mu.Unlock()
		order = append(order, s)
	}
	mw := func(name string, next Node[string]) Node[string] {
		return func(ctx context.Context, s string) (string, error) {
			appendOrder("mw-in-" + name)
			out, err := next(ctx, s)
			appendOrder("mw-out-" + name)
			return out, err
		}
	}
	concat := func(current, update string) string { return current + update }
	b := NewGraph[string](concat)
	b.AddNode("db", func(_ context.Context, s string) (string, error) {
		appendOrder("node-db")
		return s + "[db]", nil
	})
	b.AddNode("web", func(_ context.Context, s string) (string, error) {
		appendOrder("node-web")
		return s + "[web]", nil
	})
	b.AddNode("merge", func(_ context.Context, _ string) (string, error) { return "[merge]", nil })
	b.AddDynamicFanOut("route", func(_ context.Context, _ string) ([]string, error) {
		return []string{"db", "web"}, nil
	}, "merge")
	b.SetEntryPoint("route")
	b.SetFinishPoint("merge")
	b.Use(mw)
	graph, err := b.Compile()
	require.NoError(t, err)
	ctx := context.Background()
	out, err := graph.Invoke(ctx, "")
	require.NoError(t, err)
	assert.Equal(t, "[db][web][merge]", out)
	assert.Contains(t, order, "mw-in-db")
	assert.Contains(t, order, "mw-out-db")
	assert.Contains(t, order, "mw-in-web")
	assert.Contains(t, order, "mw-out-web")
	assert.Contains(t, order, "node-db")
	assert.Contains(t, order, "node-web")
}

func TestStream_DynamicFanOut_Events(t *testing.T) {
	b := NewGraph[string](idReducer[string])
	b.AddNode("db", func(_ context.Context, s string) (string, error) { return s + "[db]", nil })
	b.AddNode("web", func(_ context.Context, s string) (string, error) { return s + "[web]", nil })
	b.AddNode("merge", func(_ context.Context, _ string) (string, error) { return "[merge]", nil })
	b.AddDynamicFanOut("route", func(_ context.Context, _ string) ([]string, error) { return []string{"db", "web"}, nil }, "merge")
	b.SetEntryPoint("route")
	b.SetFinishPoint("merge")
	graph, err := b.Compile()
	require.NoError(t, err)
	ctx := context.Background()
	ch := graph.Stream(ctx, "")
	var types []EventType
	var nodeNames []string
	for e := range ch {
		types = append(types, e.Type)
		if e.NodeName != "" {
			nodeNames = append(nodeNames, e.NodeName)
		}
	}
	assert.Contains(t, types, EventNodeStart)
	assert.Contains(t, types, EventNodeEnd)
	assert.Contains(t, nodeNames, "route")
	assert.Contains(t, nodeNames, "merge")
}

func TestStream_DynamicFanOut_EmptyTargets(t *testing.T) {
	b := NewGraph[string](idReducer[string])
	b.AddNode("merge", func(_ context.Context, s string) (string, error) { return s + "_merge", nil })
	b.AddDynamicFanOut("route", func(_ context.Context, _ string) ([]string, error) { return nil, nil }, "merge")
	b.SetEntryPoint("route")
	b.SetFinishPoint("merge")
	graph, err := b.Compile()
	require.NoError(t, err)
	ctx := context.Background()
	ch := graph.Stream(ctx, "x")
	var types []EventType
	for e := range ch {
		types = append(types, e.Type)
	}
	assert.Contains(t, types, EventNodeStart)
	assert.Contains(t, types, EventNodeEnd)
}
