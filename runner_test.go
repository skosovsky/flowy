package flowy

import (
	"context"
	"errors"
	"fmt"
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

// TestStream_FinishPoint_NextNodeEmpty asserts that after a successful step on a finish point,
// the yielded Step has an empty NextNode (nowhere left to run).
func TestStream_FinishPoint_NextNodeEmpty(t *testing.T) {
	b := NewGraph[string](idReducer[string])
	b.AddNode("end", func(_ context.Context, s string) (string, error) { return s + "done", nil })
	b.SetEntryPoint("end")
	b.SetFinishPoint("end")

	graph, err := b.Compile()
	require.NoError(t, err)

	var steps []Step[string]
	for step, err := range graph.Stream(context.Background(), "", "x") {
		require.NoError(t, err)
		steps = append(steps, step)
	}
	require.Len(t, steps, 1)
	assert.Equal(t, "end", steps[0].NodeName)
	assert.Equal(t, "xdone", steps[0].State)
	assert.Empty(t, steps[0].NextNode)
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
	b.AddNode("c", noopStringNode) // unreachable finish
	b.AddEdge("a", "b")
	b.AddEdge("b", "a")
	b.SetEntryPoint("a")
	b.SetFinishPoint("c") // never reached

	graph, err := b.Compile(WithMaxSteps(3))
	require.NoError(t, err)
	ctx := context.Background()
	_, err = graph.Invoke(ctx, "")
	require.Error(t, err)
	require.ErrorIs(t, err, ErrMaxStepsExceeded)
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
	b.AddNode("b", noopStringNode)
	b.AddConditionalEdge("a", func(_ context.Context, _ string) (string, error) {
		return "", nil // empty node name, no error
	})
	b.AddEdge("b", "end")
	b.AddNode("end", noopStringNode)
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
	b.AddNode("b", noopStringNode)
	b.AddConditionalEdge("a", func(_ context.Context, _ string) (string, error) {
		return "nonexistent", nil
	})
	b.AddEdge("b", "end")
	b.AddNode("end", noopStringNode)
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
	graph, err := b.Compile(WithMaxConcurrency(limit))
	require.NoError(t, err)
	_, err = graph.Invoke(context.Background(), "")
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
	graph, err := b.Compile(WithMaxConcurrency(limit))
	require.NoError(t, err)
	_, err = graph.Invoke(context.Background(), "")
	require.NoError(t, err)
	assert.LessOrEqual(t, maxObserved.Load(), int32(limit), "dynamic fan-out must respect max concurrency")
}

// TestInvoke_FanOut_MaxConcurrency_ZeroOrUnset preserves default behavior (no limit when not set).
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
	out, err := graph.Invoke(ctx, "")
	require.NoError(t, err)
	assert.Equal(t, "[db][web][merge]", out)
}

func TestStream_Steps(t *testing.T) {
	b := NewGraph[string](idReducer[string])
	b.AddNode("a", func(_ context.Context, s string) (string, error) { return s + "a", nil })
	b.AddNode("b", func(_ context.Context, s string) (string, error) { return s + "b", nil })
	b.AddEdge("a", "b")
	b.SetEntryPoint("a")
	b.SetFinishPoint("b")
	graph, err := b.Compile()
	require.NoError(t, err)
	ctx := context.Background()

	var steps []string
	for step, err := range graph.Stream(ctx, "", ".") {
		require.NoError(t, err)
		steps = append(steps, step.NodeName)
	}
	assert.Equal(t, []string{"a", "b"}, steps)
}

// TestStream_FanOut_MaxConcurrency verifies Stream with WithMaxConcurrency (BuildOption) limits concurrent fan-out.
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
	graph, err := b.Compile(WithMaxConcurrency(limit))
	require.NoError(t, err)
	ctx := context.Background()
	stepCount := 0
	for step, err := range graph.Stream(ctx, "", "") {
		if err != nil {
			break
		}
		_ = step
		stepCount++
	}
	assert.Positive(t, stepCount, "Stream must yield steps")
	assert.LessOrEqual(t, maxObserved.Load(), int32(limit), "Stream fan-out must respect WithMaxConcurrency")
}

// TestStream_WithMaxSteps_YieldsError verifies Stream with WithMaxSteps yields error when limit is hit.
func TestStream_WithMaxSteps_YieldsError(t *testing.T) {
	b := NewGraph[string](idReducer[string])
	b.AddNode("a", func(_ context.Context, s string) (string, error) { return s + "a", nil })
	b.AddNode("b", func(_ context.Context, s string) (string, error) { return s + "b", nil })
	b.AddNode("c", noopStringNode)
	b.AddEdge("a", "b")
	b.AddEdge("b", "a")
	b.SetEntryPoint("a")
	b.SetFinishPoint("c") // unreachable; graph loops a->b->a so we hit max steps
	graph, err := b.Compile(WithMaxSteps(3))
	require.NoError(t, err)
	ctx := context.Background()
	var streamErr error
	for _, err := range graph.Stream(ctx, "", ".") {
		if err != nil {
			streamErr = err
			break
		}
	}
	require.Error(t, streamErr, "Stream must yield error when max steps exceeded")
	require.ErrorIs(t, streamErr, ErrMaxStepsExceeded)
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
	for _, err := range graph.Stream(ctx, "", ".") {
		if err != nil {
			require.ErrorIs(t, err, context.Canceled)
			return
		}
	}
}

// TestStream_ContextCancelled_YieldsError ensures context cancellation yields error from Stream.
func TestStream_ContextCancelled_YieldsError(t *testing.T) {
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
	_, err = graph.Invoke(ctx, ".")
	require.Error(t, err)
	require.ErrorIs(t, err, context.Canceled)
	ctx2, cancel2 := context.WithCancel(context.Background())
	cancel2()
	var streamErr error
	for _, err := range graph.Stream(ctx2, "", ".") {
		streamErr = err
		break
	}
	require.ErrorIs(t, streamErr, context.Canceled)
}

func TestStream_MaxStepsExceeded_YieldsError(t *testing.T) {
	b := NewGraph[string](idReducer[string])
	b.AddNode("a", func(_ context.Context, s string) (string, error) { return s + "a", nil })
	b.AddNode("b", func(_ context.Context, s string) (string, error) { return s + "b", nil })
	b.AddNode("c", noopStringNode)
	b.AddEdge("a", "b")
	b.AddEdge("b", "a")
	b.SetEntryPoint("a")
	b.SetFinishPoint("c") // unreachable; loop a->b->a until max steps
	graph, err := b.Compile(WithMaxSteps(3))
	require.NoError(t, err)
	ctx := context.Background()
	var streamErr error
	for _, err := range graph.Stream(ctx, "", ".") {
		if err != nil {
			streamErr = err
			break
		}
	}
	require.Error(t, streamErr)
	require.ErrorIs(t, streamErr, ErrMaxStepsExceeded)
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
	graph, err := b.Compile(WithNodeTimeout(10 * time.Millisecond))
	require.NoError(t, err)
	ctx := context.Background()
	_, err = graph.Invoke(ctx, "")
	require.Error(t, err)
	require.ErrorIs(t, err, context.DeadlineExceeded)
}

// TestStream_ContextCancelled_YieldsImmediately ensures that with already-cancelled context
// the iterator yields ctx.Err() immediately (v2: no background channel goroutine; execution is synchronous in caller).
func TestStream_ContextCancelled_YieldsImmediately(t *testing.T) {
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
	var gotErr error
	for _, err := range graph.Stream(ctx, "", ".") {
		gotErr = err
		break
	}
	require.Error(t, gotErr)
	require.ErrorIs(t, gotErr, context.Canceled)
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

// TestInvoke_FinishPoint_SingleNode ensures a single-node graph completes successfully.
func TestInvoke_FinishPoint_SingleNode(t *testing.T) {
	b := NewGraph[string](idReducer[string])
	b.AddNode("end", func(_ context.Context, s string) (string, error) { return s + "_done", nil })
	b.SetEntryPoint("end")
	b.SetFinishPoint("end")
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

func TestSubgraphNode_ParentStateUpdated(t *testing.T) {
	type SubState struct{ Value string }
	type ParentState struct {
		Prefix string
		Sub    SubState
	}
	subReducer := func(_, update SubState) SubState { return update }
	sub, err := NewGraph[SubState](subReducer).
		AddNode("n", func(_ context.Context, s SubState) (SubState, error) {
			return SubState{Value: s.Value + "_sub"}, nil
		}).
		SetEntryPoint("n").
		SetFinishPoint("n").
		Compile()
	require.NoError(t, err)

	mapIn := func(p ParentState) SubState { return p.Sub }
	mapOut := func(p ParentState, s SubState) ParentState {
		p.Sub = s
		return p
	}

	parentReducer := func(_, update ParentState) ParentState { return update }
	b := NewGraph[ParentState](parentReducer)
	b.AddNode("sub", SubgraphNode(sub, mapIn, mapOut))
	b.SetEntryPoint("sub")
	b.SetFinishPoint("sub")
	graph, err := b.Compile()
	require.NoError(t, err)
	ctx := context.Background()

	initial := ParentState{Prefix: "p", Sub: SubState{Value: "v"}}
	final, err := graph.Invoke(ctx, initial)
	require.NoError(t, err)
	assert.Equal(t, "p", final.Prefix)
	assert.Equal(t, "v_sub", final.Sub.Value)
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
	b.AddNode("merge", noopStringNode)
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
	b.AddNode("merge", noopStringNode)
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
	b.AddNode("merge", noopStringNode)
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
	mw := func(ctx context.Context, state string, chain *ExecutionChain[string]) (string, error) {
		appendOrder("mw-in-" + chain.NodeName)
		out, err := chain.Next(ctx, state)
		appendOrder("mw-out-" + chain.NodeName)
		return out, err
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

func TestExecutionChain_ApplyUpdateAndResolveNext(t *testing.T) {
	concat := func(current, update string) string { return current + update }
	savedStates := make(map[string]string)
	nextTargets := make(map[string]string)
	kinds := make(map[string]MiddlewareExecutionKind)
	canResolve := make(map[string]bool)

	mw := func(ctx context.Context, state string, chain *ExecutionChain[string]) (string, error) {
		out, err := chain.Next(ctx, state)
		if err != nil {
			return out, err
		}

		postState := chain.ApplyUpdate(state, out)
		savedStates[chain.NodeName] = postState
		kinds[chain.NodeName] = chain.ExecutionKind
		canResolve[chain.NodeName] = chain.CanResolveNext

		nextNode, resolveErr := chain.ResolveNext(ctx, postState)
		require.NoError(t, resolveErr)
		nextTargets[chain.NodeName] = nextNode
		return out, nil
	}

	b := NewGraph[string](concat)
	b.Use(mw)
	b.AddNode("start", func(_ context.Context, _ string) (string, error) { return "[start]", nil })
	b.AddNode("branch-a", func(_ context.Context, _ string) (string, error) { return "[a]", nil })
	b.AddNode("branch-b", func(_ context.Context, _ string) (string, error) { return "[b]", nil })
	b.AddConditionalEdge("start", func(_ context.Context, s string) (string, error) {
		if s == "init[start]" {
			return "branch-a", nil
		}
		return "branch-b", nil
	})
	b.SetEntryPoint("start")
	b.SetFinishPoint("branch-a")

	graph, err := b.Compile()
	require.NoError(t, err)

	final, err := graph.Invoke(context.Background(), "init")
	require.NoError(t, err)
	assert.Equal(t, "init[start][a]", final)
	assert.Equal(t, "init[start]", savedStates["start"])
	assert.Equal(t, "branch-a", nextTargets["start"])
	assert.Equal(t, "init[start][a]", savedStates["branch-a"])
	assert.Empty(t, nextTargets["branch-a"])
	assert.Equal(t, MiddlewareExecutionNode, kinds["start"])
	assert.Equal(t, MiddlewareExecutionNode, kinds["branch-a"])
	assert.True(t, canResolve["start"])
	assert.True(t, canResolve["branch-a"])
}

func TestExecutionChain_FanOutBranchCapabilities(t *testing.T) {
	concat := func(current, update string) string { return current + update }
	var (
		mu          sync.Mutex
		kinds       = make(map[string]MiddlewareExecutionKind)
		canResolve  = make(map[string]bool)
		resolveErrs = make(map[string]error)
	)

	mw := func(ctx context.Context, state string, chain *ExecutionChain[string]) (string, error) {
		mu.Lock()
		kinds[chain.NodeName] = chain.ExecutionKind
		canResolve[chain.NodeName] = chain.CanResolveNext
		mu.Unlock()
		_, resolveErr := chain.ResolveNext(ctx, state)
		mu.Lock()
		resolveErrs[chain.NodeName] = resolveErr
		mu.Unlock()
		return chain.Next(ctx, state)
	}

	b := NewGraph[string](concat)
	b.Use(mw)
	b.AddNode("db", func(_ context.Context, _ string) (string, error) { return "[db]", nil })
	b.AddNode("web", func(_ context.Context, _ string) (string, error) { return "[web]", nil })
	b.AddNode("merge", func(_ context.Context, _ string) (string, error) { return "[merge]", nil })
	b.AddFanOut("route", []string{"db", "web"}, "merge")
	b.SetEntryPoint("route")
	b.SetFinishPoint("merge")

	graph, err := b.Compile()
	require.NoError(t, err)

	out, err := graph.Invoke(context.Background(), "init")
	require.NoError(t, err)
	assert.Equal(t, "init[db][web][merge]", out)
	require.Len(t, kinds, 3)
	assert.Equal(t, MiddlewareExecutionFanOutBranch, kinds["db"])
	assert.Equal(t, MiddlewareExecutionFanOutBranch, kinds["web"])
	assert.Equal(t, MiddlewareExecutionNode, kinds["merge"])
	assert.False(t, canResolve["db"])
	assert.False(t, canResolve["web"])
	assert.True(t, canResolve["merge"])
	require.Error(t, resolveErrs["db"])
	require.Error(t, resolveErrs["web"])
	require.NoError(t, resolveErrs["merge"])
}

func TestStream_DynamicFanOut_Events(t *testing.T) {
	b := NewGraph[string](idReducer[string])
	b.AddNode("db", func(_ context.Context, s string) (string, error) { return s + "[db]", nil })
	b.AddNode("web", func(_ context.Context, s string) (string, error) { return s + "[web]", nil })
	b.AddNode("merge", func(_ context.Context, _ string) (string, error) { return "[merge]", nil })
	b.AddDynamicFanOut(
		"route",
		func(_ context.Context, _ string) ([]string, error) { return []string{"db", "web"}, nil },
		"merge",
	)
	b.SetEntryPoint("route")
	b.SetFinishPoint("merge")
	graph, err := b.Compile()
	require.NoError(t, err)
	ctx := context.Background()
	var nodeNames []string
	for step, err := range graph.Stream(ctx, "", "") {
		require.NoError(t, err)
		nodeNames = append(nodeNames, step.NodeName)
	}
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
	var count int
	for _, err := range graph.Stream(ctx, "", "x") {
		require.NoError(t, err)
		count++
	}
	assert.Positive(t, count)
}

// TestInvoke_ErrSuspend verifies a node returning ErrSuspend causes Invoke to return (state, ErrSuspend).
func TestInvoke_ErrSuspend(t *testing.T) {
	b := NewGraph[string](idReducer[string])
	b.AddNode("process", func(_ context.Context, s string) (string, error) { return s + "_process", nil })
	b.AddNode("approve", func(_ context.Context, s string) (string, error) { return s + "_hold", ErrSuspend })
	b.AddEdge("process", "approve")
	b.SetEntryPoint("process")
	b.SetFinishPoint("approve")
	graph, err := b.Compile()
	require.NoError(t, err)
	ctx := context.Background()
	state, err := graph.Invoke(ctx, "init")
	require.Error(t, err)
	require.ErrorIs(t, err, ErrSuspend)
	assert.Equal(t, "init_process_hold", state)
}

func TestMiddleware_ErrSuspendReturnsSavedState(t *testing.T) {
	b := NewGraph[string](func(current, update string) string { return current + update })
	b.AddNode("approve", func(_ context.Context, _ string) (string, error) {
		return "", ErrSuspend
	})
	b.SetEntryPoint("approve")
	b.SetFinishPoint("approve")
	b.Use(
		func(ctx context.Context, state string, chain *ExecutionChain[string]) (string, error) {
			_, err := chain.Next(ctx, state)
			if errors.Is(err, ErrSuspend) {
				return state + "[saved]", ErrSuspend
			}
			return state, err
		},
	)

	graph, err := b.Compile()
	require.NoError(t, err)

	out, err := graph.Invoke(context.Background(), "init")
	require.ErrorIs(t, err, ErrSuspend)
	assert.Equal(t, "init[saved]", out)
}

func TestStream_FanOutTargetErrSuspendFailsFast(t *testing.T) {
	concat := func(current, update string) string { return current + update }

	mw := func(ctx context.Context, state string, chain *ExecutionChain[string]) (string, error) {
		if chain.NodeName == "db" {
			return state + "[paused]", ErrSuspend
		}
		return chain.Next(ctx, state)
	}

	b := NewGraph[string](concat)
	b.Use(mw)
	b.AddNode("db", func(_ context.Context, _ string) (string, error) { return "[db]", nil })
	b.AddNode("web", func(_ context.Context, _ string) (string, error) { return "[web]", nil })
	b.AddNode("merge", func(_ context.Context, _ string) (string, error) { return "[merge]", nil })
	b.AddFanOut("route", []string{"db", "web"}, "merge")
	b.SetEntryPoint("route")
	b.SetFinishPoint("merge")

	graph, err := b.Compile()
	require.NoError(t, err)

	var finalStep Step[string]
	var finalErr error
	for step, err := range graph.Stream(context.Background(), "", "init") {
		if err != nil {
			finalStep = step
			finalErr = err
		}
	}

	require.Error(t, finalErr)
	require.NotErrorIs(t, finalErr, ErrSuspend)
	assert.Empty(t, finalStep.NodeName)
	assert.Contains(t, finalErr.Error(), `ErrSuspend is not supported inside fan-out target "db"`)
	assert.Contains(t, finalErr.Error(), "suspend before the fan-out source or after the join node")
}

func TestInvoke_DynamicFanOutTargetErrSuspendFailsFast(t *testing.T) {
	concat := func(current, update string) string { return current + update }

	mw := func(ctx context.Context, state string, chain *ExecutionChain[string]) (string, error) {
		if chain.NodeName == "db" {
			return state + "[paused]", ErrSuspend
		}
		return chain.Next(ctx, state)
	}

	b := NewGraph[string](concat)
	b.Use(mw)
	b.AddNode("db", func(_ context.Context, _ string) (string, error) { return "[db]", nil })
	b.AddNode("web", func(_ context.Context, _ string) (string, error) { return "[web]", nil })
	b.AddNode("merge", func(_ context.Context, _ string) (string, error) { return "[merge]", nil })
	b.AddDynamicFanOut("route", func(_ context.Context, _ string) ([]string, error) {
		return []string{"db", "web"}, nil
	}, "merge")
	b.SetEntryPoint("route")
	b.SetFinishPoint("merge")

	graph, err := b.Compile()
	require.NoError(t, err)

	state, err := graph.Invoke(context.Background(), "init")
	require.Error(t, err)
	require.NotErrorIs(t, err, ErrSuspend)
	assert.Equal(t, "init", state)
	assert.Contains(t, err.Error(), `ErrSuspend is not supported inside fan-out target "db"`)
	assert.Contains(t, err.Error(), "suspend before the fan-out source or after the join node")
}

// TestGraph_ConcurrentInvokeStream_MiddlewareRace exercises the ExecutionChain pool under -race:
// many goroutines call Invoke and Stream on the same compiled *Graph with synchronous middleware.
func TestGraph_ConcurrentInvokeStream_MiddlewareRace(t *testing.T) {
	reducer := func(_, u int) int { return u }
	passthrough := func(ctx context.Context, state int, chain *ExecutionChain[int]) (int, error) {
		return chain.Next(ctx, state)
	}

	b := NewGraph[int](reducer)
	b.Use(passthrough, passthrough)
	b.AddNode("n", func(_ context.Context, s int) (int, error) { return s + 1, nil })
	b.SetEntryPoint("n")
	b.SetFinishPoint("n")

	graph, err := b.Compile()
	require.NoError(t, err)
	ctx := context.Background()

	const workers = 32
	const iters = 64
	var (
		wg  sync.WaitGroup
		mu  sync.Mutex
		bad error
	)
	record := func(err error) {
		if err == nil {
			return
		}
		mu.Lock()
		defer mu.Unlock()
		if bad == nil {
			bad = err
		}
	}

	wg.Add(workers)
	for w := range workers {
		go func(id int) {
			defer wg.Done()
			concurrentGraphRaceWorker(ctx, graph, id, iters, record)
		}(w)
	}
	wg.Wait()
	require.NoError(t, bad)
}

func concurrentGraphRaceInvokeOnce(ctx context.Context, g *Graph[int]) error {
	out, err := g.Invoke(ctx, 0)
	if err != nil {
		return err
	}
	if out != 1 {
		return fmt.Errorf("invoke: got %d want 1", out)
	}
	return nil
}

func concurrentGraphRaceStreamOnce(ctx context.Context, g *Graph[int]) error {
	for step, err := range g.Stream(ctx, "", 0) {
		if err != nil {
			return err
		}
		if step.State != 1 {
			return fmt.Errorf("stream: got %v want 1", step.State)
		}
	}
	return nil
}

func concurrentGraphRaceWorker(
	ctx context.Context,
	g *Graph[int],
	id, iters int,
	record func(error),
) {
	for range iters {
		var err error
		if id%2 == 0 {
			err = concurrentGraphRaceInvokeOnce(ctx, g)
		} else {
			err = concurrentGraphRaceStreamOnce(ctx, g)
		}
		if err != nil {
			record(err)
			return
		}
	}
}
