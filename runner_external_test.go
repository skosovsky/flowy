// Package flowy_test runs tests that need testutil (avoids import cycle).
package flowy_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/skosovsky/flowy"
	"github.com/skosovsky/flowy/testutil"
)

func idReducer[T any](_, update T) T { return update }

func TestInvoke_HITL_InterruptBefore_Resume(t *testing.T) {
	cp := testutil.NewInMemoryCheckpointer[string]()
	b := flowy.NewGraph[string](idReducer[string])
	b.AddNode("process", func(_ context.Context, s string) (string, error) { return s + "_process", nil })
	b.AddNode("approve", func(_ context.Context, s string) (string, error) { return s + "_approve", nil })
	b.AddEdge("process", "approve")
	b.SetEntryPoint("process")
	b.SetFinishPoint("approve")
	b.InterruptBefore("approve")

	graph, err := b.Compile(flowy.WithCheckpointer(cp), flowy.WithThreadID[string]("tid1"))
	require.NoError(t, err)
	ctx := context.Background()

	state, err := graph.Invoke(ctx, "init")
	require.Error(t, err)
	require.ErrorIs(t, err, flowy.ErrInterrupt)
	assert.Equal(t, "init_process", state)

	final, err := graph.Resume(ctx, "tid1", "_delta")
	require.NoError(t, err)
	assert.Equal(t, "_delta_approve", final)
}

func TestInvoke_HITL_InterruptAfter_Resume(t *testing.T) {
	cp := testutil.NewInMemoryCheckpointer[string]()
	b := flowy.NewGraph[string](idReducer[string])
	b.AddNode("process", func(_ context.Context, s string) (string, error) { return s + "_process", nil })
	b.AddNode("approve", func(_ context.Context, s string) (string, error) { return s + "_approve", nil })
	b.AddNode("finish", func(_ context.Context, s string) (string, error) { return s + "_finish", nil })
	b.AddEdge("process", "approve")
	b.AddEdge("approve", "finish")
	b.SetEntryPoint("process")
	b.SetFinishPoint("finish")
	b.InterruptAfter("process")

	graph, err := b.Compile(flowy.WithCheckpointer(cp), flowy.WithThreadID[string]("tid1"))
	require.NoError(t, err)
	ctx := context.Background()

	// First Invoke: process runs, then interrupt after it (checkpoint has state after process, next = approve)
	state, err := graph.Invoke(ctx, "init")
	require.Error(t, err)
	require.ErrorIs(t, err, flowy.ErrInterrupt)
	assert.Equal(t, "init_process", state)

	// Resume: delta is merged with saved state via reducer; pass "init_process" so state stays and we continue to approve -> finish
	final, err := graph.Resume(ctx, "tid1", "init_process")
	require.NoError(t, err)
	assert.Equal(t, "init_process_approve_finish", final)
}

func TestResume_ThreadNotFound(t *testing.T) {
	cp := testutil.NewInMemoryCheckpointer[string]()
	b := flowy.NewGraph[string](idReducer[string])
	b.AddNode("a", func(_ context.Context, s string) (string, error) { return s, nil })
	b.SetEntryPoint("a")
	b.SetFinishPoint("a")
	graph, err := b.Compile(flowy.WithCheckpointer(cp))
	require.NoError(t, err)
	ctx := context.Background()
	_, err = graph.Resume(ctx, "nonexistent", "")
	require.Error(t, err)
	assert.ErrorIs(t, err, flowy.ErrThreadNotFound)
}

func TestResume_EmptyThreadID(t *testing.T) {
	cp := testutil.NewInMemoryCheckpointer[string]()
	b := flowy.NewGraph[string](idReducer[string])
	b.AddNode("a", func(_ context.Context, s string) (string, error) { return s, nil })
	b.SetEntryPoint("a")
	b.SetFinishPoint("a")
	graph, err := b.Compile(flowy.WithCheckpointer(cp))
	require.NoError(t, err)
	ctx := context.Background()
	_, err = graph.Resume(ctx, "", "")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "threadID")
	assert.Contains(t, err.Error(), "empty")
}

func TestResume_StaleCheckpointNode(t *testing.T) {
	cp := testutil.NewInMemoryCheckpointer[string]()
	ctx := context.Background()
	// Save a checkpoint pointing to a node that does not exist in the graph we will compile.
	err := cp.Save(ctx, "tid1", flowy.Checkpoint[string]{State: "old", NodeName: "deleted_node"})
	require.NoError(t, err)

	b := flowy.NewGraph[string](idReducer[string])
	b.AddNode("a", func(_ context.Context, s string) (string, error) { return s, nil })
	b.SetEntryPoint("a")
	b.SetFinishPoint("a")
	graph, err := b.Compile(flowy.WithCheckpointer(cp))
	require.NoError(t, err)
	_, err = graph.Resume(ctx, "tid1", "")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "checkpoint node")
	assert.Contains(t, err.Error(), "not found")
	assert.Contains(t, err.Error(), "deleted_node")
}

// TestResume_EmptyCheckpointNodeName ensures Resume returns a clear error when the checkpoint has empty NodeName.
func TestResume_EmptyCheckpointNodeName(t *testing.T) {
	cp := testutil.NewInMemoryCheckpointer[string]()
	ctx := context.Background()
	err := cp.Save(ctx, "tid1", flowy.Checkpoint[string]{State: "x", NodeName: ""})
	require.NoError(t, err)

	b := flowy.NewGraph[string](idReducer[string])
	b.AddNode("a", func(_ context.Context, s string) (string, error) { return s, nil })
	b.SetEntryPoint("a")
	b.SetFinishPoint("a")
	graph, err := b.Compile(flowy.WithCheckpointer(cp))
	require.NoError(t, err)
	_, err = graph.Resume(ctx, "tid1", "")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "empty")
	assert.Contains(t, err.Error(), "NodeName")
}

// failingCheckpointer fails Save with a sentinel error; used to test EventError on checkpoint save failure.
type failingCheckpointer[T any] struct{}

func (failingCheckpointer[T]) Save(context.Context, string, flowy.Checkpoint[T]) error {
	return errors.New("save failed")
}
func (failingCheckpointer[T]) Load(context.Context, string) (flowy.Checkpoint[T], error) {
	var zero flowy.Checkpoint[T]
	return zero, flowy.ErrThreadNotFound
}

// TestStream_SaveCheckpointError_EmitsEventError ensures that when checkpointer.Save fails at an interrupt point,
// Stream emits EventError before returning (observability).
func TestStream_SaveCheckpointError_EmitsEventError(t *testing.T) {
	var cp failingCheckpointer[string]
	b := flowy.NewGraph[string](idReducer[string])
	b.AddNode("a", func(_ context.Context, s string) (string, error) { return s + "a", nil })
	b.AddNode("b", func(_ context.Context, s string) (string, error) { return s + "b", nil })
	b.AddEdge("a", "b")
	b.SetEntryPoint("a")
	b.SetFinishPoint("b")
	b.InterruptBefore("b")
	graph, err := b.Compile(flowy.WithCheckpointer(cp), flowy.WithThreadID[string]("tid1"))
	require.NoError(t, err)
	ctx := context.Background()
	ch := graph.Stream(ctx, ".")
	var gotErr *flowy.Event[string]
	for e := range ch {
		if e.Type == flowy.EventError {
			gotErr = &e
			break
		}
	}
	require.NotNil(t, gotErr, "Stream must emit EventError when Save fails")
	assert.Contains(t, gotErr.Err.Error(), "save checkpoint")
	assert.Contains(t, gotErr.Err.Error(), "save failed")
}

// TestResume_WithNodeTimeout_Completes verifies Resume respects WithNodeTimeout (positive: node completes within timeout).
func TestResume_WithNodeTimeout_Completes(t *testing.T) {
	cp := testutil.NewInMemoryCheckpointer[string]()
	b := flowy.NewGraph[string](idReducer[string])
	b.AddNode("a", func(_ context.Context, s string) (string, error) { return s + "a", nil })
	b.AddNode("b", func(_ context.Context, s string) (string, error) { return s + "b", nil })
	b.AddEdge("a", "b")
	b.SetEntryPoint("a")
	b.SetFinishPoint("b")
	b.InterruptBefore("b")
	graph, err := b.Compile(flowy.WithCheckpointer(cp), flowy.WithThreadID[string]("tid1"))
	require.NoError(t, err)
	ctx := context.Background()
	_, err = graph.Invoke(ctx, "x")
	require.Error(t, err)
	require.ErrorIs(t, err, flowy.ErrInterrupt)
	// Resume with node timeout; node b should complete within timeout.
	final, err := graph.Resume(ctx, "tid1", "xa", flowy.WithNodeTimeout[string](5*time.Second))
	require.NoError(t, err)
	assert.Equal(t, "xab", final)
}

// TestInvoke_Concurrent verifies that a compiled graph is safe for concurrent Invoke calls (run with -race).
func TestInvoke_Concurrent(t *testing.T) {
	b := flowy.NewGraph[string](idReducer[string])
	b.AddNode("a", func(_ context.Context, s string) (string, error) { return s + "a", nil })
	b.AddNode("b", func(_ context.Context, s string) (string, error) { return s + "b", nil })
	b.AddEdge("a", "b")
	b.SetEntryPoint("a")
	b.SetFinishPoint("b")
	graph, err := b.Compile()
	require.NoError(t, err)
	ctx := context.Background()

	const concurrency = 20
	done := make(chan struct{}, concurrency)
	for i := 0; i < concurrency; i++ {
		go func(seed string) {
			defer func() { done <- struct{}{} }()
			out, err := graph.Invoke(ctx, seed)
			assert.NoError(t, err)
			assert.Equal(t, seed+"ab", out)
		}(fmt.Sprintf("x%d", i))
	}
	for i := 0; i < concurrency; i++ {
		<-done
	}
}
