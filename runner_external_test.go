// Package flowy_test runs tests that need testutil (avoids import cycle).
package flowy_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/skosovsky/flowy"
	"github.com/skosovsky/flowy/testutil"
)

func idReducer[T any](_, update T) T { return update }

// TestInvoke_HITL_ErrSuspend_Resume verifies v2 HITL: node returns ErrSuspend,
// caller persists state+start node, Resume continues.
func TestInvoke_HITL_ErrSuspend_Resume(t *testing.T) {
	store := testutil.NewStore[string]()
	b := flowy.NewGraph[string](idReducer[string])
	b.AddNode("process", func(_ context.Context, s string) (string, error) { return s + "_process", nil })
	b.AddNode("approve", func(_ context.Context, _ string) (string, error) {
		return "", flowy.ErrSuspend
	})
	b.AddNode("finish", func(_ context.Context, s string) (string, error) { return s + "_finish", nil })
	b.AddEdge("process", "approve")
	b.AddEdge("approve", "finish")
	b.SetEntryPoint("process")
	b.SetFinishPoint("finish")

	graph, err := b.Compile()
	require.NoError(t, err)
	ctx := context.Background()

	state, err := graph.Invoke(ctx, "init")
	require.Error(t, err)
	require.ErrorIs(t, err, flowy.ErrSuspend)
	assert.Equal(t, "init_process", state)

	require.NoError(t, store.Save(ctx, "tid1", state, "approve"))

	// Resume: build same graph but approve now succeeds
	b2 := flowy.NewGraph[string](idReducer[string])
	b2.AddNode("process", func(_ context.Context, s string) (string, error) { return s + "_process", nil })
	b2.AddNode("approve", func(_ context.Context, s string) (string, error) { return s + "_approve", nil })
	b2.AddNode("finish", func(_ context.Context, s string) (string, error) { return s + "_finish", nil })
	b2.AddEdge("process", "approve")
	b2.AddEdge("approve", "finish")
	b2.SetEntryPoint("process")
	b2.SetFinishPoint("finish")
	graph2, err := b2.Compile()
	require.NoError(t, err)

	loaded, startNode, ok := store.Load(ctx, "tid1")
	require.True(t, ok)
	final, err := graph2.Resume(ctx, loaded, startNode)
	require.NoError(t, err)
	assert.Equal(t, "init_process_approve_finish", final)
}

// TestResume_StaleStartNode ensures Resume returns an error when startNode references a missing node.
func TestResume_StaleStartNode(t *testing.T) {
	b := flowy.NewGraph[string](idReducer[string])
	b.AddNode("a", func(_ context.Context, s string) (string, error) { return s, nil })
	b.SetEntryPoint("a")
	b.SetFinishPoint("a")
	graph, err := b.Compile()
	require.NoError(t, err)
	ctx := context.Background()

	state, err := graph.Resume(ctx, "state", "deleted_node")
	require.Error(t, err)
	assert.Equal(t, "state", state)
	assert.Contains(t, err.Error(), "not found")
	assert.Contains(t, err.Error(), "deleted_node")
}

// TestResume_EmptyStartNode ensures Resume returns a clear error when startNode is empty.
func TestResume_EmptyStartNode(t *testing.T) {
	b := flowy.NewGraph[string](idReducer[string])
	b.AddNode("a", func(_ context.Context, s string) (string, error) { return s, nil })
	b.SetEntryPoint("a")
	b.SetFinishPoint("a")
	graph, err := b.Compile()
	require.NoError(t, err)
	ctx := context.Background()

	state, err := graph.Resume(ctx, "x", "")
	require.Error(t, err)
	assert.Equal(t, "x", state)
	assert.Contains(t, err.Error(), "start node")
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
	for i := range concurrency {
		go func(seed string) {
			defer func() { done <- struct{}{} }()
			out, err := graph.Invoke(ctx, seed)
			assert.NoError(t, err)
			assert.Equal(t, seed+"ab", out)
		}(fmt.Sprintf("x%d", i))
	}
	for range concurrency {
		<-done
	}
}
