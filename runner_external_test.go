// Package flowy_test exercises the public API surface from outside the package.
package flowy_test

import (
	"context"
	"fmt"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/skosovsky/flowy"
)

func idReducer[T any](_, update T) T { return update }

func TestPublicSurface_ClearBreak(t *testing.T) {
	graphType := reflect.TypeFor[*flowy.Graph[string]]()
	_, hasResume := graphType.MethodByName("Resume")
	_, hasResumeStream := graphType.MethodByName("ResumeStream")
	_, hasThread := graphType.MethodByName("Thread")
	_, hasWithCheckpointer := graphType.MethodByName("WithCheckpointer")
	_, hasStream := graphType.MethodByName("Stream")

	assert.False(t, hasResume)
	assert.False(t, hasResumeStream)
	assert.False(t, hasThread)
	assert.False(t, hasWithCheckpointer)
	assert.True(t, hasStream)
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
