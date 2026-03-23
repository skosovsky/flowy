package flowy

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestApplyBuildOptions_DefaultMaxSteps(t *testing.T) {
	o := applyBuildOptions(nil)
	assert.Equal(t, defaultMaxSteps, o.run.maxSteps)
	assert.Zero(t, o.run.nodeTimeout)
	assert.Zero(t, o.run.maxConcurrency)
}

func TestApplyBuildOptions_ZeroOrNegativeMaxSteps_UseDefault(t *testing.T) {
	o0 := applyBuildOptions([]BuildOption{WithMaxSteps(0)})
	assert.Equal(t, defaultMaxSteps, o0.run.maxSteps)
	oNeg := applyBuildOptions([]BuildOption{WithMaxSteps(-1)})
	assert.Equal(t, defaultMaxSteps, oNeg.run.maxSteps)
}

func TestApplyBuildOptions_WithNodeTimeout(t *testing.T) {
	o := applyBuildOptions([]BuildOption{WithNodeTimeout(5 * time.Second)})
	assert.Equal(t, 5*time.Second, o.run.nodeTimeout)
	assert.Equal(t, defaultMaxSteps, o.run.maxSteps)
}

func TestApplyBuildOptions_WithMaxConcurrency(t *testing.T) {
	o := applyBuildOptions([]BuildOption{WithMaxConcurrency(5)})
	assert.Equal(t, 5, o.run.maxConcurrency)
	oZero := applyBuildOptions([]BuildOption{WithMaxConcurrency(0)})
	assert.Zero(t, oZero.run.maxConcurrency)
}

func TestOptions_CompileMaxSteps_Respected(t *testing.T) {
	reducer := func(_, u string) string { return u }
	b := NewGraph[string](reducer)
	b.AddNode("a", func(_ context.Context, s string) (string, error) { return s + "a", nil })
	b.AddNode("b", func(_ context.Context, s string) (string, error) { return s + "b", nil })
	b.AddNode("c", func(_ context.Context, s string) (string, error) { return s, nil })
	b.AddEdge("a", "b")
	b.AddEdge("b", "a")
	b.SetEntryPoint("a")
	b.SetFinishPoint("c")
	graph, err := b.Compile(WithMaxSteps(2))
	require.NoError(t, err)
	ctx := context.Background()
	_, err = graph.Invoke(ctx, "")
	require.Error(t, err)
	require.ErrorIs(t, err, ErrMaxStepsExceeded)
}
