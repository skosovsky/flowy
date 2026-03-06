package flowy

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestApplyBuildOptions_DefaultMaxSteps(t *testing.T) {
	cfg := applyBuildOptions(nil)
	assert.Equal(t, defaultMaxSteps, cfg.maxSteps)
	assert.Zero(t, cfg.nodeTimeout)
	assert.Zero(t, cfg.maxConcurrency)
}

func TestApplyBuildOptions_ZeroOrNegativeMaxSteps_UseDefault(t *testing.T) {
	cfg0 := applyBuildOptions([]BuildOption{WithMaxSteps(0)})
	assert.Equal(t, defaultMaxSteps, cfg0.maxSteps)
	cfgNeg := applyBuildOptions([]BuildOption{WithMaxSteps(-1)})
	assert.Equal(t, defaultMaxSteps, cfgNeg.maxSteps)
}

func TestApplyBuildOptions_WithNodeTimeout(t *testing.T) {
	cfg := applyBuildOptions([]BuildOption{WithNodeTimeout(5 * time.Second)})
	assert.Equal(t, 5*time.Second, cfg.nodeTimeout)
	assert.Equal(t, defaultMaxSteps, cfg.maxSteps)
}

func TestApplyBuildOptions_WithMaxConcurrency(t *testing.T) {
	cfg := applyBuildOptions([]BuildOption{WithMaxConcurrency(5)})
	assert.Equal(t, 5, cfg.maxConcurrency)
	cfgZero := applyBuildOptions([]BuildOption{WithMaxConcurrency(0)})
	assert.Zero(t, cfgZero.maxConcurrency)
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
	_, _, err = graph.Invoke(ctx, "")
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrMaxStepsExceeded)
}
