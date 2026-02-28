package flowy

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestApplyOptions_NilDefault_UsesZeroAndDefaultMaxSteps(t *testing.T) {
	cfg := applyOptions[string](nil, nil)
	assert.Equal(t, defaultMaxSteps, cfg.maxSteps)
	assert.Zero(t, cfg.nodeTimeout)
	assert.Empty(t, cfg.threadID)
	assert.Nil(t, cfg.checkpointer)
}

func TestApplyOptions_ZeroOrNegativeMaxSteps_UseDefault(t *testing.T) {
	cfg0 := applyOptions[string](nil, []Option[string]{WithMaxSteps[string](0)})
	assert.Equal(t, defaultMaxSteps, cfg0.maxSteps)
	cfgNeg := applyOptions[string](nil, []Option[string]{WithMaxSteps[string](-1)})
	assert.Equal(t, defaultMaxSteps, cfgNeg.maxSteps)
}

func TestApplyOptions_CompileDefaults_OverriddenByInvoke(t *testing.T) {
	defaultCfg := runConfig[string]{maxSteps: 10, threadID: "tid"}
	opts := []Option[string]{WithMaxSteps[string](3)}
	cfg := applyOptions(&defaultCfg, opts)
	assert.Equal(t, 3, cfg.maxSteps)
	assert.Equal(t, "tid", cfg.threadID)
}

func TestApplyOptions_WithThreadID_WithNodeTimeout(t *testing.T) {
	cfg := applyOptions[string](nil, []Option[string]{
		WithThreadID[string]("session_1"),
		WithNodeTimeout[string](5 * time.Second),
	})
	assert.Equal(t, "session_1", cfg.threadID)
	assert.Equal(t, 5*time.Second, cfg.nodeTimeout)
	assert.Equal(t, defaultMaxSteps, cfg.maxSteps)
}

func TestApplyOptions_WithMaxConcurrency(t *testing.T) {
	cfg := applyOptions[string](nil, []Option[string]{WithMaxConcurrency[string](5)})
	assert.Equal(t, 5, cfg.maxConcurrency)
	cfgZero := applyOptions[string](nil, []Option[string]{WithMaxConcurrency[string](0)})
	assert.Equal(t, 0, cfgZero.maxConcurrency)
	defaultCfg := runConfig[string]{maxConcurrency: 3}
	cfgOverride := applyOptions(&defaultCfg, []Option[string]{WithMaxConcurrency[string](2)})
	assert.Equal(t, 2, cfgOverride.maxConcurrency)
}

func TestOptions_InvokeRespectsPerCallMaxSteps(t *testing.T) {
	b := NewGraph[string](idReducer[string])
	b.AddNode("a", func(_ context.Context, s string) (string, error) { return s + "a", nil })
	b.AddNode("b", func(_ context.Context, s string) (string, error) { return s + "b", nil })
	b.AddNode("c", noopNode)
	b.AddEdge("a", "b")
	b.AddEdge("b", "a")
	b.SetEntryPoint("a")
	b.SetFinishPoint("c")
	graph, err := b.Compile(WithMaxSteps[string](100))
	require.NoError(t, err)
	ctx := context.Background()
	_, err = graph.Invoke(ctx, "", WithMaxSteps[string](2))
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrMaxStepsExceeded)
}
