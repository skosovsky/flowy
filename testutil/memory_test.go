package testutil

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func TestStore_SaveLoad(t *testing.T) {
	s := NewStore[string]()
	ctx := context.Background()
	require.NoError(t, s.Save(ctx, "k1", "s1", "n1"))
	require.NoError(t, s.Save(ctx, "k2", "s2", "n2"))

	state, startNode, ok := s.Load(ctx, "k1")
	require.True(t, ok)
	assert.Equal(t, "s1", state)
	assert.Equal(t, "n1", startNode)

	state, startNode, ok = s.Load(ctx, "k2")
	require.True(t, ok)
	assert.Equal(t, "s2", state)
	assert.Equal(t, "n2", startNode)
}

func TestStore_LoadNotFound(t *testing.T) {
	s := NewStore[string]()
	ctx := context.Background()
	_, _, ok := s.Load(ctx, "missing")
	assert.False(t, ok)
}

func TestStore_SaveEmptyStartNode(t *testing.T) {
	s := NewStore[string]()
	ctx := context.Background()
	require.NoError(t, s.Save(ctx, "k", "state", ""))
	state, startNode, ok := s.Load(ctx, "k")
	require.True(t, ok)
	assert.Equal(t, "state", state)
	assert.Empty(t, startNode)
}

func TestStore_Concurrent(t *testing.T) {
	t.Parallel()
	s := NewStore[int]()
	ctx := context.Background()
	var wg sync.WaitGroup
	for i := range 10 {
		wg.Add(1)
		go func(j int) {
			defer wg.Done()
			_ = s.Save(ctx, "key", j, "n")
			_, _, _ = s.Load(ctx, "key")
		}(i)
	}
	wg.Wait()
}
