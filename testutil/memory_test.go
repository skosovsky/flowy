package testutil

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/skosovsky/flowy"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func TestInMemoryCheckpointer_SaveLoad(t *testing.T) {
	cp := NewInMemoryCheckpointer[string]()
	ctx := context.Background()
	require.NoError(t, cp.Save(ctx, "tid1", flowy.Checkpoint[string]{State: "s1", NodeName: "n1"}))
	require.NoError(t, cp.Save(ctx, "tid2", flowy.Checkpoint[string]{State: "s2", NodeName: "n2"}))

	c1, err := cp.Load(ctx, "tid1")
	require.NoError(t, err)
	assert.Equal(t, "s1", c1.State)
	assert.Equal(t, "n1", c1.NodeName)

	c2, err := cp.Load(ctx, "tid2")
	require.NoError(t, err)
	assert.Equal(t, "s2", c2.State)
}

func TestInMemoryCheckpointer_LoadNotFound(t *testing.T) {
	cp := NewInMemoryCheckpointer[string]()
	ctx := context.Background()
	_, err := cp.Load(ctx, "missing")
	require.Error(t, err)
	assert.ErrorIs(t, err, flowy.ErrThreadNotFound)
}

func TestInMemoryCheckpointer_Concurrent(t *testing.T) {
	t.Parallel()
	cp := NewInMemoryCheckpointer[int]()
	ctx := context.Background()
	var wg sync.WaitGroup
	for i := range 10 {
		wg.Add(1)
		go func(j int) {
			defer wg.Done()
			_ = cp.Save(ctx, "key", flowy.Checkpoint[int]{State: j, NodeName: "n"})
			_, _ = cp.Load(ctx, "key")
		}(i)
	}
	wg.Wait()
}
