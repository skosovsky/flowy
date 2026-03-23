package testutil

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/skosovsky/flowy/checkpoint"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func TestMemoryCheckpointer_SaveLoadLatestHistory(t *testing.T) {
	cp := NewMemoryCheckpointer()
	ctx := context.Background()

	first := checkpoint.Checkpoint{
		ID:        "cp-1",
		ThreadID:  "thread-1",
		RunID:     "run-1",
		Node:      "a",
		Next:      "b",
		StateData: []byte(`"state-1"`),
		CreatedAt: time.Unix(1, 0).UTC(),
	}
	second := checkpoint.Checkpoint{
		ID:        "cp-2",
		ThreadID:  "thread-1",
		RunID:     "run-1",
		Node:      "b",
		Next:      "",
		StateData: []byte(`"state-2"`),
		CreatedAt: time.Unix(2, 0).UTC(),
	}

	require.NoError(t, cp.Save(ctx, first))
	require.NoError(t, cp.Save(ctx, second))

	latest, err := cp.LoadLatest(ctx, "thread-1")
	require.NoError(t, err)
	assert.Equal(t, second.ID, latest.ID)

	history, err := cp.GetHistory(ctx, "thread-1", 0)
	require.NoError(t, err)
	require.Len(t, history, 2)
	assert.Equal(t, second.ID, history[0].ID)
	assert.Equal(t, first.ID, history[1].ID)

	limited, err := cp.GetHistory(ctx, "thread-1", 1)
	require.NoError(t, err)
	require.Len(t, limited, 1)
	assert.Equal(t, second.ID, limited[0].ID)
}

func TestMemoryCheckpointer_OrdersByCreatedAtNotInsertionOrder(t *testing.T) {
	cp := NewMemoryCheckpointer()
	ctx := context.Background()

	newer := checkpoint.Checkpoint{
		ID:        "cp-newer",
		ThreadID:  "thread-order",
		RunID:     "run-1",
		Node:      "newer",
		Next:      "",
		StateData: []byte(`"newer"`),
		CreatedAt: time.Unix(2, 0).UTC(),
	}
	olderInsertedLater := checkpoint.Checkpoint{
		ID:        "cp-older",
		ThreadID:  "thread-order",
		RunID:     "run-1",
		Node:      "older",
		Next:      "",
		StateData: []byte(`"older"`),
		CreatedAt: time.Unix(1, 0).UTC(),
	}

	require.NoError(t, cp.Save(ctx, newer))
	require.NoError(t, cp.Save(ctx, olderInsertedLater))

	latest, err := cp.LoadLatest(ctx, "thread-order")
	require.NoError(t, err)
	assert.Equal(t, newer.ID, latest.ID)

	history, err := cp.GetHistory(ctx, "thread-order", 1)
	require.NoError(t, err)
	require.Len(t, history, 1)
	assert.Equal(t, newer.ID, history[0].ID)
}

func TestMemoryCheckpointer_UsesIDTieBreakForEqualCreatedAt(t *testing.T) {
	cp := NewMemoryCheckpointer()
	ctx := context.Background()

	createdAt := time.Unix(1, 0).UTC()
	first := checkpoint.Checkpoint{
		ID:        "cp-z",
		ThreadID:  "thread-tie",
		RunID:     "run-1",
		Node:      "a",
		Next:      "",
		StateData: []byte(`"a"`),
		CreatedAt: createdAt,
	}
	second := checkpoint.Checkpoint{
		ID:        "cp-a",
		ThreadID:  "thread-tie",
		RunID:     "run-1",
		Node:      "b",
		Next:      "",
		StateData: []byte(`"b"`),
		CreatedAt: createdAt,
	}

	require.NoError(t, cp.Save(ctx, first))
	require.NoError(t, cp.Save(ctx, second))

	latest, err := cp.LoadLatest(ctx, "thread-tie")
	require.NoError(t, err)
	assert.Equal(t, first.ID, latest.ID)

	history, err := cp.GetHistory(ctx, "thread-tie", 0)
	require.NoError(t, err)
	require.Len(t, history, 2)
	assert.Equal(t, first.ID, history[0].ID)
	assert.Equal(t, second.ID, history[1].ID)
}

func TestMemoryCheckpointer_SaveIsIdempotentByID(t *testing.T) {
	cp := NewMemoryCheckpointer()
	ctx := context.Background()

	checkpoint := checkpoint.Checkpoint{
		ID:        "cp-1",
		ThreadID:  "thread-1",
		RunID:     "run-1",
		Node:      "a",
		Next:      "b",
		StateData: []byte(`"state"`),
		CreatedAt: time.Unix(1, 0).UTC(),
	}

	require.NoError(t, cp.Save(ctx, checkpoint))
	require.NoError(t, cp.Save(ctx, checkpoint))

	history, err := cp.GetHistory(ctx, "thread-1", 0)
	require.NoError(t, err)
	require.Len(t, history, 1)
	assert.Equal(t, checkpoint.ID, history[0].ID)
}

func TestMemoryCheckpointer_NoCheckpoint(t *testing.T) {
	cp := NewMemoryCheckpointer()

	_, err := cp.LoadLatest(context.Background(), "missing")
	require.ErrorIs(t, err, checkpoint.ErrNoCheckpoint)

	_, err = cp.GetHistory(context.Background(), "missing", 0)
	require.ErrorIs(t, err, checkpoint.ErrNoCheckpoint)
}
