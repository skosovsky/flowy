package flowy_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/skosovsky/flowy"
	"github.com/skosovsky/flowy/checkpoint"
	"github.com/skosovsky/flowy/testutil"
)

// persistStringStep saves one step using the checkpoint envelope (same shape as former core persistence).
func persistStringStep(
	ctx context.Context,
	cp *testutil.MemoryCheckpointer,
	threadID, runID string,
	step flowy.Step[string],
	ser checkpoint.JSONSerializer[string],
) error {
	raw, err := ser.Marshal(step.State)
	if err != nil {
		return err
	}
	env, err := checkpoint.EncodeStateData(raw)
	if err != nil {
		return err
	}
	id, err := checkpoint.NewSortableID()
	if err != nil {
		return err
	}
	return cp.Save(ctx, checkpoint.Checkpoint{
		ID:        id,
		ThreadID:  threadID,
		RunID:     runID,
		Node:      step.NodeName,
		Next:      step.NextNode,
		StateData: env,
		CreatedAt: time.Now().UTC(),
	})
}

func TestThreadInvoke_HITL_LoadLatest_Resume(t *testing.T) {
	cp := testutil.NewMemoryCheckpointer()
	approved := false

	b := flowy.NewGraph[string](idReducer[string])
	b.AddNode("process", func(_ context.Context, s string) (string, error) { return s + "_process", nil })
	b.AddNode("approve", func(_ context.Context, s string) (string, error) {
		if !approved {
			return s, flowy.ErrSuspend
		}
		return s + "_approve", nil
	})
	b.AddNode("finish", func(_ context.Context, s string) (string, error) { return s + "_finish", nil })
	b.AddEdge("process", "approve")
	b.AddEdge("approve", "finish")
	b.SetEntryPoint("process")
	b.SetFinishPoint("finish")

	graph, err := b.Compile()
	require.NoError(t, err)

	ctx := context.Background()
	const threadID = "thread-1"
	runID, err := checkpoint.NewSortableID()
	require.NoError(t, err)
	ser := checkpoint.JSONSerializer[string]{}

	var state string
	for step, err := range graph.Stream(ctx, "", "init") {
		if err != nil {
			if errors.Is(err, flowy.ErrSuspend) {
				assert.Equal(t, "approve", step.NodeName)
				assert.Equal(t, "approve", step.NextNode)
				assert.Equal(t, "init_process", step.State)
				require.NoError(t, persistStringStep(ctx, cp, threadID, runID, step, ser))
				break
			}
			t.Fatalf("unexpected err: %v", err)
		}
		require.NoError(t, persistStringStep(ctx, cp, threadID, runID, step, ser))
		state = step.State
	}

	loaded, err := cp.LoadLatest(ctx, threadID)
	require.NoError(t, err)
	assert.Equal(t, "approve", loaded.Node)
	assert.Equal(t, "approve", loaded.Next)

	approved = true
	raw, err := checkpoint.DecodeStateData(loaded.StateData)
	require.NoError(t, err)
	require.NoError(t, ser.Unmarshal(raw, &state))

	var final string
	for step, err := range graph.Stream(ctx, loaded.Next, state) {
		if err != nil {
			t.Fatalf("unexpected err: %v", err)
		}
		require.NoError(t, persistStringStep(ctx, cp, threadID, runID, step, ser))
		final = step.State
	}
	assert.Equal(t, "init_process_approve_finish", final)

	history, err := cp.GetHistory(ctx, threadID, 0)
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(history), 2)
}

func TestThreadLoadLatest_NoCheckpoint(t *testing.T) {
	cp := testutil.NewMemoryCheckpointer()
	_, err := cp.LoadLatest(context.Background(), "missing")
	require.ErrorIs(t, err, checkpoint.ErrNoCheckpoint)
}

func TestCheckpointHistory_PublicSteps(t *testing.T) {
	tests := []struct {
		name      string
		build     func(*flowy.GraphBuilder[string])
		wantNodes []string
		wantNexts []string
	}{
		{
			name: "sequential",
			build: func(b *flowy.GraphBuilder[string]) {
				b.AddNode("a", func(_ context.Context, s string) (string, error) { return s + "a", nil })
				b.AddNode("b", func(_ context.Context, s string) (string, error) { return s + "b", nil })
				b.AddEdge("a", "b")
				b.SetEntryPoint("a")
				b.SetFinishPoint("b")
			},
			wantNodes: []string{"b", "a"},
			wantNexts: []string{"", "b"},
		},
		{
			name: "conditional",
			build: func(b *flowy.GraphBuilder[string]) {
				b.AddNode("start", func(_ context.Context, s string) (string, error) { return s + "[start]", nil })
				b.AddNode("left", func(_ context.Context, s string) (string, error) { return s + "[left]", nil })
				b.AddChoice("start", func(_ context.Context, _ string) (string, error) { return "left", nil })
				b.SetEntryPoint("start")
				b.SetFinishPoint("left")
			},
			wantNodes: []string{"left", "start"},
			wantNexts: []string{"", "left"},
		},
		{
			name: "linear_route_merge",
			build: func(b *flowy.GraphBuilder[string]) {
				// Same Stream step shape as route + join (two top-level nodes); checkpoint tests persistence, not concurrency.
				b.AddNode("route", func(_ context.Context, s string) (string, error) { return s + "[route]", nil })
				b.AddNode("merge", func(_ context.Context, s string) (string, error) { return s + "[merge]", nil })
				b.AddEdge("route", "merge")
				b.SetEntryPoint("route")
				b.SetFinishPoint("merge")
			},
			wantNodes: []string{"merge", "route"},
			wantNexts: []string{"", "merge"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cp := testutil.NewMemoryCheckpointer()
			b := flowy.NewGraph[string](idReducer[string])
			tc.build(b)

			graph, err := b.Compile()
			require.NoError(t, err)
			ctx := context.Background()
			runID, err := checkpoint.NewSortableID()
			require.NoError(t, err)
			ser := checkpoint.JSONSerializer[string]{}

			for step, err := range graph.Stream(ctx, "", "") {
				require.NoError(t, err)
				require.NoError(t, persistStringStep(ctx, cp, "thread-"+tc.name, runID, step, ser))
			}

			history, err := cp.GetHistory(ctx, "thread-"+tc.name, 0)
			require.NoError(t, err)
			require.Len(t, history, len(tc.wantNodes))

			for i := range tc.wantNodes {
				assert.Equal(t, tc.wantNodes[i], history[i].Node)
				assert.Equal(t, tc.wantNexts[i], history[i].Next)
			}
		})
	}
}
