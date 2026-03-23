package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/skosovsky/flowy"
	"github.com/skosovsky/flowy/checkpoint"
)

type binaryState struct {
	Value string
}

type binaryStateSerializer struct{}

func (binaryStateSerializer) Marshal(state binaryState) ([]byte, error) {
	return []byte("bin:" + state.Value), nil
}

func (binaryStateSerializer) Unmarshal(data []byte, state *binaryState) error {
	payload := string(data)
	if !strings.HasPrefix(payload, "bin:") {
		return fmt.Errorf("unexpected payload %q", payload)
	}
	state.Value = strings.TrimPrefix(payload, "bin:")
	return nil
}

func TestCheckpointer_SaveLoadLatestHistory(t *testing.T) {
	cp := New(newTestPool(t))
	ctx := context.Background()

	first := checkpoint.Checkpoint{
		ID:        "00000000-0000-7000-8000-000000000001",
		ThreadID:  "thread-1",
		RunID:     "run-1",
		Node:      "start",
		Next:      "finish",
		StateData: []byte(`{"step":1}`),
		CreatedAt: time.Unix(1, 0).UTC(),
	}
	second := checkpoint.Checkpoint{
		ID:        "00000000-0000-7000-8000-000000000002",
		ThreadID:  "thread-1",
		RunID:     "run-1",
		Node:      "finish",
		Next:      "",
		StateData: []byte(`{"step":2}`),
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
}

func TestCheckpointer_SaveIsIdempotentByID(t *testing.T) {
	cp := New(newTestPool(t))
	ctx := context.Background()

	checkpoint := checkpoint.Checkpoint{
		ID:        "00000000-0000-7000-8000-000000000003",
		ThreadID:  "thread-2",
		RunID:     "run-2",
		Node:      "approve",
		Next:      "approve",
		StateData: []byte(`{"approved":false}`),
		CreatedAt: time.Unix(3, 0).UTC(),
	}

	require.NoError(t, cp.Save(ctx, checkpoint))
	require.NoError(t, cp.Save(ctx, checkpoint))

	history, err := cp.GetHistory(ctx, "thread-2", 0)
	require.NoError(t, err)
	require.Len(t, history, 1)
	assert.Equal(t, checkpoint.ID, history[0].ID)
}

func TestCheckpointer_UsesIDTieBreakForEqualCreatedAt(t *testing.T) {
	cp := New(newTestPool(t))
	ctx := context.Background()

	createdAt := time.Unix(5, 0).UTC()
	first := checkpoint.Checkpoint{
		ID:        "00000000-0000-7000-8000-0000000000ff",
		ThreadID:  "thread-tie",
		RunID:     "run-tie",
		Node:      "first",
		Next:      "",
		StateData: []byte(`{"step":"first"}`),
		CreatedAt: createdAt,
	}
	second := checkpoint.Checkpoint{
		ID:        "00000000-0000-7000-8000-000000000001",
		ThreadID:  "thread-tie",
		RunID:     "run-tie",
		Node:      "second",
		Next:      "",
		StateData: []byte(`{"step":"second"}`),
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

func TestCheckpointer_CoreEnvelopeRoundTripBinarySerializer(t *testing.T) {
	cp := New(newTestPool(t))
	ctx := context.Background()

	b := flowy.NewGraph[binaryState](func(_, update binaryState) binaryState { return update })
	b.AddNode("finish", func(_ context.Context, s binaryState) (binaryState, error) {
		s.Value += "_done"
		return s, nil
	})
	b.SetEntryPoint("finish")
	b.SetFinishPoint("finish")

	graph, err := b.Compile()
	require.NoError(t, err)

	ser := binaryStateSerializer{}
	var final binaryState
	for step, streamErr := range graph.Stream(ctx, "", binaryState{Value: "init"}) {
		require.NoError(t, streamErr)
		raw, mErr := ser.Marshal(step.State)
		require.NoError(t, mErr)
		env, encErr := checkpoint.EncodeStateData(raw)
		require.NoError(t, encErr)
		id, idErr := checkpoint.NewSortableID()
		require.NoError(t, idErr)
		require.NoError(t, cp.Save(ctx, checkpoint.Checkpoint{
			ID:        id,
			ThreadID:  "thread-envelope",
			RunID:     "run-1",
			Node:      step.NodeName,
			Next:      step.NextNode,
			StateData: env,
			CreatedAt: time.Now().UTC(),
		}))
		final = step.State
	}
	assert.Equal(t, binaryState{Value: "init_done"}, final)

	latest, err := cp.LoadLatest(ctx, "thread-envelope")
	require.NoError(t, err)
	assert.True(t, json.Valid(latest.StateData))

	payload, err := checkpoint.DecodeStateData(latest.StateData)
	require.NoError(t, err)
	var loaded binaryState
	require.NoError(t, ser.Unmarshal(payload, &loaded))
	assert.Equal(t, final, loaded)
}

func TestCheckpointer_NoCheckpoint(t *testing.T) {
	cp := New(newTestPool(t))

	_, err := cp.LoadLatest(context.Background(), "missing")
	require.ErrorIs(t, err, checkpoint.ErrNoCheckpoint)

	_, err = cp.GetHistory(context.Background(), "missing", 0)
	require.ErrorIs(t, err, checkpoint.ErrNoCheckpoint)
}

func newTestPool(t *testing.T) *pgxpool.Pool {
	t.Helper()

	ctx := context.Background()
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "postgres:16-alpine",
			ExposedPorts: []string{"5432/tcp"},
			Env: map[string]string{
				"POSTGRES_DB":       "flowy",
				"POSTGRES_USER":     "postgres",
				"POSTGRES_PASSWORD": "postgres",
			},
			WaitingFor: wait.ForLog("database system is ready to accept connections").WithOccurrence(2),
		},
		Started: true,
	})
	if err != nil {
		t.Skipf("docker unavailable: %v", err)
	}

	t.Cleanup(func() {
		_ = container.Terminate(ctx)
	})

	host, err := container.Host(ctx)
	require.NoError(t, err)

	port, err := container.MappedPort(ctx, "5432/tcp")
	require.NoError(t, err)

	dsn := "postgres://postgres:postgres@" + net.JoinHostPort(host, port.Port()) + "/flowy?sslmode=disable"

	var pool *pgxpool.Pool
	require.Eventually(t, func() bool {
		if pool != nil {
			pool.Close()
		}

		candidate, poolErr := pgxpool.New(ctx, dsn)
		if poolErr != nil {
			return false
		}
		if pingErr := candidate.Ping(ctx); pingErr != nil {
			candidate.Close()
			return false
		}
		pool = candidate
		return true
	}, 20*time.Second, 250*time.Millisecond)

	t.Cleanup(pool.Close)

	_, err = pool.Exec(ctx, SchemaSQL())
	require.NoError(t, err)

	return pool
}
