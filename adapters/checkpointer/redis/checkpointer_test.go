package redis

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/skosovsky/flowy/checkpoint"
)

func TestCheckpointer_SaveLoadLatestHistory(t *testing.T) {
	cp, mini := newCheckpointer(t, Options{})
	_ = mini

	first := checkpoint.Checkpoint{
		ID:        "cp-1",
		ThreadID:  "thread-1",
		RunID:     "run-1",
		Node:      "a",
		Next:      "b",
		StateData: []byte(`{"step":1}`),
		CreatedAt: time.Unix(1, 0).UTC(),
	}
	second := checkpoint.Checkpoint{
		ID:        "cp-2",
		ThreadID:  "thread-1",
		RunID:     "run-1",
		Node:      "b",
		Next:      "",
		StateData: []byte(`{"step":2}`),
		CreatedAt: time.Unix(2, 0).UTC(),
	}

	require.NoError(t, cp.Save(context.Background(), first))
	require.NoError(t, cp.Save(context.Background(), second))

	latest, err := cp.LoadLatest(context.Background(), "thread-1")
	require.NoError(t, err)
	assert.Equal(t, second.ID, latest.ID)

	history, err := cp.GetHistory(context.Background(), "thread-1", 0)
	require.NoError(t, err)
	require.Len(t, history, 2)
	assert.Equal(t, second.ID, history[0].ID)
	assert.Equal(t, first.ID, history[1].ID)
}

func TestCheckpointer_SaveIsIdempotentByID(t *testing.T) {
	cp, mini := newCheckpointer(t, Options{})
	_ = mini

	checkpoint := checkpoint.Checkpoint{
		ID:        "cp-1",
		ThreadID:  "thread-2",
		RunID:     "run-2",
		Node:      "approve",
		Next:      "approve",
		StateData: []byte(`{"approved":false}`),
		CreatedAt: time.Unix(3, 0).UTC(),
	}

	require.NoError(t, cp.Save(context.Background(), checkpoint))
	require.NoError(t, cp.Save(context.Background(), checkpoint))

	history, err := cp.GetHistory(context.Background(), "thread-2", 0)
	require.NoError(t, err)
	require.Len(t, history, 1)
	assert.Equal(t, checkpoint.ID, history[0].ID)
}

func TestCheckpointer_TTL(t *testing.T) {
	cp, mini := newCheckpointer(t, Options{TTL: time.Minute})

	checkpoint := checkpoint.Checkpoint{
		ID:        "cp-ttl",
		ThreadID:  "thread-ttl",
		RunID:     "run-ttl",
		Node:      "node",
		Next:      "",
		StateData: []byte(`{"ok":true}`),
		CreatedAt: time.Unix(4, 0).UTC(),
	}

	require.NoError(t, cp.Save(context.Background(), checkpoint))

	assert.Equal(t, time.Minute, mini.TTL(cp.streamKey("thread-ttl")))
	assert.Equal(t, time.Minute, mini.TTL(cp.idsKey("thread-ttl")))
	assert.Equal(t, time.Minute, mini.TTL(cp.orderKey("thread-ttl")))
	assert.Equal(t, time.Minute, mini.TTL(cp.sequenceKey("thread-ttl")))
	assert.Equal(t, time.Minute, mini.TTL(cp.lastMSKey("thread-ttl")))
}

func TestCheckpointer_NoCheckpoint(t *testing.T) {
	cp, mini := newCheckpointer(t, Options{})
	_ = mini

	_, err := cp.LoadLatest(context.Background(), "missing")
	require.ErrorIs(t, err, checkpoint.ErrNoCheckpoint)

	_, err = cp.GetHistory(context.Background(), "missing", 0)
	require.ErrorIs(t, err, checkpoint.ErrNoCheckpoint)
}

func TestCheckpointer_HistoryFollowsCreatedAtNotInsertionOrder(t *testing.T) {
	cp, mini := newCheckpointer(t, Options{})
	_ = mini

	newer := checkpoint.Checkpoint{
		ID:        "cp-newer",
		ThreadID:  "thread-order",
		RunID:     "run-order",
		Node:      "newer",
		Next:      "",
		StateData: []byte(`{"step":"newer"}`),
		CreatedAt: time.Unix(20, 0).UTC(),
	}
	olderInsertedLater := checkpoint.Checkpoint{
		ID:        "cp-older",
		ThreadID:  "thread-order",
		RunID:     "run-order",
		Node:      "older",
		Next:      "",
		StateData: []byte(`{"step":"older"}`),
		CreatedAt: time.Unix(10, 0).UTC(),
	}

	require.NoError(t, cp.Save(context.Background(), newer))
	require.NoError(t, cp.Save(context.Background(), olderInsertedLater))

	latest, err := cp.LoadLatest(context.Background(), "thread-order")
	require.NoError(t, err)
	assert.Equal(t, newer.ID, latest.ID)

	history, err := cp.GetHistory(context.Background(), "thread-order", 0)
	require.NoError(t, err)
	require.Len(t, history, 2)
	assert.Equal(t, newer.ID, history[0].ID)
	assert.Equal(t, olderInsertedLater.ID, history[1].ID)
}

func TestCheckpointer_HistoryUsesIDTieBreakForSameCreatedAt(t *testing.T) {
	cp, mini := newCheckpointer(t, Options{})
	_ = mini

	createdAt := time.Unix(20, 0).UTC()
	first := checkpoint.Checkpoint{
		ID:        "cp-z",
		ThreadID:  "thread-same-created-at",
		RunID:     "run-order",
		Node:      "first",
		Next:      "",
		StateData: []byte(`{"step":"first"}`),
		CreatedAt: createdAt,
	}
	second := checkpoint.Checkpoint{
		ID:        "cp-a",
		ThreadID:  "thread-same-created-at",
		RunID:     "run-order",
		Node:      "second",
		Next:      "",
		StateData: []byte(`{"step":"second"}`),
		CreatedAt: createdAt,
	}

	require.NoError(t, cp.Save(context.Background(), first))
	require.NoError(t, cp.Save(context.Background(), second))

	latest, err := cp.LoadLatest(context.Background(), "thread-same-created-at")
	require.NoError(t, err)
	assert.Equal(t, first.ID, latest.ID)

	history, err := cp.GetHistory(context.Background(), "thread-same-created-at", 0)
	require.NoError(t, err)
	require.Len(t, history, 2)
	assert.Equal(t, first.ID, history[0].ID)
	assert.Equal(t, second.ID, history[1].ID)
}

func TestCheckpointer_HistoryFollowsNanosecondCreatedAtWithinSameMillisecond(t *testing.T) {
	cp, mini := newCheckpointer(t, Options{})
	_ = mini

	base := time.Date(2026, time.March, 22, 14, 30, 45, 123_000_000, time.UTC)
	newer := checkpoint.Checkpoint{
		ID:        "cp-newer-ns",
		ThreadID:  "thread-ns-order",
		RunID:     "run-order",
		Node:      "newer",
		Next:      "",
		StateData: []byte(`{"step":"newer"}`),
		CreatedAt: base.Add(900 * time.Nanosecond),
	}
	olderInsertedLater := checkpoint.Checkpoint{
		ID:        "cp-older-ns",
		ThreadID:  "thread-ns-order",
		RunID:     "run-order",
		Node:      "older",
		Next:      "",
		StateData: []byte(`{"step":"older"}`),
		CreatedAt: base.Add(100 * time.Nanosecond),
	}

	require.NoError(t, cp.Save(context.Background(), newer))
	require.NoError(t, cp.Save(context.Background(), olderInsertedLater))

	latest, err := cp.LoadLatest(context.Background(), "thread-ns-order")
	require.NoError(t, err)
	assert.Equal(t, newer.ID, latest.ID)

	history, err := cp.GetHistory(context.Background(), "thread-ns-order", 0)
	require.NoError(t, err)
	require.Len(t, history, 2)
	assert.Equal(t, newer.ID, history[0].ID)
	assert.Equal(t, olderInsertedLater.ID, history[1].ID)
}

func TestFormatUnixNanoOrderKey_PadsRealisticTimestamp(t *testing.T) {
	ts := time.Date(2026, time.March, 22, 14, 30, 45, 123_456_789, time.UTC)
	assert.Equal(t, "01774189845123456789", formatUnixNanoOrderKey(ts))
}

func TestCheckpointer_LoadLatest_UsesBoundedReadPath(t *testing.T) {
	cp, recorder := newCheckpointerWithRecorder(t, Options{})

	first := checkpoint.Checkpoint{
		ID:        "cp-1",
		ThreadID:  "thread-bounded-latest",
		RunID:     "run-1",
		Node:      "a",
		Next:      "b",
		StateData: []byte(`{"step":1}`),
		CreatedAt: time.Unix(1, 0).UTC(),
	}
	second := checkpoint.Checkpoint{
		ID:        "cp-2",
		ThreadID:  "thread-bounded-latest",
		RunID:     "run-1",
		Node:      "b",
		Next:      "",
		StateData: []byte(`{"step":2}`),
		CreatedAt: time.Unix(2, 0).UTC(),
	}

	require.NoError(t, cp.Save(context.Background(), first))
	require.NoError(t, cp.Save(context.Background(), second))

	recorder.Reset()
	latest, err := cp.LoadLatest(context.Background(), "thread-bounded-latest")
	require.NoError(t, err)
	assert.Equal(t, second.ID, latest.ID)
	assert.True(t, recorder.HasCommand("zrevrange"))
	assert.False(t, recorder.HasFullStreamScan(cp.streamKey("thread-bounded-latest")))
}

func TestCheckpointer_GetHistoryLimit_UsesBoundedReadPath(t *testing.T) {
	cp, recorder := newCheckpointerWithRecorder(t, Options{})

	for i := range 3 {
		checkpoint := checkpoint.Checkpoint{
			ID:        fmt.Sprintf("cp-%d", i),
			ThreadID:  "thread-bounded-history",
			RunID:     "run-1",
			Node:      fmt.Sprintf("node-%d", i),
			Next:      "",
			StateData: fmt.Appendf(nil, `{"step":%d}`, i),
			CreatedAt: time.Unix(int64(i+1), 0).UTC(),
		}
		require.NoError(t, cp.Save(context.Background(), checkpoint))
	}

	recorder.Reset()
	history, err := cp.GetHistory(context.Background(), "thread-bounded-history", 1)
	require.NoError(t, err)
	require.Len(t, history, 1)
	assert.True(t, recorder.HasCommand("zrevrange"))
	assert.False(t, recorder.HasFullStreamScan(cp.streamKey("thread-bounded-history")))
}

func newCheckpointer(t *testing.T, opts Options) (*Checkpointer, *miniredis.Miniredis) {
	t.Helper()

	mini := miniredis.RunT(t)
	client := goredis.NewClient(&goredis.Options{Addr: mini.Addr()})
	t.Cleanup(func() {
		_ = client.Close()
		mini.Close()
	})

	cp := New(client, opts)
	return cp, mini
}

func newCheckpointerWithRecorder(t *testing.T, opts Options) (*Checkpointer, *commandRecorder) {
	t.Helper()

	mini := miniredis.RunT(t)
	client := goredis.NewClient(&goredis.Options{Addr: mini.Addr()})
	recorder := &commandRecorder{}
	client.AddHook(recorder)
	t.Cleanup(func() {
		_ = client.Close()
		mini.Close()
	})

	cp := New(client, opts)
	return cp, recorder
}

type commandRecorder struct {
	mu       sync.Mutex
	commands []recordedCommand
}

type recordedCommand struct {
	name string
	args []any
}

func (r *commandRecorder) DialHook(next goredis.DialHook) goredis.DialHook {
	return next
}

func (r *commandRecorder) ProcessHook(next goredis.ProcessHook) goredis.ProcessHook {
	return func(ctx context.Context, cmd goredis.Cmder) error {
		r.record(cmd)
		return next(ctx, cmd)
	}
}

func (r *commandRecorder) ProcessPipelineHook(next goredis.ProcessPipelineHook) goredis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []goredis.Cmder) error {
		for _, cmd := range cmds {
			r.record(cmd)
		}
		return next(ctx, cmds)
	}
}

func (r *commandRecorder) Reset() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.commands = nil
}

func (r *commandRecorder) HasCommand(name string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, command := range r.commands {
		if command.name == strings.ToLower(name) {
			return true
		}
	}
	return false
}

func (r *commandRecorder) HasFullStreamScan(streamKey string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, command := range r.commands {
		if command.name != "xrange" || len(command.args) < 4 {
			continue
		}
		key := fmt.Sprint(command.args[1])
		start := fmt.Sprint(command.args[2])
		end := fmt.Sprint(command.args[3])
		if key == streamKey && start == "-" && end == "+" {
			return true
		}
	}
	return false
}

func (r *commandRecorder) record(cmd goredis.Cmder) {
	args := cmd.Args()
	if len(args) == 0 {
		return
	}

	record := recordedCommand{
		name: strings.ToLower(fmt.Sprint(args[0])),
		args: append([]any(nil), args...),
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	r.commands = append(r.commands, record)
}
