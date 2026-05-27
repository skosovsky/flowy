// Package redis provides Redis checkpointer adapter.
package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/skosovsky/flowy"
	"github.com/skosovsky/flowy/checkpoint"
)

const defaultPrefix = "flowy"

// Options configures the Redis checkpointer.
type Options struct {
	Prefix string
	TTL    time.Duration
}

// Checkpointer stores snapshots in Redis list history (latest at index 0).
type Checkpointer[T any] struct {
	client     goredis.Cmdable
	prefix     string
	ttl        time.Duration
	serializer flowy.StateSerializer[T]
}

// NewCheckpointer creates a Redis-backed checkpointer.
func NewCheckpointer[T any](
	client goredis.Cmdable,
	opts Options,
	serializer flowy.StateSerializer[T],
) *Checkpointer[T] {
	prefix := opts.Prefix
	if prefix == "" {
		prefix = defaultPrefix
	}
	return &Checkpointer[T]{
		client:     client,
		prefix:     prefix,
		ttl:        opts.TTL,
		serializer: serializer,
	}
}

func (c *Checkpointer[T]) Save(ctx context.Context, snapshot flowy.Snapshot[T]) error {
	stored, err := checkpoint.EncodeStoredSnapshot(snapshot, c.serializer)
	if err != nil {
		return err
	}
	payload, err := json.Marshal(stored)
	if err != nil {
		return fmt.Errorf("redis checkpoint marshal: %w", err)
	}
	key := c.historyKey(snapshot.ThreadID)
	if err := c.client.LPush(ctx, key, payload).Err(); err != nil {
		return err
	}
	if c.ttl > 0 {
		return c.client.Expire(ctx, key, c.ttl).Err()
	}
	return nil
}

func (c *Checkpointer[T]) Load(ctx context.Context, threadID string) (flowy.Snapshot[T], error) {
	values, err := c.client.LRange(ctx, c.historyKey(threadID), 0, 0).Result()
	if err != nil {
		return flowy.Snapshot[T]{}, err
	}
	if len(values) == 0 {
		return flowy.Snapshot[T]{}, checkpoint.ErrNoSnapshot
	}
	return c.decode(values[0])
}

func (c *Checkpointer[T]) GetHistory(
	ctx context.Context,
	threadID string,
	limit int,
) ([]flowy.Snapshot[T], error) {
	end := int64(-1)
	if limit > 0 {
		end = int64(limit - 1)
	}
	values, err := c.client.LRange(ctx, c.historyKey(threadID), 0, end).Result()
	if err != nil {
		return nil, err
	}
	out := make([]flowy.Snapshot[T], 0, len(values))
	for _, item := range values {
		snapshot, decodeErr := c.decode(item)
		if decodeErr != nil {
			return nil, decodeErr
		}
		out = append(out, snapshot)
	}
	return out, nil
}

func (c *Checkpointer[T]) Prune(ctx context.Context, threadID string, retainCount int) error {
	key := c.historyKey(threadID)
	if retainCount <= 0 {
		return c.client.Del(ctx, key).Err()
	}
	return c.client.LTrim(ctx, key, 0, int64(retainCount-1)).Err()
}

func (c *Checkpointer[T]) decode(raw string) (flowy.Snapshot[T], error) {
	var stored checkpoint.StoredSnapshot
	if err := json.Unmarshal([]byte(raw), &stored); err != nil {
		return flowy.Snapshot[T]{}, fmt.Errorf("redis checkpoint unmarshal: %w", err)
	}
	snapshot, err := checkpoint.DecodeStoredSnapshot(stored, c.serializer)
	if err != nil {
		return flowy.Snapshot[T]{}, err
	}
	return snapshot, nil
}

func (c *Checkpointer[T]) historyKey(threadID string) string {
	return fmt.Sprintf("%s:thread:%s:history", c.prefix, threadID)
}

var _ flowy.Checkpointer[any] = (*Checkpointer[any])(nil)
