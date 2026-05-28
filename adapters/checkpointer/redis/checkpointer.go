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
type Checkpointer[T, E any] struct {
	client     goredis.Cmdable
	prefix     string
	ttl        time.Duration
	serializer flowy.StateSerializer[T]
}

// NewCheckpointer creates a Redis-backed checkpointer.
func NewCheckpointer[T, E any](
	client goredis.Cmdable,
	opts Options,
	serializer flowy.StateSerializer[T],
) *Checkpointer[T, E] {
	prefix := opts.Prefix
	if prefix == "" {
		prefix = defaultPrefix
	}
	return &Checkpointer[T, E]{
		client:     client,
		prefix:     prefix,
		ttl:        opts.TTL,
		serializer: serializer,
	}
}

func (c *Checkpointer[T, E]) Save(ctx context.Context, snapshot flowy.Snapshot[T, E]) error {
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

func (c *Checkpointer[T, E]) Load(ctx context.Context, threadID string) (flowy.Snapshot[T, E], error) {
	values, err := c.client.LRange(ctx, c.historyKey(threadID), 0, 0).Result()
	if err != nil {
		return flowy.Snapshot[T, E]{}, err
	}
	if len(values) == 0 {
		return flowy.Snapshot[T, E]{}, checkpoint.ErrNoSnapshot
	}
	return c.decode(values[0])
}

func (c *Checkpointer[T, E]) GetHistory(
	ctx context.Context,
	threadID string,
	limit int,
) ([]flowy.Snapshot[T, E], error) {
	end := int64(-1)
	if limit > 0 {
		end = int64(limit - 1)
	}
	values, err := c.client.LRange(ctx, c.historyKey(threadID), 0, end).Result()
	if err != nil {
		return nil, err
	}
	out := make([]flowy.Snapshot[T, E], 0, len(values))
	for _, item := range values {
		snapshot, decodeErr := c.decode(item)
		if decodeErr != nil {
			return nil, decodeErr
		}
		out = append(out, snapshot)
	}
	return out, nil
}

func (c *Checkpointer[T, E]) Prune(ctx context.Context, threadID string, retainCount int) error {
	key := c.historyKey(threadID)
	if retainCount <= 0 {
		return c.client.Del(ctx, key).Err()
	}
	return c.client.LTrim(ctx, key, 0, int64(retainCount-1)).Err()
}

func (c *Checkpointer[T, E]) Delete(ctx context.Context, threadID string) error {
	return c.client.Del(ctx, c.historyKey(threadID)).Err()
}

func (c *Checkpointer[T, E]) decode(raw string) (flowy.Snapshot[T, E], error) {
	var stored checkpoint.StoredSnapshot
	if err := json.Unmarshal([]byte(raw), &stored); err != nil {
		return flowy.Snapshot[T, E]{}, fmt.Errorf("redis checkpoint unmarshal: %w", err)
	}
	snapshot, err := checkpoint.DecodeStoredSnapshot[T, E](stored, c.serializer)
	if err != nil {
		return flowy.Snapshot[T, E]{}, err
	}
	return snapshot, nil
}

func (c *Checkpointer[T, E]) historyKey(threadID string) string {
	return fmt.Sprintf("%s:thread:%s:history", c.prefix, threadID)
}

var _ flowy.Checkpointer[any, any] = (*Checkpointer[any, any])(nil)
