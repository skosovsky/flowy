// Package redis provides Redis checkpointer adapter.
//
// Handoff transactional outbox (SaveWithOutbox) is not supported — use the 3-phase Handoff FSM
// with RecoverStaleHandoff for stale pending. Postgres adapter implements TransactionalCheckpointer.
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

const saveOccScript = `
local head = redis.call('LINDEX', KEYS[1], 0)
local current = 0
if head then
  local ok, stored = pcall(cjson.decode, head)
  if ok and stored.revision then
    current = tonumber(stored.revision)
  end
end
local expected = tonumber(ARGV[1])
if current ~= expected then
  return 0
end
redis.call('LPUSH', KEYS[1], ARGV[2])
if tonumber(ARGV[3]) > 0 then
  redis.call('EXPIRE', KEYS[1], tonumber(ARGV[3]))
end
return 1
`

// Options configures the Redis checkpointer.
type Options struct {
	Prefix string
	TTL    time.Duration
	// LeasePrefix is the Redis key prefix for lease records used by atomic DeleteIfIdle.
	// Defaults to Prefix when empty. Lease keys: {LeasePrefix}:lease:{threadID}
	LeasePrefix string
}

// Checkpointer stores snapshots in Redis list history (latest at index 0).
type Checkpointer[T, E any] struct {
	client      goredis.Cmdable
	prefix      string
	leasePrefix string
	ttl         time.Duration
	serializer  flowy.StateSerializer[T]
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
	leasePrefix := opts.LeasePrefix
	if leasePrefix == "" {
		leasePrefix = prefix
	}
	return &Checkpointer[T, E]{
		client:      client,
		prefix:      prefix,
		leasePrefix: leasePrefix,
		ttl:         opts.TTL,
		serializer:  serializer,
	}
}

func (c *Checkpointer[T, E]) Save(
	ctx context.Context,
	expectedRevision uint64,
	snapshot flowy.Snapshot[T, E],
) (uint64, error) {
	newRevision := expectedRevision + 1
	snapshot.Revision = newRevision
	stored, err := checkpoint.EncodeStoredSnapshot(snapshot, c.serializer)
	if err != nil {
		return 0, err
	}
	payload, err := json.Marshal(stored)
	if err != nil {
		return 0, fmt.Errorf("redis checkpoint marshal: %w", err)
	}
	key := c.historyKey(snapshot.ThreadID)
	ttlSec := int64(0)
	if c.ttl > 0 {
		ttlSec = int64(c.ttl.Seconds())
	}
	result, err := c.client.Eval(ctx, saveOccScript, []string{key},
		expectedRevision, string(payload), ttlSec,
	).Int64()
	if err != nil {
		return 0, err
	}
	if result == 0 {
		return 0, flowy.ErrConcurrencyConflict
	}
	return newRevision, nil
}

func (c *Checkpointer[T, E]) Load(ctx context.Context, threadID string) (flowy.Snapshot[T, E], uint64, error) {
	values, err := c.client.LRange(ctx, c.historyKey(threadID), 0, 0).Result()
	if err != nil {
		return flowy.Snapshot[T, E]{}, 0, err
	}
	if len(values) == 0 {
		return flowy.Snapshot[T, E]{}, 0, fmt.Errorf("%w: %w", flowy.ErrThreadNotFound, checkpoint.ErrNoSnapshot)
	}
	snapshot, err := c.decode(values[0])
	if err != nil {
		return flowy.Snapshot[T, E]{}, 0, err
	}
	return snapshot, snapshot.Revision, nil
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

const deleteIfIdleScript = `
if redis.call('exists', KEYS[2]) == 1 then
  return 0
end
return redis.call('del', KEYS[1])
`

func (c *Checkpointer[T, E]) DeleteIfIdle(ctx context.Context, threadID string) error {
	result, err := c.client.Eval(ctx, deleteIfIdleScript, []string{
		c.historyKey(threadID),
		c.leaseKey(threadID),
	}).Int64()
	if err != nil {
		return err
	}
	if result == 0 {
		held, existsErr := c.client.Exists(ctx, c.leaseKey(threadID)).Result()
		if existsErr == nil && held > 0 {
			return flowy.ErrThreadLeaseBusy
		}
	}
	return nil
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

func (c *Checkpointer[T, E]) leaseKey(threadID string) string {
	return fmt.Sprintf("%s:lease:%s", c.leasePrefix, threadID)
}

// NativeDeleteIfIdle marks atomic delete-if-idle in Redis storage.
func (*Checkpointer[T, E]) NativeDeleteIfIdle() {}

var _ flowy.Checkpointer[any, any] = (*Checkpointer[any, any])(nil)
var _ flowy.NativeDeleteIfIdleCheckpointer = (*Checkpointer[any, any])(nil)
