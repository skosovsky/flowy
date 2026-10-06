// Package redis provides Redis checkpointer adapter.
//
// Handoff transactional outbox (SaveWithOutbox) is not supported — use the 3-phase Handoff FSM
// with RecoverStaleHandoff for stale pending. Postgres adapter implements TransactionalCheckpointer.
package redis

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strconv"
	"time"
	"unicode/utf8"

	goredis "github.com/redis/go-redis/v9"

	"github.com/skosovsky/flowy"
	"github.com/skosovsky/flowy/checkpoint"
	"github.com/skosovsky/flowy/internal/nilvalue"
	"github.com/skosovsky/flowy/internal/rediskeys"
)

const defaultPrefix = "flowy"

var (
	ErrConfiguration         = errors.New("flowy redis: invalid standalone configuration")
	ErrDeploymentUnsupported = errors.New("flowy redis: only standalone deployment supported")
)

const saveOccScript = `
local activeLease = redis.call('GET', KEYS[2])
if activeLease then
  local ok, lease = pcall(cjson.decode, activeLease)
  if not ok or type(lease) ~= 'table' then return -2 end
  if not lease.owner or not lease.incarnation then return -2 end
  if ARGV[4] == '' or lease.owner ~= ARGV[4] or lease.incarnation ~= ARGV[5] then return -1 end
elseif ARGV[4] ~= '' then
  return -1
end
local head = redis.call('LINDEX', KEYS[1], 0)
local current = '0'
if head then
  local ok, stored = pcall(cjson.decode, head)
  if not ok or type(stored) ~= 'table' then return -3 end
  if type(stored.revision) ~= 'string' then return -3 end
  current = stored.revision
  if not string.match(current, '^[1-9][0-9]*$') or string.len(current) > 20 or
     (string.len(current) == 20 and current > '18446744073709551615') then return -3 end
end
local expected = ARGV[1]
if current ~= expected then
  return 0
end
redis.call('LPUSH', KEYS[1], ARGV[2])
if tonumber(ARGV[3]) > 0 then
  redis.call('PEXPIRE', KEYS[1], ARGV[3])
else
  redis.call('PERSIST', KEYS[1])
end
return 1
`

// Options configures the Redis checkpointer.
type Options struct {
	Prefix string
	TTL    time.Duration
	// LeasePrefix is the Redis key prefix for lease records used by atomic DeleteIfIdle.
	// Defaults to Prefix when empty. Keys use independently encoded v2 identities.
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

// NewCheckpointer creates a standalone-only Redis checkpointer. Cmdable wrappers
// must address one standalone server; ClusterClient and Ring are unsupported.
func NewCheckpointer[T, E any](
	client goredis.Cmdable,
	opts Options,
	serializer flowy.StateSerializer[T],
) (*Checkpointer[T, E], error) {
	if nilvalue.IsNil(client) || nilvalue.IsNil(serializer) || opts.TTL < 0 || !utf8.ValidString(opts.Prefix) ||
		!utf8.ValidString(opts.LeasePrefix) {
		return nil, ErrConfiguration
	}
	switch client.(type) {
	case *goredis.ClusterClient, *goredis.Ring:
		return nil, ErrDeploymentUnsupported
	}

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
	}, nil
}

func (c *Checkpointer[T, E]) Save(
	ctx context.Context,
	expectedRevision uint64,
	snapshot flowy.Snapshot[T, E],
) (uint64, error) {
	if expectedRevision == math.MaxUint64 {
		return 0, flowy.ErrExecutionCapability
	}
	newRevision := expectedRevision + 1
	snapshot.Revision = newRevision
	stored, err := checkpoint.EncodeRecord(snapshot, c.serializer)
	if err != nil {
		return 0, err
	}
	payload, err := marshalRecord(stored)
	if err != nil {
		return 0, fmt.Errorf("redis checkpoint marshal: %w", err)
	}
	key := c.historyKey(snapshot.ThreadID)
	ttlMillis := c.ttl.Milliseconds()
	if c.ttl%time.Millisecond != 0 {
		ttlMillis++
	}
	owner, incarnation := "", ""
	if lease, supplied := flowy.ExecutionLeaseFromContext(ctx); supplied {
		if lease.ExecutionID != snapshot.ThreadID || lease.Owner == "" || lease.Incarnation == 0 {
			return 0, flowy.ErrLeaseLost
		}
		owner, incarnation = lease.Owner, strconv.FormatUint(lease.Incarnation, 10)
	}
	result, err := c.client.Eval(ctx, saveOccScript, []string{key, c.leaseKey(snapshot.ThreadID)},
		strconv.FormatUint(expectedRevision, 10), string(payload), ttlMillis, owner, incarnation,
	).Int64()
	if err != nil {
		return 0, err
	}
	if result == 0 {
		return 0, flowy.ErrConcurrencyConflict
	}
	if result == -1 {
		return 0, flowy.ErrLeaseLost
	}
	if result == -2 {
		return 0, flowy.ErrExecutionIncompatible
	}
	if result == -3 {
		return 0, checkpoint.ErrInvalidRecord
	}
	if result != 1 {
		return 0, flowy.ErrExecutionCapability
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
	snapshot, err := c.decode(threadID, values[0])
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
		snapshot, decodeErr := c.decode(threadID, item)
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
  return -1
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
	switch result {
	case -1:
		return flowy.ErrThreadLeaseBusy
	case 0, 1:
		return nil
	default:
		return flowy.ErrExecutionCapability
	}
}

func (c *Checkpointer[T, E]) decode(threadID string, raw string) (flowy.Snapshot[T, E], error) {
	var stored redisRecord
	if err := json.Unmarshal([]byte(raw), &stored); err != nil {
		return flowy.Snapshot[T, E]{}, fmt.Errorf("redis checkpoint unmarshal: %w", err)
	}
	snapshot, err := checkpoint.DecodeRecord[T, E](
		stored.checkpointRecord(),
		c.serializer,
		checkpoint.DecodeRecordOptions{
			ExpectedThreadID:         threadID,
			ExpectedRevision:         0,
			ExpectedExecutionPointer: "",
		},
	)
	if err != nil {
		return flowy.Snapshot[T, E]{}, err
	}
	return snapshot, nil
}

// Redis uses a decimal string revision: Lua JSON numbers cannot represent all
// uint64 values. Numeric legacy records require an explicit offline migration.
type redisRecord struct {
	checkpoint.Record

	ExactRevision uint64 `json:"revision,string"`
}

func marshalRecord(record checkpoint.Record) ([]byte, error) {
	return json.Marshal(redisRecord{Record: record, ExactRevision: record.Revision})
}

func (r redisRecord) checkpointRecord() checkpoint.Record {
	r.Record.Revision = r.ExactRevision
	return r.Record
}

func (c *Checkpointer[T, E]) historyKey(threadID string) string {
	return rediskeys.Key(c.prefix, threadID, "history")
}

func (c *Checkpointer[T, E]) leaseKey(threadID string) string {
	return rediskeys.Key(c.leasePrefix, threadID, "lease")
}

// NativeDeleteIfIdle marks atomic delete-if-idle in Redis storage.
func (*Checkpointer[T, E]) NativeDeleteIfIdle() {}

var _ flowy.Checkpointer[any, any] = (*Checkpointer[any, any])(nil)
var _ flowy.NativeDeleteIfIdleCheckpointer = (*Checkpointer[any, any])(nil)
