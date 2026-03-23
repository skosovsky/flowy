package redis

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/skosovsky/flowy/checkpoint"
)

const defaultPrefix = "flowy"
const orderIndexBatchSize int64 = 128

// Options configures the Redis checkpointer.
type Options struct {
	Prefix string
	TTL    time.Duration
}

// Checkpointer stores flowy checkpoints in Redis Streams.
// The stream is the append-only payload log; order_key defines canonical recency semantics.
// History is newest-first by CreatedAt, breaking ties by Checkpoint.ID descending.
type Checkpointer struct {
	client     goredis.Cmdable
	prefix     string
	ttl        time.Duration
	saveScript *goredis.Script
}

// saveCheckpointLua is the Redis Lua script for idempotent checkpoint append (see Save).
const saveCheckpointLua = `
local ids = KEYS[1]
local stream = KEYS[2]
local order_key = KEYS[3]
local sequence_key = KEYS[4]
local last_ms_key = KEYS[5]
local checkpoint_id = ARGV[1]
local run_id = ARGV[2]
local node = ARGV[3]
local next_node = ARGV[4]
local created_at = ARGV[5]
local state_data = ARGV[6]
local created_at_ms = tonumber(ARGV[7])
-- Keep nanosecond ordering as a zero-padded string: Lua numbers lose precision above 2^53.
local created_at_ns_padded = ARGV[8]
local ttl = tonumber(ARGV[9])

local function refresh_ttl(key)
    if ttl and ttl > 0 then
        redis.call("PEXPIRE", key, ttl)
    end
end

if redis.call("SISMEMBER", ids, checkpoint_id) == 1 then
    refresh_ttl(ids)
    refresh_ttl(stream)
    refresh_ttl(order_key)
    refresh_ttl(sequence_key)
    refresh_ttl(last_ms_key)
    return 0
end

local sequence = redis.call("INCR", sequence_key)
local last_ms = tonumber(redis.call("GET", last_ms_key) or "0")
local stream_ms = created_at_ms
if stream_ms < last_ms then
    stream_ms = last_ms
end
local stream_id = tostring(stream_ms) .. "-" .. tostring(sequence)
local order_member = created_at_ns_padded .. "|" .. checkpoint_id .. "|" .. stream_id

redis.call("SADD", ids, checkpoint_id)
redis.call(
    "XADD", stream, stream_id,
    "checkpoint_id", checkpoint_id,
    "run_id", run_id,
    "node", node,
    "next", next_node,
    "created_at", created_at,
    "sequence", tostring(sequence),
    "state_data", state_data
)
redis.call("ZADD", order_key, 0, order_member)
redis.call("SET", last_ms_key, tostring(stream_ms))

refresh_ttl(ids)
refresh_ttl(stream)
refresh_ttl(order_key)
refresh_ttl(sequence_key)
refresh_ttl(last_ms_key)

return 1
`

// New creates a Redis Streams-backed checkpoint.Checkpointer.
func New(client goredis.Cmdable, opts Options) *Checkpointer {
	prefix := opts.Prefix
	if prefix == "" {
		prefix = defaultPrefix
	}

	return &Checkpointer{
		client:     client,
		prefix:     prefix,
		ttl:        opts.TTL,
		saveScript: goredis.NewScript(saveCheckpointLua),
	}
}

func (c *Checkpointer) Save(ctx context.Context, cp checkpoint.Checkpoint) error {
	createdAtUTC := cp.CreatedAt.UTC()
	_, err := c.saveScript.Run(ctx, c.client, []string{
		c.idsKey(cp.ThreadID),
		c.streamKey(cp.ThreadID),
		c.orderKey(cp.ThreadID),
		c.sequenceKey(cp.ThreadID),
		c.lastMSKey(cp.ThreadID),
	},
		cp.ID,
		cp.RunID,
		cp.Node,
		cp.Next,
		createdAtUTC.Format(time.RFC3339Nano),
		string(cp.StateData),
		strconv.FormatInt(createdAtUTC.UnixMilli(), 10),
		formatUnixNanoOrderKey(createdAtUTC),
		strconv.FormatInt(c.ttl.Milliseconds(), 10),
	).Result()
	return err
}

func (c *Checkpointer) LoadLatest(ctx context.Context, threadID string) (checkpoint.Checkpoint, error) {
	members, err := c.orderMembers(ctx, threadID, 0, 0)
	if err != nil {
		return checkpoint.Checkpoint{}, err
	}
	if len(members) == 0 {
		return checkpoint.Checkpoint{}, checkpoint.ErrNoCheckpoint
	}
	records, err := c.recordsForOrderMembers(ctx, threadID, members)
	if err != nil {
		return checkpoint.Checkpoint{}, err
	}
	return records[0].checkpoint, nil
}

func (c *Checkpointer) GetHistory(ctx context.Context, threadID string, limit int) ([]checkpoint.Checkpoint, error) {
	records, err := c.historyRecords(ctx, threadID, limit)
	if err != nil {
		return nil, err
	}
	history := make([]checkpoint.Checkpoint, 0, len(records))
	for _, record := range records {
		history = append(history, record.checkpoint)
	}
	return history, nil
}

func (c *Checkpointer) historyRecords(ctx context.Context, threadID string, limit int) ([]checkpointRecord, error) {
	if limit > 0 {
		return c.historyRecordsLimited(ctx, threadID, limit)
	}
	return c.allRecords(ctx, threadID)
}

func (c *Checkpointer) historyRecordsLimited(
	ctx context.Context,
	threadID string,
	limit int,
) ([]checkpointRecord, error) {
	members, err := c.orderMembers(ctx, threadID, 0, int64(limit-1))
	if err != nil {
		return nil, err
	}
	if len(members) == 0 {
		return nil, checkpoint.ErrNoCheckpoint
	}
	return c.recordsForOrderMembers(ctx, threadID, members)
}

type checkpointRecord struct {
	checkpoint checkpoint.Checkpoint
	sequence   int64
}

func (c *Checkpointer) allRecords(ctx context.Context, threadID string) ([]checkpointRecord, error) {
	var out []checkpointRecord
	for start := int64(0); ; start += orderIndexBatchSize {
		members, err := c.orderMembers(ctx, threadID, start, start+orderIndexBatchSize-1)
		if err != nil {
			return nil, err
		}
		if len(members) == 0 {
			if len(out) == 0 {
				return nil, checkpoint.ErrNoCheckpoint
			}
			return out, nil
		}

		records, err := c.recordsForOrderMembers(ctx, threadID, members)
		if err != nil {
			return nil, err
		}
		out = append(out, records...)
		if len(members) < int(orderIndexBatchSize) {
			return out, nil
		}
	}
}

func (c *Checkpointer) orderMembers(ctx context.Context, threadID string, start, stop int64) ([]string, error) {
	members, err := c.client.ZRevRange(ctx, c.orderKey(threadID), start, stop).Result()
	if err != nil {
		return nil, err
	}
	return members, nil
}

func (c *Checkpointer) recordsForOrderMembers(
	ctx context.Context,
	threadID string,
	members []string,
) ([]checkpointRecord, error) {
	if len(members) == 0 {
		return nil, checkpoint.ErrNoCheckpoint
	}

	pipe := c.client.Pipeline()
	cmds := make([]*goredis.XMessageSliceCmd, len(members))
	for i, member := range members {
		streamID, err := streamIDFromOrderMember(member)
		if err != nil {
			return nil, err
		}
		cmds[i] = pipe.XRange(ctx, c.streamKey(threadID), streamID, streamID)
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return nil, err
	}

	out := make([]checkpointRecord, 0, len(members))
	for i, cmd := range cmds {
		messages, err := cmd.Result()
		if err != nil {
			return nil, err
		}
		if len(messages) != 1 {
			return nil, fmt.Errorf("missing redis stream message for order member %q", members[i])
		}

		record, err := c.messageToRecord(threadID, messages[0])
		if err != nil {
			return nil, err
		}
		out = append(out, record)
	}
	return out, nil
}

func (c *Checkpointer) messageToRecord(threadID string, msg goredis.XMessage) (checkpointRecord, error) {
	createdAtRaw, err := fieldString(msg.Values, "created_at")
	if err != nil {
		return checkpointRecord{}, err
	}
	createdAt, err := time.Parse(time.RFC3339Nano, createdAtRaw)
	if err != nil {
		return checkpointRecord{}, fmt.Errorf("parse created_at: %w", err)
	}

	id, err := fieldString(msg.Values, "checkpoint_id")
	if err != nil {
		return checkpointRecord{}, err
	}
	runID, err := fieldString(msg.Values, "run_id")
	if err != nil {
		return checkpointRecord{}, err
	}
	node, err := fieldString(msg.Values, "node")
	if err != nil {
		return checkpointRecord{}, err
	}
	nextNode, err := fieldString(msg.Values, "next")
	if err != nil {
		return checkpointRecord{}, err
	}
	stateData, err := fieldString(msg.Values, "state_data")
	if err != nil {
		return checkpointRecord{}, err
	}
	sequence, err := fieldInt64(msg.Values, "sequence")
	if err != nil {
		return checkpointRecord{}, err
	}

	return checkpointRecord{
		checkpoint: checkpoint.Checkpoint{
			ID:        id,
			ThreadID:  threadID,
			RunID:     runID,
			Node:      node,
			Next:      nextNode,
			StateData: []byte(stateData),
			CreatedAt: createdAt,
		},
		sequence: sequence,
	}, nil
}

func (c *Checkpointer) streamKey(threadID string) string {
	return fmt.Sprintf("%s:thread:%s:stream", c.prefix, threadID)
}

func (c *Checkpointer) idsKey(threadID string) string {
	return fmt.Sprintf("%s:thread:%s:ids", c.prefix, threadID)
}

func (c *Checkpointer) orderKey(threadID string) string {
	return fmt.Sprintf("%s:thread:%s:order", c.prefix, threadID)
}

func (c *Checkpointer) sequenceKey(threadID string) string {
	return fmt.Sprintf("%s:thread:%s:seq", c.prefix, threadID)
}

func (c *Checkpointer) lastMSKey(threadID string) string {
	return fmt.Sprintf("%s:thread:%s:last-ms", c.prefix, threadID)
}

func fieldString(fields map[string]any, key string) (string, error) {
	value, ok := fields[key]
	if !ok {
		return "", fmt.Errorf("missing redis stream field %q", key)
	}

	switch v := value.(type) {
	case string:
		return v, nil
	case []byte:
		return string(v), nil
	default:
		return fmt.Sprintf("%v", v), nil
	}
}

func fieldInt64(fields map[string]any, key string) (int64, error) {
	value, err := fieldString(fields, key)
	if err != nil {
		return 0, err
	}
	parsed, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parse redis stream field %q: %w", key, err)
	}
	return parsed, nil
}

func streamIDFromOrderMember(member string) (string, error) {
	lastCut := strings.LastIndexByte(member, '|')
	if lastCut < 0 || lastCut+1 >= len(member) {
		return "", fmt.Errorf("invalid redis order member %q", member)
	}
	streamID := member[lastCut+1:]
	if streamID == "" {
		return "", fmt.Errorf("invalid redis order member %q", member)
	}
	return streamID, nil
}

func formatUnixNanoOrderKey(ts time.Time) string {
	return fmt.Sprintf("%020d", ts.UTC().UnixNano())
}

var _ checkpoint.Checkpointer = (*Checkpointer)(nil)
