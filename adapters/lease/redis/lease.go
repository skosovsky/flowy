// Package redis provides Redis LeaseManager for flowy thread leases.
package redis

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"
	"unicode/utf8"

	goredis "github.com/redis/go-redis/v9"

	"github.com/skosovsky/flowy"
	"github.com/skosovsky/flowy/internal/nilvalue"
	"github.com/skosovsky/flowy/internal/rediskeys"
)

const defaultPrefix = "flowy"

var (
	ErrConfiguration         = errors.New("flowy redis: invalid standalone configuration")
	ErrDeploymentUnsupported = errors.New("flowy redis: only standalone deployment supported")
)

const acquireReplyFields = 3

// Options configures Redis lease keys. Use the same LeasePrefix as the Redis checkpointer.
type Options struct {
	Prefix string
}

// LeaseManager stores fenced leases in standalone Redis key schema v2.
type LeaseManager struct {
	client goredis.Cmdable
	prefix string
}

// NewLeaseManager supports only a standalone Redis server. Cmdable wrappers
// must address the same standalone server; ClusterClient and Ring are rejected.
func NewLeaseManager(client goredis.Cmdable, opts Options) (*LeaseManager, error) {
	if nilvalue.IsNil(client) || !utf8.ValidString(opts.Prefix) {
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
	return &LeaseManager{client: client, prefix: prefix}, nil
}

const acquireScript = `
local current = redis.call('GET', KEYS[1])
if current then
  if cjson.decode(current).owner == ARGV[1] then return {0, '', ''} end
  return {-1, '', ''}
end
local incremented = redis.pcall('INCR', KEYS[2])
if type(incremented) == 'table' and incremented.err then return {-2, '', ''} end
local fence = redis.call('GET', KEYS[2])
if fence == '0' or string.sub(fence, 1, 1) == '-' then return {-2, '', ''} end
local time = redis.call('TIME')
local expiry = time[1] * 1000 + math.floor(time[2] / 1000) + tonumber(ARGV[2])
redis.call('SET', KEYS[1], cjson.encode({owner=ARGV[1], incarnation=fence}), 'PX', ARGV[2])
return {1, fence, string.format('%.0f', expiry)}
`

func (m *LeaseManager) Acquire(
	ctx context.Context,
	threadID, owner string,
	ttl time.Duration,
) (flowy.ExecutionLease, error) {
	if threadID == "" || owner == "" {
		return flowy.ExecutionLease{}, errors.New("flowy: lease acquire requires threadID and owner")
	}
	if ttl <= 0 {
		return flowy.ExecutionLease{}, errors.New("flowy: lease ttl must be positive")
	}
	result, err := m.client.Eval(ctx, acquireScript, []string{m.leaseKey(threadID), m.fenceKey(threadID)}, owner, leaseTTLMillis(ttl)).
		Slice()
	if err != nil {
		return flowy.ExecutionLease{}, err
	}
	if len(result) != acquireReplyFields {
		return flowy.ExecutionLease{}, flowy.ErrExecutionCapability
	}
	switch result[0] {
	case int64(1):
		return decodeAcquiredLease(threadID, owner, result)
	case int64(0):
		return flowy.ExecutionLease{}, fmt.Errorf("%w: %s", flowy.ErrThreadLeaseBusy, owner)
	case int64(-2):
		return flowy.ExecutionLease{}, flowy.ErrExecutionCapability
	default:
		return flowy.ExecutionLease{}, flowy.ErrLeaseHeld
	}
}

const renewScript = `
local current = redis.call('GET', KEYS[1])
if not current then return '' end
local record = cjson.decode(current)
if record.owner ~= ARGV[1] or record.incarnation ~= ARGV[2] then return '' end
local time = redis.call('TIME')
local expiry = time[1] * 1000 + math.floor(time[2] / 1000) + tonumber(ARGV[3])
redis.call('PEXPIRE', KEYS[1], ARGV[3])
return string.format('%.0f', expiry)
`

const releaseScript = `
local current = redis.call('GET', KEYS[1])
if current == false then return 1 end
local record = cjson.decode(current)
if record.owner ~= ARGV[1] or record.incarnation ~= ARGV[2] then return 0 end
redis.call('DEL', KEYS[1])
return 1
`

func (m *LeaseManager) Renew(
	ctx context.Context,
	lease flowy.ExecutionLease,
	ttl time.Duration,
) (flowy.ExecutionLease, error) {
	if lease.ExecutionID == "" || lease.Owner == "" || lease.Incarnation == 0 || ttl <= 0 {
		return flowy.ExecutionLease{}, flowy.ErrLeaseLost
	}
	millis := leaseTTLMillis(ttl)
	result, err := m.client.Eval(ctx, renewScript, []string{m.leaseKey(lease.ExecutionID)}, lease.Owner, strconv.FormatUint(lease.Incarnation, 10), millis).
		Text()
	if err != nil {
		return flowy.ExecutionLease{}, err
	}
	if result == "" {
		return flowy.ExecutionLease{}, flowy.ErrLeaseLost
	}
	expiry, err := strconv.ParseInt(result, 10, 64)
	if err != nil {
		return flowy.ExecutionLease{}, err
	}
	lease.ExpiresAt = time.UnixMilli(expiry).UTC()
	return lease, nil
}

func (m *LeaseManager) Release(ctx context.Context, lease flowy.ExecutionLease) error {
	if lease.ExecutionID == "" || lease.Owner == "" || lease.Incarnation == 0 {
		return flowy.ErrLeaseLost
	}
	result, err := m.client.Eval(ctx, releaseScript, []string{m.leaseKey(lease.ExecutionID)}, lease.Owner, strconv.FormatUint(lease.Incarnation, 10)).
		Int64()
	if err != nil {
		return err
	}
	if result != 1 {
		return flowy.ErrLeaseLost
	}
	return nil
}

func (m *LeaseManager) IsHeld(ctx context.Context, threadID string) (bool, error) {
	_, held, err := m.Holder(ctx, threadID)
	return held, err
}

func (m *LeaseManager) Holder(ctx context.Context, threadID string) (string, bool, error) {
	owner, err := m.client.Get(ctx, m.leaseKey(threadID)).Result()
	if errors.Is(err, goredis.Nil) {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	var record struct {
		Owner       string `json:"owner"`
		Incarnation uint64 `json:"incarnation,string"`
	}
	if decodeErr := json.Unmarshal([]byte(owner), &record); decodeErr != nil {
		return "", false, decodeErr
	}
	if record.Owner == "" || record.Incarnation == 0 {
		return "", false, flowy.ErrExecutionIncompatible
	}
	return record.Owner, true, nil
}

func (m *LeaseManager) leaseKey(threadID string) string {
	return rediskeys.Key(m.prefix, threadID, "lease")
}

func (m *LeaseManager) fenceKey(threadID string) string {
	return rediskeys.Key(m.prefix, threadID, "fence")
}

func decodeAcquiredLease(threadID, owner string, reply []any) (flowy.ExecutionLease, error) {
	fenceText, fenceOK := reply[1].(string)
	expiryText, expiryOK := reply[2].(string)
	if !fenceOK || !expiryOK {
		return flowy.ExecutionLease{}, flowy.ErrExecutionCapability
	}
	fence, err := strconv.ParseUint(fenceText, 10, 64)
	if err != nil {
		return flowy.ExecutionLease{}, err
	}
	expiry, err := strconv.ParseInt(expiryText, 10, 64)
	if err != nil {
		return flowy.ExecutionLease{}, err
	}
	return flowy.ExecutionLease{
		ExecutionID: threadID,
		Owner:       owner,
		Incarnation: fence,
		ExpiresAt:   time.UnixMilli(expiry).UTC(),
	}, nil
}

// NativeLeaseManager marks storage-backed lease keys in Redis.
func (*LeaseManager) NativeLeaseManager() {}

var _ flowy.LeaseManager = (*LeaseManager)(nil)
var _ flowy.NativeLeaseManager = (*LeaseManager)(nil)

// Redis expiry precision is milliseconds; positive fractions must not expire early.
func leaseTTLMillis(ttl time.Duration) int64 {
	millis := ttl.Milliseconds()
	if ttl%time.Millisecond != 0 {
		millis++
	}
	return millis
}
