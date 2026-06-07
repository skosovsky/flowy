// Package redis provides Redis LeaseManager for flowy thread leases.
package redis

import (
	"context"
	"errors"
	"fmt"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/skosovsky/flowy"
)

const defaultPrefix = "flowy"

// Options configures Redis lease keys. Use the same LeasePrefix as the Redis checkpointer.
type Options struct {
	Prefix string
}

// LeaseManager stores thread leases at {prefix}:lease:{threadID}.
type LeaseManager struct {
	client goredis.Cmdable
	prefix string
}

// NewLeaseManager creates a Redis-backed lease manager.
func NewLeaseManager(client goredis.Cmdable, opts Options) *LeaseManager {
	prefix := opts.Prefix
	if prefix == "" {
		prefix = defaultPrefix
	}
	return &LeaseManager{client: client, prefix: prefix}
}

const acquireScript = `
local key = KEYS[1]
local owner = ARGV[1]
local ttl = tonumber(ARGV[2])
local current = redis.call('GET', key)
if current == false then
  redis.call('SET', key, owner, 'EX', ttl)
  return 1
end
if current == owner then
  return 0
end
return -1
`

func (m *LeaseManager) Acquire(ctx context.Context, threadID, owner string, ttl time.Duration) error {
	if threadID == "" || owner == "" {
		return errors.New("flowy: lease acquire requires threadID and owner")
	}
	if ttl <= 0 {
		return errors.New("flowy: lease ttl must be positive")
	}
	secs := int(ttl.Seconds())
	if secs <= 0 {
		secs = 1
	}
	result, err := m.client.Eval(ctx, acquireScript, []string{m.leaseKey(threadID)}, owner, secs).Int64()
	if err != nil {
		return err
	}
	switch result {
	case 1:
		return nil
	case 0:
		return fmt.Errorf("%w: %s", flowy.ErrThreadLeaseBusy, owner)
	default:
		holder, held, holderErr := m.Holder(ctx, threadID)
		if holderErr != nil {
			return holderErr
		}
		if held {
			return fmt.Errorf("%w: %s", flowy.ErrLeaseHeld, holder)
		}
		return fmt.Errorf("%w: %s", flowy.ErrLeaseHeld, holder)
	}
}

func (m *LeaseManager) Renew(ctx context.Context, threadID, owner string, ttl time.Duration) error {
	key := m.leaseKey(threadID)
	current, err := m.client.Get(ctx, key).Result()
	if errors.Is(err, goredis.Nil) {
		return fmt.Errorf("%w: %s", flowy.ErrLeaseHeld, owner)
	}
	if err != nil {
		return err
	}
	if current != owner {
		return fmt.Errorf("%w: %s", flowy.ErrLeaseHeld, current)
	}
	secs := int(ttl.Seconds())
	if secs <= 0 {
		secs = 1
	}
	return m.client.Set(ctx, key, owner, time.Duration(secs)*time.Second).Err()
}

func (m *LeaseManager) Release(ctx context.Context, threadID, owner string) error {
	key := m.leaseKey(threadID)
	current, err := m.client.Get(ctx, key).Result()
	if errors.Is(err, goredis.Nil) {
		return nil
	}
	if err != nil {
		return err
	}
	if current != owner {
		return fmt.Errorf("%w: %s", flowy.ErrLeaseHeld, current)
	}
	return m.client.Del(ctx, key).Err()
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
	return owner, true, nil
}

func (m *LeaseManager) leaseKey(threadID string) string {
	return fmt.Sprintf("%s:lease:%s", m.prefix, threadID)
}

// NativeLeaseManager marks storage-backed lease keys in Redis.
func (*LeaseManager) NativeLeaseManager() {}

var _ flowy.LeaseManager = (*LeaseManager)(nil)
var _ flowy.NativeLeaseManager = (*LeaseManager)(nil)
