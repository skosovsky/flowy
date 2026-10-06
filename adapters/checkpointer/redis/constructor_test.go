package redis

import (
	"testing"

	goredis "github.com/redis/go-redis/v9"

	"github.com/skosovsky/flowy"
	redislease "github.com/skosovsky/flowy/adapters/lease/redis"
)

func mustCheckpointer[T, E any](
	t *testing.T,
	client goredis.Cmdable,
	options Options,
	codec flowy.StateSerializer[T],
) *Checkpointer[T, E] {
	t.Helper()
	checkpointer, err := NewCheckpointer[T, E](client, options, codec)
	if err != nil {
		t.Fatal(err)
	}
	return checkpointer
}

func mustRedisLeaseManager(t *testing.T, client goredis.Cmdable, options redislease.Options) *redislease.LeaseManager {
	t.Helper()
	manager, err := redislease.NewLeaseManager(client, options)
	if err != nil {
		t.Fatal(err)
	}
	return manager
}
