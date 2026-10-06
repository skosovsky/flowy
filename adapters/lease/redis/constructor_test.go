package redis

import (
	"testing"

	goredis "github.com/redis/go-redis/v9"
)

func mustLeaseManager(t *testing.T, client goredis.Cmdable, options Options) *LeaseManager {
	t.Helper()
	manager, err := NewLeaseManager(client, options)
	if err != nil {
		t.Fatal(err)
	}
	return manager
}
