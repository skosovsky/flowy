package redis

import (
	"errors"
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

type nilCmdable struct{ goredis.Cmdable }

func TestConstructorRejectsNilClient(t *testing.T) {
	t.Parallel()
	var typedClient *nilCmdable
	for _, client := range []goredis.Cmdable{nil, typedClient} {
		// Arrange/Act.
		manager, err := NewLeaseManager(client, Options{})
		// Assert.
		if manager != nil || !errors.Is(err, ErrConfiguration) {
			t.Fatalf("manager=%v err=%v", manager, err)
		}
	}
}
