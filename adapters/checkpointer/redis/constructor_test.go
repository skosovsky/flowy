package redis

import (
	"errors"
	"testing"

	goredis "github.com/redis/go-redis/v9"

	"github.com/skosovsky/flowy"
	redislease "github.com/skosovsky/flowy/adapters/lease/redis"
	"github.com/skosovsky/flowy/checkpoint"
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

type nilCmdable struct{ goredis.Cmdable }

func TestConstructorRejectsNilCollaborators(t *testing.T) {
	t.Parallel()
	var typedClient *nilCmdable
	var typedCodec *checkpoint.JSONSerializer[int]
	for _, client := range []goredis.Cmdable{nil, typedClient} {
		// Arrange/Act.
		cp, err := NewCheckpointer[int, flowy.NoEffect](client, Options{}, checkpoint.JSONSerializer[int]{})
		// Assert.
		if cp != nil || !errors.Is(err, ErrConfiguration) {
			t.Fatalf("cp=%v err=%v", cp, err)
		}
	}
	client := goredis.NewClient(&goredis.Options{Addr: "unused"})
	t.Cleanup(func() { _ = client.Close() })
	for _, codec := range []flowy.StateSerializer[int]{nil, typedCodec} {
		cp, err := NewCheckpointer[int, flowy.NoEffect](client, Options{}, codec)
		if cp != nil || !errors.Is(err, ErrConfiguration) {
			t.Fatalf("cp=%v err=%v", cp, err)
		}
	}
}
