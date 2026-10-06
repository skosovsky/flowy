package redis

import (
	"context"
	"errors"
	"math"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	goredis "github.com/redis/go-redis/v9"

	"github.com/skosovsky/flowy"
	"github.com/skosovsky/flowy/checkpoint"
)

type releaseAfterDeleteClient struct {
	goredis.Cmdable

	after       func()
	existsCalls int
}

func (c *releaseAfterDeleteClient) Eval(ctx context.Context, script string, keys []string, args ...any) *goredis.Cmd {
	result := c.Cmdable.Eval(ctx, script, keys, args...)
	if script == deleteIfIdleScript && result.Err() == nil {
		c.after()
	}
	return result
}
func (c *releaseAfterDeleteClient) Exists(_ context.Context, _ ...string) *goredis.IntCmd {
	c.existsCalls++
	return goredis.NewIntResult(0, errors.New("second-query failure"))
}

func TestDeleteIfIdleBusyIsOneAtomicObservation(t *testing.T) {
	// Arrange: release occurs after the authoritative Lua result, before Go sees it.
	ctx := context.Background()
	server := miniredis.RunT(t)
	client := goredis.NewClient(&goredis.Options{Addr: server.Addr()})
	defer client.Close()
	barrier := &releaseAfterDeleteClient{Cmdable: client}
	cp := mustCheckpointer[state, string](t, barrier, Options{}, checkpoint.JSONSerializer[state]{})
	if _, err := cp.Save(ctx, 0, flowy.Snapshot[state, string]{ThreadID: "run", ExecutionPointer: "node"}); err != nil {
		t.Fatal(err)
	}
	if err := client.Set(ctx, cp.leaseKey("run"), "leased", 0).Err(); err != nil {
		t.Fatal(err)
	}
	barrier.after = func() {
		if err := client.Del(ctx, cp.leaseKey("run")).Err(); err != nil {
			t.Fatal(err)
		}
	}
	// Act.
	busy := cp.DeleteIfIdle(ctx, "run")
	history, err := cp.GetHistory(ctx, "run", 0)
	// Assert: release cannot turn a rejected deletion into success; no second query.
	if !errors.Is(busy, flowy.ErrThreadLeaseBusy) || err != nil || len(history) != 1 || barrier.existsCalls != 0 {
		t.Fatalf("busy=%v history=%d/%v queries=%d", busy, len(history), err, barrier.existsCalls)
	}
	if err = cp.DeleteIfIdle(ctx, "run"); err != nil {
		t.Fatal(err)
	}
	if err = cp.DeleteIfIdle(ctx, "absent"); err != nil {
		t.Fatal(err)
	}
	if err = client.Close(); err != nil {
		t.Fatal(err)
	}
	if err = cp.DeleteIfIdle(ctx, "run"); err == nil {
		t.Fatal("server failure hidden")
	}
}

func TestCheckpointMillisecondTTLContract(t *testing.T) {
	for _, ttl := range []time.Duration{0, -1, 1, 500 * time.Millisecond, 1500 * time.Millisecond} {
		t.Run(ttl.String(), func(t *testing.T) {
			assertCheckpointTTLContract(t, ttl)
		})
	}
}

func TestStandaloneOnlyConstructorAndUnambiguousNamespaces(t *testing.T) {
	// Arrange: constructors must reject known distributed clients without any network operation.
	cluster := goredis.NewClusterClient(&goredis.ClusterOptions{Addrs: []string{"127.0.0.1:1"}})
	defer cluster.Close()
	cp, err := NewCheckpointer[state, string](cluster, Options{}, checkpoint.JSONSerializer[state]{})
	if cp != nil || !errors.Is(err, ErrDeploymentUnsupported) {
		t.Fatalf("cluster=%v/%v", cp, err)
	}
	ring := goredis.NewRing(&goredis.RingOptions{Addrs: map[string]string{"one": "127.0.0.1:1"}})
	defer ring.Close()
	cp, err = NewCheckpointer[state, string](ring, Options{}, checkpoint.JSONSerializer[state]{})
	if cp != nil || !errors.Is(err, ErrDeploymentUnsupported) {
		t.Fatalf("ring=%v/%v", cp, err)
	}
	server := miniredis.RunT(t)
	client := goredis.NewClient(&goredis.Options{Addr: server.Addr()})
	defer client.Close()
	keys := make(map[string]bool)
	for _, prefix := range []string{"app", "app:one", "app{one}", "префикс", "app:"} {
		cp = mustCheckpointer[state, string](t, client, Options{Prefix: prefix}, checkpoint.JSONSerializer[state]{})
		for _, id := range []string{"run", "{run}", "руна", "a:b", "a", "a}:b{", "a/b"} {
			for _, key := range []string{cp.historyKey(id), cp.leaseKey(id)} {
				// Act/Assert: every valid namespace/ID/kind pair has a separate storage identity.
				if keys[key] {
					t.Fatalf("key alias: %q/%q=%q", prefix, id, key)
				}
				keys[key] = true
			}
		}
	}
	// Duration ceiling is integer-safe even at the full time.Duration limit.
	maximum := time.Duration(math.MaxInt64)
	millis := maximum.Milliseconds() + 1
	if millis <= 0 || millis >= 1<<53 {
		t.Fatalf("unsafe Lua integer precision: %d", millis)
	}
}

func assertCheckpointTTLContract(t *testing.T, ttl time.Duration) {
	t.Helper()
	// Arrange: fixed standalone server clock.
	ctx := context.Background()
	server := miniredis.RunT(t)
	client := goredis.NewClient(&goredis.Options{Addr: server.Addr()})
	defer client.Close()
	cp, err := NewCheckpointer[state, string](client, Options{TTL: ttl}, checkpoint.JSONSerializer[state]{})
	if ttl < 0 {
		if !errors.Is(err, ErrConfiguration) || cp != nil {
			t.Fatalf("negative config=%v/%v", cp, err)
		}
		return
	}
	if err != nil {
		t.Fatal(err)
	}
	snapshot := flowy.Snapshot[state, string]{ThreadID: "run", ExecutionPointer: "node"}
	// Act: successful saves refresh expiry; failed OCC leaves it unchanged.
	revision, err := cp.Save(ctx, 0, snapshot)
	if err != nil {
		t.Fatal(err)
	}
	expected := ttl
	if ttl%time.Millisecond != 0 {
		expected = time.Duration(ttl.Milliseconds()+1) * time.Millisecond
	}
	if got := server.TTL(cp.historyKey("run")); got != expected {
		t.Fatalf("expiry=%v expected=%v", got, expected)
	}
	if ttl > time.Millisecond {
		server.FastForward(time.Millisecond)
	}
	previous := server.TTL(cp.historyKey("run"))
	_, conflict := cp.Save(ctx, 0, snapshot)
	if !errors.Is(conflict, flowy.ErrConcurrencyConflict) || server.TTL(cp.historyKey("run")) != previous {
		t.Fatalf("OCC changed expiry=%v", conflict)
	}
	if _, err = cp.Save(ctx, revision, snapshot); err != nil {
		t.Fatal(err)
	}
	// Assert: positive supported expiry never becomes persistent; zero is explicitly persistent.
	if got := server.TTL(cp.historyKey("run")); got != expected {
		t.Fatalf("refresh=%v expected=%v", got, expected)
	}
	persistent := mustCheckpointer[state, string](t, client, Options{}, checkpoint.JSONSerializer[state]{})
	if _, err = persistent.Save(ctx, revision+1, snapshot); err != nil {
		t.Fatal(err)
	}
	if server.TTL(cp.historyKey("run")) != 0 {
		t.Fatal("zero Save left old expiration")
	}
}
