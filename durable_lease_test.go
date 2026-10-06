package flowy_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/skosovsky/flowy"
	"github.com/skosovsky/flowy/checkpoint"
	"github.com/skosovsky/flowy/testutil"
)

type heartbeatProbeStore struct {
	flowy.ExecutionStore

	renewed chan context.Context
	phase   <-chan struct{}
	failure error
}

func (s *heartbeatProbeStore) RenewExecution(
	ctx context.Context,
	lease flowy.ExecutionLease,
	ttl time.Duration,
) (flowy.ExecutionLease, error) {
	select {
	case <-s.phase:
	default:
		return s.ExecutionStore.RenewExecution(ctx, lease, ttl)
	}
	select {
	case s.renewed <- ctx:
	default:
	}
	if s.failure != nil {
		return flowy.ExecutionLease{}, s.failure
	}
	return s.ExecutionStore.RenewExecution(ctx, lease, ttl)
}

type blockingInitialCodec struct {
	once    sync.Once
	entered chan struct{}
	proceed chan struct{}
}

func (c *blockingInitialCodec) Marshal(state durableTestState) ([]byte, error) {
	c.once.Do(func() { close(c.entered); <-c.proceed })
	return checkpoint.JSONSerializer[durableTestState]{}.Marshal(state)
}

func (*blockingInitialCodec) Unmarshal(payload []byte) (durableTestState, error) {
	return checkpoint.JSONSerializer[durableTestState]{}.Unmarshal(payload)
}

func TestDurableLeaseCoversPreExecutionWork(t *testing.T) {
	for _, stage := range []string{"initial codec", "migration"} {
		for _, lost := range []bool{false, true} {
			t.Run(stage+map[bool]string{false: "/renewed", true: "/lost"}[lost], func(t *testing.T) {
				assertPreExecutionLease(t, stage, lost)
			})
		}
	}
}

func assertPreExecutionLease(t *testing.T, stage string, lost bool) {
	t.Helper()
	// Arrange: fake store time avoids a real-time expiry race in the test itself.
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	clock := &testExecutionClock{}
	clock.set(time.Now())
	base := testutil.NewMemoryExecutionStore(clock.Now)
	counts := &faultExecutionStore{ExecutionStore: base}
	store := &heartbeatProbeStore{ExecutionStore: counts, renewed: make(chan context.Context, 1)}
	if lost {
		store.failure = flowy.ErrLeaseLost
	}
	entered, proceed := make(chan struct{}), make(chan struct{})
	store.phase = entered
	var unblock sync.Once
	t.Cleanup(func() { unblock.Do(func() { close(proceed) }) })
	var calls atomic.Int32
	builder := flowy.NewGraph[durableTestState, flowy.NoEffect](
		func(_, update durableTestState) durableTestState { return update },
	)
	builder.AddNode("node", func(_ context.Context, state durableTestState) (durableTestState, flowy.Directive, error) {
		calls.Add(1)
		return state, flowy.End(), nil
	}).AllowNoOutgoingRoute("node").SetEntryPoint("node")
	graph, err := builder.Compile()
	if err != nil {
		t.Fatal(err)
	}
	var codec flowy.StateSerializer[durableTestState] = checkpoint.JSONSerializer[durableTestState]{}
	options := flowy.DurableOptions{Owner: "worker", LeaseTTL: 30 * time.Millisecond}
	var token flowy.ResumeToken
	if stage == "initial codec" {
		codec = &blockingInitialCodec{entered: entered, proceed: proceed}
	} else {
		token = seedLeaseMigration(ctx, t, base)
		options.Migrations = []flowy.ExecutionMigration{
			{ID: "move", Source: durableDescriptor("old"), Target: durableDescriptor("current"),
				Transform: func(state flowy.MigrationState) (flowy.MigrationState, error) {
					close(entered)
					<-proceed
					return state, nil
				},
			},
		}
	}
	runner, err := flowy.NewDurableRunner(graph, store, durableDescriptor("current"), codec,
		checkpoint.JSONSerializer[[]flowy.NoEffect]{}, options)
	if err != nil {
		t.Fatal(err)
	}
	// Act: hold host work until a heartbeat is observed in that same phase.
	done := make(chan error, 1)
	go func() {
		if stage == "initial codec" {
			_, runErr := runner.Start(ctx, "run", durableTestState{})
			done <- runErr
			return
		}
		_, runErr := runner.Resume(ctx, token)
		done <- runErr
	}()
	awaitDurableBarrier(ctx, t, entered)
	renewCtx := awaitHeartbeatProbe(ctx, t, store.renewed)
	if lost {
		awaitDurableBarrier(ctx, t, renewCtx.Done())
	}
	unblock.Do(func() { close(proceed) })
	assertDurableLeaseResult(ctx, t, done, counts, &calls, lost)
	assertRetryWorkerReleased(t, base)
}

func seedLeaseMigration(ctx context.Context, t *testing.T, store flowy.ExecutionStore) flowy.ResumeToken {
	t.Helper()
	lease, err := store.AcquireExecution(ctx, "run", "seed", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	envelope, err := store.CommitExecution(ctx, 0, lease, flowy.ExecutionEnvelope{
		ExecutionID: "run",
		Descriptor:  durableDescriptor("old"),
		Activation:  1,
		Progress: flowy.MigrationState{
			ExecutionPointer: "node",
			StatePayload:     []byte(`{"Value":0}`),
		},
		EffectsPayload: []byte("null"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if releaseErr := store.ReleaseExecution(ctx, lease); releaseErr != nil {
		t.Fatal(releaseErr)
	}
	return flowy.ResumeToken{ThreadID: "run", SnapshotRevision: envelope.Revision}
}

func awaitDurableBarrier(ctx context.Context, t *testing.T, barrier <-chan struct{}) {
	t.Helper()
	select {
	case <-barrier:
	case <-ctx.Done():
		t.Fatal("durable barrier not reached", ctx.Err())
	}
}

func awaitHeartbeatProbe(ctx context.Context, t *testing.T, probe <-chan context.Context) context.Context {
	t.Helper()
	select {
	case observed := <-probe:
		return observed
	case <-ctx.Done():
		t.Fatal("heartbeat absent during pre-execution work", ctx.Err())
	}
	return ctx
}

func assertDurableLeaseResult(
	ctx context.Context,
	t *testing.T,
	done <-chan error,
	store *faultExecutionStore,
	calls *atomic.Int32,
	lost bool,
) {
	t.Helper()
	select {
	case err := <-done:
		if lost {
			if !errors.Is(err, flowy.ErrLeaseLost) || calls.Load() != 0 || store.commits.Load() != 0 {
				t.Fatalf("lost owner executed: %v calls=%d commits=%d", err, calls.Load(), store.commits.Load())
			}
			return
		}
		if err != nil || calls.Load() != 1 {
			t.Fatalf("renewed owner failed: %v calls=%d", err, calls.Load())
		}
	case <-ctx.Done():
		t.Fatal("execution did not exit", ctx.Err())
	}
}
