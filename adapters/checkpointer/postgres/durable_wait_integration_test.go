//go:build integration

package postgres

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/skosovsky/flowy"
	"github.com/skosovsky/flowy/checkpoint"
)

func postgresWaitProfile() flowy.WaitCapabilityProfile {
	return flowy.WaitCapabilityProfile{Label: "postgres-aggregate-waits", JournalOwner: "postgres-aggregate",
		LeaseOwner: "postgres-fence", TimerOwner: "host-driven-pg-discovery", ClockOwner: "injected",
		RetryOwner: "activity-runtime", RecoveryOwner: "execution-runtime"}
}

func postgresWaitSpec(deadline time.Time) flowy.DurableWaitSpec {
	return flowy.DurableWaitSpec{ID: "approval", CorrelationID: "request", Deadline: deadline,
		MatcherLabel: "match", PayloadCodec: "payload", ContinuationLabel: "transition",
		EventPointer: "accepted", TimeoutPointer: "timed-out", WinnerPolicy: flowy.WaitFirstCommitted}
}

func postgresWaitRunner(t *testing.T, store flowy.ExecutionStore, spec flowy.DurableWaitSpec,
	calls *atomic.Int32, clocks ...flowy.ExecutionClock,
) *flowy.DurableRunner[intState, flowy.NoEffect] {
	t.Helper()
	b := flowy.NewGraph[intState, flowy.NoEffect](func(_, update intState) intState { return update })
	b.AddNode("waiting", func(_ context.Context, state intState) (intState, flowy.Directive, error) {
		calls.Add(1)
		state.Value++
		return state, flowy.Await(spec), nil
	}).AllowNoOutgoingRoute("waiting")
	for _, id := range []string{"accepted", "timed-out"} {
		b.AddNode(id, func(_ context.Context, state intState) (intState, flowy.Directive, error) {
			calls.Add(1)
			return state, flowy.End(), nil
		}).AllowNoOutgoingRoute(id)
	}
	b.SetEntryPoint("waiting")
	graph, err := b.Compile()
	if err != nil {
		t.Fatal(err)
	}
	profile := postgresWaitProfile()
	if capable, ok := store.(interface {
		WaitCapabilities() flowy.WaitCapabilityProfile
	}); ok {
		profile = capable.WaitCapabilities()
	}
	var clock flowy.ExecutionClock
	if len(clocks) != 0 {
		clock = clocks[0]
	}
	runner, err := flowy.NewDurableRunner(graph, store, flowy.ExecutionDescriptor{
		GraphID:           "wait-test",
		GraphRevision:     "current",
		StateCodec:        "json-state",
		EffectsCodec:      "host-effects-v1",
		ExecutionContract: "sync",
		ReplayPolicy:      flowy.StepReplayPolicy{Label: "test-safe-steps", Mode: flowy.StepReplaySafe},
	},
		checkpoint.JSONSerializer[intState]{}, checkpoint.JSONSerializer[[]flowy.NoEffect]{},
		flowy.DurableOptions{Owner: "wait-worker", LeaseTTL: time.Minute, WaitProfile: &profile, Clock: clock})
	if err != nil {
		t.Fatal(err)
	}
	return runner
}

func TestWaitRegistrationPersistentRestartAndDiscovery(t *testing.T) {
	// Arrange: an earlier non-due head must not hide the next due generation.
	ctx, pool := racePool(t)
	if _, err := pool.Exec(ctx, ExecutionSchemaSQL()); err != nil {
		t.Fatal(err)
	}
	profile := postgresWaitProfile()
	profile.Label = testThreadID(t)
	store, err := NewWaitExecutionStore(pool, profile)
	if err != nil {
		t.Fatal(err)
	}
	base := testThreadID(t)
	deadline := time.Date(2026, 10, 5, 0, 0, 0, 0, time.UTC)
	var calls atomic.Int32
	if _, err = postgresWaitRunner(t, store, postgresWaitSpec(deadline.Add(time.Hour)), &calls).
		Start(ctx, base+"01", intState{}); err != nil {
		t.Fatal(err)
	}
	armed, err := postgresWaitRunner(t, store, postgresWaitSpec(deadline), &calls).Start(ctx, base+"02", intState{})
	if err != nil {
		t.Fatal(err)
	}
	pool.Close()
	restartCtx, restartPool := racePool(t)
	restarted, err := NewWaitExecutionStore(restartPool, profile)
	if err != nil {
		t.Fatal(err)
	}
	// Act: registration and due discovery use only the fresh pool's committed history.
	recovered, recoverErr := postgresWaitRunner(t, restarted, postgresWaitSpec(deadline), &calls).
		Resume(restartCtx, armed.ResumeToken)
	first, scanErr := restarted.DiscoverDueWaits(restartCtx, deadline, DiscoveryCursor{}, 1)
	if scanErr != nil {
		t.Fatal(scanErr)
	}
	// Assert: the index excludes the future head and returns the due generation directly.
	if recoverErr != nil || recovered.State.Value != 1 || recovered.ResumeToken != armed.ResumeToken ||
		calls.Load() != 2 || len(first.Waits) != 1 || first.More ||
		first.Waits[0].Wait.ExecutionID != base+"02" ||
		first.Waits[0].Revision != armed.ResumeToken.SnapshotRevision ||
		!first.Waits[0].Wait.Spec.Deadline.Equal(deadline) {
		t.Fatalf(
			"persistent indexed discovery: recovered=%+v err=%v page=%+v calls=%d",
			recovered,
			recoverErr,
			first,
			calls.Load(),
		)
	}
	changed := first.Waits[0].Wait
	changed.Spec.Deadline = changed.Spec.Deadline.Add(time.Hour)
	if changedErr := restarted.RegisterWait(restartCtx, changed); !errors.Is(changedErr, flowy.ErrWaitConflict) {
		t.Fatalf("changed registration contract accepted: %v", changedErr)
	}
}

type waitRegistrationFaultStore struct {
	*ExecutionStore

	fail atomic.Bool
}

func (s *waitRegistrationFaultStore) RegisterWait(ctx context.Context, record flowy.DurableWaitRecord) error {
	if err := s.ExecutionStore.RegisterWait(ctx, record); err != nil {
		return err
	}
	if s.fail.Swap(false) {
		return errors.New("injected registration acknowledgement loss")
	}
	return nil
}

func TestWaitRegistrationFailurePersistentRecovery(t *testing.T) {
	// Arrange: armed commit succeeds but the caller receives no registration acknowledgement.
	ctx, pool := racePool(t)
	if _, err := pool.Exec(ctx, ExecutionSchemaSQL()); err != nil {
		t.Fatal(err)
	}
	store, err := NewWaitExecutionStore(pool, postgresWaitProfile())
	if err != nil {
		t.Fatal(err)
	}
	fault := &waitRegistrationFaultStore{ExecutionStore: store}
	fault.fail.Store(true)
	var calls atomic.Int32
	spec := postgresWaitSpec(time.Date(2026, 10, 5, 0, 0, 0, 0, time.UTC))
	armed, err := postgresWaitRunner(t, fault, spec, &calls).Start(ctx, testThreadID(t), intState{})
	if !errors.Is(err, flowy.ErrWaitRegistration) || armed == nil || armed.ResumeToken.SnapshotRevision != 2 {
		t.Fatalf("registration acknowledgement fault missing: %+v err=%v", armed, err)
	}
	pool.Close()
	restartCtx, restartPool := racePool(t)
	restarted, err := NewWaitExecutionStore(restartPool, postgresWaitProfile())
	if err != nil {
		t.Fatal(err)
	}
	// Act.
	recovered, recoverErr := postgresWaitRunner(t, restarted, spec, &calls).Resume(restartCtx, armed.ResumeToken)
	// Assert.
	if recoverErr != nil || recovered.State.Value != 1 || recovered.Status != flowy.RunStatusSuspended ||
		recovered.ResumeToken != armed.ResumeToken || calls.Load() != 1 {
		t.Fatalf(
			"persistent registration recovery repeated node: %+v err=%v calls=%d",
			recovered,
			recoverErr,
			calls.Load(),
		)
	}
}
