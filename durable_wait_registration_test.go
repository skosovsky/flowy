package flowy_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/skosovsky/flowy"
	"github.com/skosovsky/flowy/checkpoint"
	"github.com/skosovsky/flowy/testutil"
)

type waitRegistrationStore struct {
	*faultExecutionStore

	profile          flowy.WaitCapabilityProfile
	registrations    atomic.Int32
	failRegistration atomic.Bool
}

func waitProfileForTest() flowy.WaitCapabilityProfile {
	return flowy.WaitCapabilityProfile{Label: "wait-test", JournalOwner: "store", LeaseOwner: "store",
		TimerOwner: "timer", ClockOwner: "clock", RetryOwner: "runtime", RecoveryOwner: "runtime"}
}

func (s *waitRegistrationStore) WaitCapabilities() flowy.WaitCapabilityProfile { return s.profile }

func (s *waitRegistrationStore) RegisterWait(ctx context.Context, expected flowy.DurableWaitRecord) error {
	s.registrations.Add(1)
	envelope, err := s.LoadExecution(ctx, expected.ExecutionID)
	if err != nil {
		return err
	}
	records, err := flowy.InspectExecutionWaits(envelope)
	if err != nil {
		return err
	}
	for _, record := range records {
		if record.Generation == expected.Generation && record.State == flowy.WaitArmed &&
			record.ArmRevision == expected.ArmRevision && record.Profile == s.profile {
			if s.failRegistration.Swap(false) {
				return errInjectedCommit
			}
			return nil
		}
	}
	return flowy.ErrWaitNotArmed
}

func newWaitRegistrationStore() *waitRegistrationStore {
	return &waitRegistrationStore{faultExecutionStore: &faultExecutionStore{
		ExecutionStore: testutil.NewMemoryExecutionStore(nil)}, profile: waitProfileForTest()}
}

func waitSpecForTest() flowy.DurableWaitSpec {
	return flowy.DurableWaitSpec{ID: "approval", CorrelationID: "request",
		Deadline: time.Date(2026, 10, 5, 0, 0, 0, 0, time.UTC), MatcherLabel: "match",
		PayloadCodec: "payload", ContinuationLabel: "transition", EventPointer: "accepted",
		TimeoutPointer: "timed-out", WinnerPolicy: flowy.WaitFirstCommitted}
}

func waitRunnerForTest(t *testing.T, store flowy.ExecutionStore, profile *flowy.WaitCapabilityProfile,
	calls *atomic.Int32, codec flowy.StateSerializer[durableTestState], clocks ...flowy.ExecutionClock,
) (*flowy.DurableRunner[durableTestState, flowy.NoEffect], error) {
	t.Helper()
	b := flowy.NewGraph[durableTestState, flowy.NoEffect](
		func(_, update durableTestState) durableTestState { return update })
	b.AddNode("waiting", func(_ context.Context, state durableTestState) (durableTestState, flowy.Directive, error) {
		calls.Add(1)
		state.Value++
		return state, flowy.Effect(flowy.Await(waitSpecForTest()), flowy.NoEffect{}), nil
	})
	for _, id := range []string{"accepted", "timed-out"} {
		b.AddNode(id, func(_ context.Context, state durableTestState) (durableTestState, flowy.Directive, error) {
			calls.Add(1)
			return state, flowy.End(), nil
		}).AllowNoOutgoingRoute(id)
	}
	b.AllowNoOutgoingRoute("waiting").SetEntryPoint("waiting")
	graph, err := b.Compile()
	if err != nil {
		t.Fatal(err)
	}
	var clock flowy.ExecutionClock
	if len(clocks) != 0 {
		clock = clocks[0]
	}
	return flowy.NewDurableRunner(graph, store, durableDescriptor("current"), codec,
		checkpoint.JSONSerializer[[]flowy.NoEffect]{}, flowy.DurableOptions{
			Owner: "worker", LeaseTTL: time.Minute, WaitProfile: profile, Clock: clock})
}

func mustWaitRunner(t *testing.T, store flowy.ExecutionStore, profile *flowy.WaitCapabilityProfile,
	calls *atomic.Int32,
) *flowy.DurableRunner[durableTestState, flowy.NoEffect] {
	t.Helper()
	runner, err := waitRunnerForTest(t, store, profile, calls, checkpoint.JSONSerializer[durableTestState]{})
	if err != nil {
		t.Fatal(err)
	}
	return runner
}

func TestDurableWaitAtomicArmAndRecoveryWithoutNodePolling(t *testing.T) {
	t.Parallel()
	// Arrange.
	ctx := context.Background()
	store := newWaitRegistrationStore()
	var calls atomic.Int32
	profile := store.profile
	runner := mustWaitRunner(t, store, &profile, &calls)
	profile.Label = "mutated-host-variable"
	// Act: commit reduced state + effect + arm, release owner, then recover registration.
	armed, err := runner.Start(ctx, "run", durableTestState{Value: 10})
	if err != nil {
		t.Fatal(err)
	}
	lease, leaseErr := store.AcquireExecution(ctx, "run", "probe", time.Minute)
	if leaseErr != nil {
		t.Fatalf("wait retained worker lease: %v", leaseErr)
	}
	if releaseErr := store.ReleaseExecution(ctx, lease); releaseErr != nil {
		t.Fatal(releaseErr)
	}
	recovered, recoverErr := runner.Resume(ctx, armed.ResumeToken)
	// Assert.
	if recoverErr != nil || recovered.Status != flowy.RunStatusSuspended || recovered.State.Value != 11 ||
		len(recovered.Effects) != 1 || recovered.ResumeToken != armed.ResumeToken || calls.Load() != 1 ||
		store.registrations.Load() != 2 || store.commits.Load() != 2 {
		t.Fatalf("wait recovery lost state or polled node: %+v err=%v calls=%d registrations=%d commits=%d",
			recovered, recoverErr, calls.Load(), store.registrations.Load(), store.commits.Load())
	}
	envelope, loadErr := store.LoadExecution(ctx, "run")
	if loadErr != nil {
		t.Fatal(loadErr)
	}
	records, inspectErr := flowy.InspectExecutionWaits(envelope)
	if inspectErr != nil || len(records) != 1 || records[0].ArmRevision != 2 || records[0].Profile != store.profile ||
		!records[0].Spec.Deadline.Equal(waitSpecForTest().Deadline) || envelope.Terminal != nil {
		t.Fatalf("atomic arm missing: %+v err=%v", records, inspectErr)
	}
}

func TestDurableWaitRegistrationFailureRecoversCommittedState(t *testing.T) {
	t.Parallel()
	// Arrange.
	store := newWaitRegistrationStore()
	store.failRegistration.Store(true)
	var calls atomic.Int32
	runner := mustWaitRunner(t, store, &store.profile, &calls)
	// Act.
	armed, err := runner.Start(context.Background(), "run", durableTestState{})
	if !errors.Is(err, flowy.ErrWaitRegistration) || armed == nil || armed.State.Value != 1 ||
		armed.ResumeToken.SnapshotRevision != 2 || armed.Status != flowy.RunStatusSuspended {
		t.Fatalf("registration failure discarded boundary: %+v err=%v", armed, err)
	}
	recovered, recoverErr := runner.Resume(context.Background(), armed.ResumeToken)
	// Assert.
	if recoverErr != nil || recovered.State.Value != 1 || recovered.ResumeToken != armed.ResumeToken ||
		calls.Load() != 1 || store.registrations.Load() != 2 || store.commits.Load() != 2 {
		t.Fatalf("registration recovery re-executed: %+v err=%v", recovered, recoverErr)
	}
}

func TestDurableWaitArmCommitFailureNeverRegisters(t *testing.T) {
	t.Parallel()
	// Arrange.
	store := newWaitRegistrationStore()
	store.failAt = 2
	var calls atomic.Int32
	runner := mustWaitRunner(t, store, &store.profile, &calls)
	// Act.
	failed, err := runner.Start(context.Background(), "run", durableTestState{Value: 7})
	// Assert: no partial state or armed wait is published, and no registration promise made.
	if !errors.Is(err, errInjectedCommit) || failed == nil || store.registrations.Load() != 0 ||
		failed.ResumeToken.SnapshotRevision != 1 {
		t.Fatalf("failed arm registered: %+v err=%v registrations=%d", failed, err, store.registrations.Load())
	}
	envelope, loadErr := store.LoadExecution(context.Background(), "run")
	if loadErr != nil {
		t.Fatal(loadErr)
	}
	state, decodeErr := (checkpoint.JSONSerializer[durableTestState]{}).Unmarshal(envelope.Progress.StatePayload)
	if decodeErr != nil {
		t.Fatal(decodeErr)
	}
	if state.Value != 7 || len(envelope.WaitsPayload) != 0 || envelope.Terminal != nil {
		t.Fatalf("partial arm visible: %+v state=%+v", envelope, state)
	}
}

func TestDurableWaitDisabledProfileRejectsBeforeRegistration(t *testing.T) {
	t.Parallel()
	// Arrange.
	store := newWaitRegistrationStore()
	var calls atomic.Int32
	runner := mustWaitRunner(t, store, nil, &calls)
	// Act.
	result, err := runner.Start(context.Background(), "run", durableTestState{})
	// Assert.
	if !errors.Is(err, flowy.ErrExecutionCapability) || result == nil || result.ResumeToken.SnapshotRevision != 1 ||
		store.commits.Load() != 1 || store.registrations.Load() != 0 {
		t.Fatalf("disabled wait capability acknowledged: %+v err=%v", result, err)
	}
}

func TestDurableWaitWrongProfileRejectsBeforeHostDecode(t *testing.T) {
	t.Parallel()
	// Arrange.
	store := newWaitRegistrationStore()
	var calls, decodes atomic.Int32
	runner := mustWaitRunner(t, store, &store.profile, &calls)
	armed, err := runner.Start(context.Background(), "run", durableTestState{})
	if err != nil {
		t.Fatal(err)
	}
	store.profile.Label = "replacement-profile"
	replacement, buildErr := waitRunnerForTest(t, store, &store.profile, &calls, failingDecode{calls: &decodes})
	if buildErr != nil {
		t.Fatal(buildErr)
	}
	// Act.
	_, err = replacement.Resume(context.Background(), armed.ResumeToken)
	// Assert.
	if !errors.Is(err, flowy.ErrExecutionCapability) || decodes.Load() != 0 || calls.Load() != 1 ||
		store.registrations.Load() != 1 || store.commits.Load() != 2 {
		t.Fatalf("incompatible wait decoded/dispatched: err=%v decodes=%d calls=%d", err, decodes.Load(), calls.Load())
	}
}

func TestDurableWaitUnsupportedAndMismatchedBackendRejectConstructor(t *testing.T) {
	t.Parallel()
	// Arrange.
	var calls atomic.Int32
	profile := waitProfileForTest()
	store := newWaitRegistrationStore()
	profile.Label = "wrong"
	// Act.
	_, mismatch := waitRunnerForTest(t, store, &profile, &calls, checkpoint.JSONSerializer[durableTestState]{})
	_, absent := waitRunnerForTest(t, testutil.NewMemoryExecutionStore(nil), &profile, &calls,
		checkpoint.JSONSerializer[durableTestState]{})
	// Assert.
	if !errors.Is(mismatch, flowy.ErrExecutionCapability) || !errors.Is(absent, flowy.ErrExecutionCapability) ||
		calls.Load() != 0 || store.registrations.Load() != 0 || store.commits.Load() != 0 {
		t.Fatalf("incompatible backend enabled: mismatch=%v absent=%v", mismatch, absent)
	}
}

type changingWaitProfileStore struct {
	*waitRegistrationStore

	reads atomic.Int32
}

func (s *changingWaitProfileStore) WaitCapabilities() flowy.WaitCapabilityProfile {
	profile := s.profile
	if s.reads.Add(1) > 1 {
		profile.Label = "changed-after-construction"
	}
	return profile
}

func TestDurableWaitChangedBackendProfileCannotArm(t *testing.T) {
	t.Parallel()
	// Arrange: the store declares the configured profile only during construction.
	store := &changingWaitProfileStore{waitRegistrationStore: newWaitRegistrationStore()}
	var calls atomic.Int32
	runner := mustWaitRunner(t, store, &store.profile, &calls)
	// Act.
	result, err := runner.Start(context.Background(), "run", durableTestState{})
	// Assert: core does not silently adopt the replacement owner assignment.
	if !errors.Is(err, flowy.ErrExecutionCapability) || result != nil || calls.Load() != 0 ||
		store.commits.Load() != 0 || store.registrations.Load() != 0 {
		t.Fatalf("changed backend profile armed: %+v err=%v", result, err)
	}
}

func TestDurableWaitStreamAcknowledgesOnlyCommittedArmAndReplaysWithoutNode(t *testing.T) {
	t.Parallel()
	// Arrange.
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	store := newWaitRegistrationStore()
	var calls atomic.Int32
	runner := mustWaitRunner(t, store, &store.profile, &calls)
	// Act.
	handle, err := runner.Stream(ctx, "run", durableTestState{})
	if err != nil {
		t.Fatal(err)
	}
	assertWaitStreamCommitted(ctx, t, store, handle)
	armed, err := handle.WaitResult()
	if err != nil {
		t.Fatal(err)
	}
	replayed, err := runner.ResumeStream(ctx, armed.ResumeToken)
	if err != nil {
		t.Fatal(err)
	}
	assertWaitStreamCommitted(ctx, t, store, replayed)
	recovered, recoverErr := replayed.WaitResult()
	// Assert.
	if recoverErr != nil || recovered.ResumeToken != armed.ResumeToken || recovered.State.Value != 1 ||
		calls.Load() != 1 || store.registrations.Load() != 2 || store.commits.Load() != 2 {
		t.Fatalf("waiting stream replay dispatched: %+v err=%v calls=%d", recovered, recoverErr, calls.Load())
	}
}

func assertWaitStreamCommitted(ctx context.Context, t *testing.T, store *waitRegistrationStore,
	handle flowy.StreamHandle[durableTestState, flowy.NoEffect],
) {
	t.Helper()
	acknowledged := 0
	for event := range handle.Events() {
		if event.Type != flowy.EventSuspended {
			continue
		}
		acknowledged++
		envelope, err := store.LoadExecution(ctx, "run")
		if err != nil {
			t.Fatal(err)
		}
		records, inspectErr := flowy.InspectExecutionWaits(envelope)
		if inspectErr != nil || len(records) != 1 || records[0].State != flowy.WaitArmed ||
			envelope.Revision != 2 || envelope.Terminal != nil || event.State.Value != 1 {
			t.Fatalf("stream acknowledged uncommitted arm: event=%+v records=%+v err=%v", event, records, inspectErr)
		}
	}
	if acknowledged != 1 {
		t.Fatalf("expected one committed wait acknowledgement, got %d", acknowledged)
	}
}
