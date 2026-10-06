package flowy_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/skosovsky/flowy"
	"github.com/skosovsky/flowy/testutil"
)

type forkInitialFaultStore struct {
	*testutil.MemoryExecutionStore

	fail atomic.Bool
}

func (s *forkInitialFaultStore) CommitExecution(ctx context.Context, revision uint64,
	lease flowy.ExecutionLease, envelope flowy.ExecutionEnvelope,
) (flowy.ExecutionEnvelope, error) {
	if envelope.Fork != nil && revision == 0 && s.fail.Swap(false) {
		return flowy.ExecutionEnvelope{}, errInjectedCommit
	}
	return s.MemoryExecutionStore.CommitExecution(ctx, revision, lease, envelope)
}

func TestForkInitialCommitFaultAcknowledgesNoTargetAndCanRetry(t *testing.T) {
	t.Parallel()
	// Arrange.
	ctx := context.Background()
	store := &forkInitialFaultStore{MemoryExecutionStore: testutil.NewMemoryExecutionStore(nil)}
	source := seedForkSource(t, store)
	var nodes, live atomic.Int32
	runner := forkRunnerForTest(t, store, nil, &nodes, &live)
	request := forkRequestForTest(source, "target")
	store.fail.Store(true)
	// Act.
	token, err := runner.Fork(ctx, request)
	_, loadErr := store.LoadExecution(ctx, "target")
	// Assert: failed commit neither returns a token nor makes target runnable.
	if !errors.Is(err, errInjectedCommit) || token != (flowy.ResumeToken{}) ||
		!errors.Is(loadErr, flowy.ErrThreadNotFound) ||
		nodes.Load() != 0 ||
		live.Load() != 0 {
		t.Fatalf("failed fork exposed target: token=%+v err=%v load=%v", token, err, loadErr)
	}
	lease, leaseErr := store.AcquireExecution(ctx, "target", "release-probe", time.Minute)
	if leaseErr != nil {
		t.Fatalf("failed fork retained worker: %v", leaseErr)
	}
	if leaseErr = store.ReleaseExecution(ctx, lease); leaseErr != nil {
		t.Fatal(leaseErr)
	}
	retried, retryErr := runner.Fork(ctx, request)
	retained, retainedErr := store.LoadExecution(ctx, "source")
	if retryErr != nil || retried.SnapshotRevision != 1 || retainedErr != nil || retained.Digest != source.Digest ||
		nodes.Load() != 0 {
		t.Fatalf(
			"fork retry changed source or skipped boundary: %+v err=%v source=%+v/%v",
			retried,
			retryErr,
			retained,
			retainedErr,
		)
	}
}

func TestForkExistingActiveTargetRejectsWithoutTransform(t *testing.T) {
	t.Parallel()
	// Arrange.
	ctx := context.Background()
	store := testutil.NewMemoryExecutionStore(nil)
	source := seedForkSource(t, store)
	var nodes, live, transforms atomic.Int32
	runner := forkRunnerForTest(t, store, nil, &nodes, &live)
	request := forkRequestForTest(source, "target")
	if _, err := runner.Fork(ctx, request); err != nil {
		t.Fatal(err)
	}
	head, err := store.LoadExecution(ctx, "target")
	if err != nil {
		t.Fatal(err)
	}
	lease, err := store.AcquireExecution(ctx, "target", "active-target", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	request.Transform.Transform = func(state flowy.ExecutionProgress) (flowy.ExecutionProgress, error) {
		transforms.Add(1)
		return state, nil
	}
	// Act: existing committed identity is a target conflict even while leased.
	_, conflict := runner.Fork(ctx, request)
	after, loadErr := store.LoadExecution(ctx, "target")
	// Assert.
	if !errors.Is(conflict, flowy.ErrForkTargetConflict) || transforms.Load() != 0 || loadErr != nil ||
		after.Digest != head.Digest {
		t.Fatalf("active target overwritten/misclassified: err=%v after=%+v/%v", conflict, after, loadErr)
	}
	if err = store.ReleaseExecution(ctx, lease); err != nil {
		t.Fatal(err)
	}
}

func TestForkLiveAuthorizationIsRecheckedAtDispatch(t *testing.T) {
	t.Parallel()
	// Arrange: creation and Resume are authorized, then the dispatch gate refuses.
	ctx := context.Background()
	store := testutil.NewMemoryExecutionStore(nil)
	source := seedForkSource(t, store)
	var nodes, live, gates atomic.Int32
	policy := &flowy.ForkExecutionPolicy{Label: "short-lived-permission", Mode: flowy.ForkLive,
		Authorize: func(context.Context, flowy.ForkLineage) error {
			if gates.Add(1) > 2 {
				return errors.New("permission expired before dispatch")
			}
			return nil
		}}
	runner := forkRunnerForTest(t, store, policy, &nodes, &live)
	request := forkRequestForTest(source, "target")
	request.Mode, request.PolicyLabel = flowy.ForkLive, policy.Label
	request.Projection = &flowy.ForkProjection{
		Label: "clear-permission",
		Project: func(state flowy.ExecutionProgress) (flowy.ExecutionProgress, error) {
			state.StatePayload = []byte(`{"Value":5,"Approved":false}`)
			return state, nil
		},
	}
	token, err := runner.Fork(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	// Act.
	_, resumeErr := runner.Resume(ctx, token)
	head, loadErr := store.LoadExecution(ctx, "target")
	// Assert: no external dispatch; an attempted boundary remains recoverable, not successful.
	if !errors.Is(resumeErr, flowy.ErrForkPolicy) || !errors.Is(resumeErr, flowy.ErrActivityUnknown) ||
		live.Load() != 0 ||
		nodes.Load() != 1 ||
		gates.Load() != 3 ||
		loadErr != nil ||
		head.Terminal != nil ||
		head.Activation != 1 {
		t.Fatalf(
			"stale authorization dispatched/committed success: err=%v live=%d gates=%d head=%+v/%v",
			resumeErr,
			live.Load(),
			gates.Load(),
			head,
			loadErr,
		)
	}
}
