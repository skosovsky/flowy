package flowy

import (
	"context"
	"errors"
	"testing"
	"time"
)

type cleanupProbeCP struct {
	*memoryCP[int, NoEffect]

	failure         error
	cleanupContexts []context.Context
}

func (p *cleanupProbeCP) Prune(ctx context.Context, _ string, _ int) error {
	p.cleanupContexts = append(p.cleanupContexts, ctx)
	return p.failure
}
func (p *cleanupProbeCP) DeleteIfIdle(ctx context.Context, _ string) error {
	p.cleanupContexts = append(p.cleanupContexts, ctx)
	return p.failure
}

type cleanupProbeLease struct {
	LeaseManager

	failure         error
	releaseContexts []context.Context
}

func (p *cleanupProbeLease) Release(ctx context.Context, lease ExecutionLease) error {
	p.releaseContexts = append(p.releaseContexts, ctx)
	return errors.Join(p.LeaseManager.Release(ctx, lease), p.failure)
}
func assertBoundedCleanupContext(ctx context.Context, t *testing.T) {
	t.Helper()
	deadline, ok := ctx.Deadline()
	if !ok || time.Until(deadline) > contextCancelSaveTimeout || ctx.Err() != context.Canceled {
		t.Fatalf("cleanup deadline=%v has=%v finished error=%v", deadline, ok, ctx.Err())
	}
}
func TestPostRunCleanupReturnsBoundedPolicyAndLeaseFailures(t *testing.T) {
	t.Parallel()
	for _, policy := range []string{"prune", "delete"} {
		t.Run(policy, func(t *testing.T) {
			t.Parallel()
			// Arrange: cleanup errors must preserve a completed execution outcome.
			policyErr := errors.New("policy unavailable")
			releaseErr := errors.New("release unavailable")
			cp := &cleanupProbeCP{memoryCP: newMemoryCP[int, NoEffect](), failure: policyErr, cleanupContexts: nil}
			lease := &cleanupProbeLease{
				LeaseManager:    NewMemoryLeaseManager(),
				failure:         releaseErr,
				releaseContexts: nil,
			}
			b := NewGraph[int, NoEffect](func(_, update int) int { return update })
			b.AddNode("done", func(_ context.Context, state int) (int, Directive, error) { return state, End(), nil }).
				SetEntryPoint("done").
				AllowNoOutgoingRoute("done")
			option := WithRetentionLimit(2)
			if policy == "delete" {
				option = WithDeleteOnSuccess(true)
			}
			graph, err := b.Compile(option)
			if err != nil {
				t.Fatal(err)
			}
			runner := graph.NewRunnerWithOptions(
				cp,
				[]RunnerOption[int, NoEffect]{WithLeaseManager[int, NoEffect](lease)},
			)
			// Act.
			result, err := runner.Start(
				context.Background(),
				"cleanup",
				42,
				WithRunLease[int, NoEffect]("worker", time.Minute),
			)
			// Assert.
			if result == nil || result.Status != RunStatusCompleted || !errors.Is(err, ErrRunCleanup) ||
				!errors.Is(err, releaseErr) ||
				!errors.Is(err, policyErr) {
				t.Fatalf("result=%+v err=%v", result, err)
			}
			if len(lease.releaseContexts) != 1 || len(cp.cleanupContexts) != 1 {
				t.Fatalf("release=%d policy=%d", len(lease.releaseContexts), len(cp.cleanupContexts))
			}
			assertBoundedCleanupContext(lease.releaseContexts[0], t)
			assertBoundedCleanupContext(cp.cleanupContexts[0], t)
		})
	}
}
func TestRejectedResumeJoinsBoundedReleaseFailure(t *testing.T) {
	t.Parallel()
	// Arrange: resume admission fails after acquiring ownership.
	releaseErr := errors.New("release unavailable")
	lease := &cleanupProbeLease{LeaseManager: NewMemoryLeaseManager(), failure: releaseErr, releaseContexts: nil}
	runner := ownershipGraph(
		t,
		func(_ context.Context, state int) (int, Directive, error) { return state, End(), nil },
	).NewRunnerWithOptions(newMemoryCP[int, NoEffect](), []RunnerOption[int, NoEffect]{WithLeaseManager[int, NoEffect](lease)})
	// Act.
	result, err := runner.Resume(
		context.Background(),
		ResumeToken{ThreadID: "missing", SnapshotRevision: 1},
		WithRunLease[int, NoEffect]("worker", time.Minute),
	)
	// Assert.
	if result != nil || !errors.Is(err, ErrThreadNotFound) || !errors.Is(err, ErrRunCleanup) ||
		!errors.Is(err, releaseErr) {
		t.Fatalf("result=%+v err=%v", result, err)
	}
	if len(lease.releaseContexts) != 1 {
		t.Fatal("admission leaked lease")
	}
	assertBoundedCleanupContext(lease.releaseContexts[0], t)
}
func TestCanceledRunCleanupDetachesButPreservesValuesAndError(t *testing.T) {
	t.Parallel()
	// Arrange: the run's canceled context cannot be reused as cleanup authority.
	type key struct{}
	parent, cancel := context.WithCancel(context.WithValue(context.Background(), key{}, "trace"))
	releaseErr := errors.New("release unavailable")
	lease := &cleanupProbeLease{LeaseManager: NewMemoryLeaseManager(), failure: releaseErr, releaseContexts: nil}
	graph := ownershipGraph(t, func(ctx context.Context, state int) (int, Directive, error) {
		cancel()
		<-ctx.Done()
		return state, End(), ctx.Err()
	})
	runner := graph.NewRunnerWithOptions(
		newMemoryCP[int, NoEffect](),
		[]RunnerOption[int, NoEffect]{WithLeaseManager[int, NoEffect](lease)},
	)
	defer cancel()
	// Act.
	result, err := runner.Start(parent, "cancel", 42, WithRunLease[int, NoEffect]("worker", time.Minute))
	// Assert.
	if result.Status != RunStatusContextCanceled || !errors.Is(err, context.Canceled) || !errors.Is(err, releaseErr) ||
		!errors.Is(err, ErrRunCleanup) {
		t.Fatalf("result=%+v err=%v", result, err)
	}
	if len(lease.releaseContexts) != 1 || lease.releaseContexts[0].Value(key{}) != "trace" {
		t.Fatal("cleanup lost context values")
	}
	assertBoundedCleanupContext(lease.releaseContexts[0], t)
}

func TestStreamConsumerStopPreservesCleanupFailure(t *testing.T) {
	t.Parallel()
	// Arrange: consumer stop checkpoints successfully, then release reports failure.
	ready := make(chan struct{})
	releaseErr := errors.New("release unavailable")
	lease := &cleanupProbeLease{LeaseManager: NewMemoryLeaseManager(), failure: releaseErr, releaseContexts: nil}
	graph := ownershipGraph(t, func(ctx context.Context, state int) (int, Directive, error) {
		close(ready)
		<-ctx.Done()
		return state, End(), ctx.Err()
	})
	runner := graph.NewRunnerWithOptions(
		newMemoryCP[int, NoEffect](),
		[]RunnerOption[int, NoEffect]{WithLeaseManager[int, NoEffect](lease)},
	)
	handle, err := runner.Stream(
		context.Background(),
		"stop-cleanup",
		42,
		WithRunLease[int, NoEffect]("worker", time.Minute),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(handle.RequestStop)
	<-ready
	// Act.
	handle.RequestStop()
	result, waitErr := handle.WaitResult()
	// Assert: local-stop normalization must not erase the joined cleanup error.
	if result == nil || result.Status != RunStatusContextCanceled || !errors.Is(waitErr, ErrRunCleanup) ||
		!errors.Is(waitErr, releaseErr) {
		t.Fatalf("result=%+v err=%v", result, waitErr)
	}
}
