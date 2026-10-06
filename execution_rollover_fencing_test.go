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

func TestLateRolloverProjectionLosesFenceWithoutPublishing(t *testing.T) {
	// Arrange: a projector that does not cooperate with ownership loss.
	var nanos atomic.Int64
	nanos.Store(time.Now().UnixNano())
	store := testutil.NewMemoryExecutionStore(func() time.Time { return time.Unix(0, nanos.Load()) })
	ctx := context.Background()
	var calls, projections atomic.Int32
	runner := lifecycleRunner(t, store, 1, &calls)
	paused, err := runner.Start(ctx, "source", 0)
	if err != nil {
		t.Fatal(err)
	}
	before, err := store.LoadExecution(ctx, "source")
	if err != nil {
		t.Fatal(err)
	}
	entered, release := make(chan struct{}), make(chan struct{})
	request := lifecycleRolloverRequest("target", 1, &projections)
	request.Project = wrapRolloverProjection(
		request.Project,
		func(p flowy.RolloverPayload) (flowy.RolloverPayload, error) { close(entered); <-release; return p, nil },
	)
	done := make(chan error, 1)
	go func() { _, rolloverErr := runner.Rollover(ctx, paused.ResumeToken, request); done <- rolloverErr }()
	<-entered
	// Act: elapsed TTL permits another owner; it does not stop the old projector.
	nanos.Add(int64(2 * time.Minute))
	successor, err := store.AcquireExecution(ctx, "source", "successor", time.Minute)
	if err != nil {
		close(release)
		<-done
		t.Fatal(err)
	}
	close(release)
	lateErr := <-done
	after, loadErr := store.LoadExecution(ctx, "source")
	_, absentErr := store.LoadExecution(ctx, "target")
	if err = store.ReleaseExecution(ctx, successor); err != nil {
		t.Fatal(err)
	}
	// Assert: only a fresh explicit attempt may publish; late completion owns no authority.
	if !errors.Is(lateErr, flowy.ErrLeaseLost) || loadErr != nil || before.Digest != after.Digest ||
		!errors.Is(absentErr, flowy.ErrThreadNotFound) ||
		calls.Load() != 1 ||
		projections.Load() != 1 {
		t.Fatalf(
			"late=%v load=%v target=%v calls/projections=%d/%d",
			lateErr,
			loadErr,
			absentErr,
			calls.Load(),
			projections.Load(),
		)
	}
	_, err = runner.Rollover(ctx, paused.ResumeToken, lifecycleRolloverRequest("target", 1, &projections))
	if err != nil || projections.Load() != 2 || calls.Load() != 1 {
		t.Fatalf("explicit retry=%v calls/projections=%d/%d", err, calls.Load(), projections.Load())
	}
}

func TestOrdinaryCommitCannotReplaceRolloverAuthority(t *testing.T) {
	// Arrange: one legitimate atomic publication.
	ctx := context.Background()
	store := testutil.NewMemoryExecutionStore(nil)
	var calls, projections atomic.Int32
	runner := lifecycleRunner(t, store, 1, &calls)
	paused, err := runner.Start(ctx, "source", 0)
	if err != nil {
		t.Fatal(err)
	}
	token, err := runner.Rollover(ctx, paused.ResumeToken, lifecycleRolloverRequest("target", 1, &projections))
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"source", "target"} {
		head, loadErr := store.LoadExecution(ctx, id)
		if loadErr != nil {
			t.Fatal(loadErr)
		}
		lease, leaseErr := store.AcquireExecution(ctx, id, "malicious", time.Minute)
		if leaseErr != nil {
			t.Fatal(leaseErr)
		}
		forged := flowy.CloneExecutionEnvelope(head)
		forged.Rollover = nil
		forged.Transfer = nil
		forged.Terminal = nil
		// Act: ordinary writes cannot revive source or erase target creation authority.
		_, rejected := store.CommitExecution(ctx, head.Revision, lease, forged)
		after, afterErr := store.LoadExecution(ctx, id)
		if err = store.ReleaseExecution(ctx, lease); err != nil {
			t.Fatal(err)
		}
		// Assert: exact before-image remains authoritative on both sides.
		expected := flowy.ErrExecutionCorrupt
		if id == "source" {
			expected = flowy.ErrExecutionTransferred
		}
		if !errors.Is(rejected, expected) || afterErr != nil || after.Digest != head.Digest {
			t.Fatalf("id=%s rejected=%v load=%v", id, rejected, afterErr)
		}
	}
	replay, err := runner.Rollover(ctx, paused.ResumeToken, lifecycleRolloverRequest("target", 1, &projections))
	if err != nil || replay != token || calls.Load() != 1 || projections.Load() != 1 {
		t.Fatalf("replay=%v token=%+v", err, replay)
	}
}
