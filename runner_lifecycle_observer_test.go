package flowy

import (
	"context"
	"errors"
	"testing"
)

func TestLifecycleObserverResumeRejected(t *testing.T) {
	type state struct{}
	obs := &spyLifecycleObserver{}
	SetLifecycleObserver(obs)
	t.Cleanup(func() { SetLifecycleObserver(nil) })

	b := NewGraph[state, NoEffect](func(_ state, u state) state { return u })
	b.AddNode("work", func(_ context.Context, s state) (state, Directive, error) {
		return s, End(), nil
	})
	b.AllowNoOutgoingRoute("work")
	b.SetEntryPoint("work")
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	cp := newMemoryCP[state, NoEffect]()
	if _, saveErr := cp.Save(context.Background(), 0, Snapshot[state, NoEffect]{
		ThreadID:         "resume-reject-th",
		ExecutionPointer: "work",
		State:            state{},
	}); saveErr != nil {
		t.Fatalf("seed: %v", saveErr)
	}

	_, err = g.NewRunner(cp).Resume(context.Background(), ResumeToken{
		ThreadID:         "resume-reject-th",
		SnapshotRevision: 99,
	})
	if !errors.Is(err, ErrConcurrencyConflict) {
		t.Fatalf("expected ErrConcurrencyConflict, got %v", err)
	}

	obs.mu.Lock()
	defer obs.mu.Unlock()
	if len(obs.rejected) != 1 || obs.rejected[0] != "stale_token" {
		t.Fatalf("expected resume_rejected stale_token, got %+v", obs.rejected)
	}
}

func TestLifecycleObserverCheckpointSoftError(t *testing.T) {
	type state struct{}
	obs := &spyLifecycleObserver{}
	SetLifecycleObserver(obs)
	t.Cleanup(func() { SetLifecycleObserver(nil) })

	b := NewGraph[state, NoEffect](func(_ state, u state) state { return u })
	b.AddNode("work", func(_ context.Context, s state) (state, Directive, error) {
		return s, Suspend("hold"), nil
	})
	b.AllowNoOutgoingRoute("work")
	b.SetEntryPoint("work")
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	cp := &failingMemoryCP[state, NoEffect]{failSave: true}
	res, err := g.NewRunner(cp).Start(context.Background(), "soft-error-th", state{},
		WithCheckpointErrorPolicy[state, NoEffect](CheckpointPolicySkipOnSaveError),
	)
	if err != nil {
		t.Fatalf("expected nil error on skip-on-save suspend, got %v", err)
	}
	if res == nil || res.Status != RunStatusSuspended {
		t.Fatalf("expected suspended result, got %+v", res)
	}

	obs.mu.Lock()
	defer obs.mu.Unlock()
	if obs.checkpointSofts != 1 {
		t.Fatalf("expected one checkpoint_soft_error metric, got %d", obs.checkpointSofts)
	}
}

func TestLifecycleObserverHandoffPatchEnqueuedFailed(t *testing.T) {
	obs := &spyLifecycleObserver{}
	SetLifecycleObserver(obs)
	t.Cleanup(func() { SetLifecycleObserver(nil) })

	type state struct{}
	outbox := &stubHandoffOutbox{}
	cp := &handoffPatchFailCP[state, NoEffect]{failOnStatus: HandoffStatusEnqueued}
	b := NewGraph[state, NoEffect](func(_ state, u state) state { return u })
	b.AddNode("work", func(_ context.Context, s state) (state, Directive, error) {
		return s, Handoff("bg"), nil
	})
	b.AllowNoOutgoingRoute("work")
	b.SetEntryPoint("work")
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	_, _ = g.NewRunner(cp).Start(context.Background(), "obs-patch-enq-th", state{},
		WithHandoffOutbox[state, NoEffect](outbox),
	)
	obs.mu.Lock()
	defer obs.mu.Unlock()
	last := obs.enqueued[len(obs.enqueued)-1]
	if last != "patch_enqueued_failed" {
		t.Fatalf("expected patch_enqueued_failed metric, got %+v", obs.enqueued)
	}
}

func TestLifecycleObserverHandoffPatchOrphanFailed(t *testing.T) {
	obs := &spyLifecycleObserver{}
	SetLifecycleObserver(obs)
	t.Cleanup(func() { SetLifecycleObserver(nil) })

	type state struct{}
	cp := &handoffPatchFailCP[state, NoEffect]{
		failOnStatuses: map[HandoffStatus]struct{}{ //nolint:exhaustive // patch statuses only
			HandoffStatusEnqueued: {},
			HandoffStatusOrphaned: {},
		},
	}
	b := NewGraph[state, NoEffect](func(_ state, u state) state { return u })
	b.AddNode("work", func(_ context.Context, s state) (state, Directive, error) {
		return s, Handoff("bg"), nil
	})
	b.AllowNoOutgoingRoute("work")
	b.SetEntryPoint("work")
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	_, _ = g.NewRunner(cp).Start(context.Background(), "obs-patch-orphan-th", state{},
		WithHandoffOutbox[state, NoEffect](&stubHandoffOutbox{}),
	)
	obs.mu.Lock()
	defer obs.mu.Unlock()
	last := obs.enqueued[len(obs.enqueued)-1]
	if last != "patch_orphan_failed" {
		t.Fatalf("expected patch_orphan_failed metric, got %+v", obs.enqueued)
	}
}
