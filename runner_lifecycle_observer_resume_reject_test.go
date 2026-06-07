package flowy

import (
	"context"
	"testing"
	"time"
)

type resumeRejectState struct{}

type resumeRejectCase struct {
	name       string
	setup      func(t *testing.T) (Runner[resumeRejectState, NoEffect], ResumeToken)
	wantReason string
}

func TestLifecycleObserverResumeRejectedMatrix(t *testing.T) {
	cases := []resumeRejectCase{
		{name: "zero_revision", setup: resumeRejectSetupZeroRevision, wantReason: "zero_revision"},
		{name: "handoff_pending", setup: resumeRejectSetupHandoffPending, wantReason: "handoff_pending"},
		{name: "handoff_orphaned", setup: resumeRejectSetupHandoffOrphaned, wantReason: "handoff_orphaned"},
		{name: "empty_token", setup: resumeRejectSetupEmptyToken, wantReason: "empty_token"},
		{
			name: "invalid_handoff_status", setup: resumeRejectSetupInvalidHandoffStatus,
			wantReason: "invalid_handoff_status",
		},
		{name: "invalid_snapshot", setup: resumeRejectSetupInvalidSnapshot, wantReason: "invalid_snapshot"},
		{name: "invalid_pointer", setup: resumeRejectSetupInvalidPointer, wantReason: "invalid_pointer"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assertResumeRejectedObserver(t, tc.setup, tc.wantReason)
		})
	}
}

func assertResumeRejectedObserver(
	t *testing.T,
	setup func(t *testing.T) (Runner[resumeRejectState, NoEffect], ResumeToken),
	wantReason string,
) {
	t.Helper()
	obs := &spyLifecycleObserver{}
	SetLifecycleObserver(obs)
	t.Cleanup(func() { SetLifecycleObserver(nil) })

	runner, token := setup(t)
	_, _ = runner.Resume(context.Background(), token)

	obs.mu.Lock()
	defer obs.mu.Unlock()
	if len(obs.rejected) != 1 || obs.rejected[0] != wantReason {
		t.Fatalf("expected resume_rejected %q, got %+v", wantReason, obs.rejected)
	}
}

func resumeRejectSetupZeroRevision(t *testing.T) (Runner[resumeRejectState, NoEffect], ResumeToken) {
	t.Helper()
	cp := newMemoryCP[resumeRejectState, NoEffect]()
	if _, err := cp.Save(context.Background(), 0, Snapshot[resumeRejectState, NoEffect]{
		ThreadID: "resume-zero-rev-th", ExecutionPointer: "work", State: resumeRejectState{},
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	g := resumeRejectedWorkGraph(t)
	return g.NewRunner(cp), ResumeToken{ThreadID: "resume-zero-rev-th"}
}

func resumeRejectSetupHandoffPending(t *testing.T) (Runner[resumeRejectState, NoEffect], ResumeToken) {
	t.Helper()
	cp := newMemoryCP[resumeRejectState, NoEffect]()
	if _, err := cp.Save(context.Background(), 0, Snapshot[resumeRejectState, NoEffect]{
		ThreadID: "resume-pending-th", ExecutionPointer: "work", State: resumeRejectState{},
		RunMeta: RunMetadata{
			HandoffStatus: HandoffStatusPending, HandoffPendingAt: time.Now().UTC(),
		},
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	g := resumeRejectedWorkGraph(t)
	_, rev, err := cp.Load(context.Background(), "resume-pending-th")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	return g.NewRunner(cp), ResumeToken{ThreadID: "resume-pending-th", SnapshotRevision: rev}
}

func resumeRejectSetupHandoffOrphaned(t *testing.T) (Runner[resumeRejectState, NoEffect], ResumeToken) {
	t.Helper()
	cp := newMemoryCP[resumeRejectState, NoEffect]()
	if _, err := cp.Save(context.Background(), 0, Snapshot[resumeRejectState, NoEffect]{
		ThreadID: "resume-orphan-th", ExecutionPointer: "work", State: resumeRejectState{},
		RunMeta: RunMetadata{HandoffStatus: HandoffStatusOrphaned},
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	g := resumeRejectedWorkGraph(t)
	_, rev, err := cp.Load(context.Background(), "resume-orphan-th")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	return g.NewRunner(cp), ResumeToken{ThreadID: "resume-orphan-th", SnapshotRevision: rev}
}

func resumeRejectSetupEmptyToken(t *testing.T) (Runner[resumeRejectState, NoEffect], ResumeToken) {
	t.Helper()
	g := resumeRejectedWorkGraph(t)
	return g.NewRunner(newMemoryCP[resumeRejectState, NoEffect]()), ResumeToken{}
}

func resumeRejectSetupInvalidHandoffStatus(t *testing.T) (Runner[resumeRejectState, NoEffect], ResumeToken) {
	t.Helper()
	cp := newMemoryCP[resumeRejectState, NoEffect]()
	if _, err := cp.Save(context.Background(), 0, Snapshot[resumeRejectState, NoEffect]{
		ThreadID: "resume-invalid-hs-th", ExecutionPointer: "work", State: resumeRejectState{},
		RunMeta: RunMetadata{HandoffStatus: HandoffStatus("corrupt")},
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	g := resumeRejectedWorkGraph(t)
	_, rev, err := cp.Load(context.Background(), "resume-invalid-hs-th")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	return g.NewRunner(cp), ResumeToken{ThreadID: "resume-invalid-hs-th", SnapshotRevision: rev}
}

func resumeRejectSetupInvalidSnapshot(t *testing.T) (Runner[resumeRejectState, NoEffect], ResumeToken) {
	t.Helper()
	cp := newMemoryCP[resumeRejectState, NoEffect]()
	if _, err := cp.Save(context.Background(), 0, Snapshot[resumeRejectState, NoEffect]{
		ThreadID: "resume-invalid-snap-th", ExecutionPointer: "", State: resumeRejectState{},
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	g := resumeRejectedWorkGraph(t)
	_, rev, err := cp.Load(context.Background(), "resume-invalid-snap-th")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	return g.NewRunner(cp), ResumeToken{ThreadID: "resume-invalid-snap-th", SnapshotRevision: rev}
}

func resumeRejectSetupInvalidPointer(t *testing.T) (Runner[resumeRejectState, NoEffect], ResumeToken) {
	t.Helper()
	cp := newMemoryCP[resumeRejectState, NoEffect]()
	if _, err := cp.Save(context.Background(), 0, Snapshot[resumeRejectState, NoEffect]{
		ThreadID: "resume-bad-ptr-th", ExecutionPointer: "missing", State: resumeRejectState{},
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	g := resumeRejectedWorkGraph(t)
	_, rev, err := cp.Load(context.Background(), "resume-bad-ptr-th")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	return g.NewRunner(cp), ResumeToken{ThreadID: "resume-bad-ptr-th", SnapshotRevision: rev}
}

func resumeRejectedWorkGraph(t *testing.T) *Graph[resumeRejectState, NoEffect] {
	t.Helper()
	b := NewGraph[resumeRejectState, NoEffect](
		func(_ resumeRejectState, u resumeRejectState) resumeRejectState { return u },
	)
	b.AddNode("work", func(_ context.Context, s resumeRejectState) (resumeRejectState, Directive, error) {
		return s, End(), nil
	})
	b.AllowNoOutgoingRoute("work")
	b.SetEntryPoint("work")
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	return g
}
