package flowy

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestResumeEmptyToken(t *testing.T) {
	t.Parallel()

	type state struct{}
	g, err := NewGraph[state, NoEffect](func(_ state, u state) state { return u }).
		AddNode("n", func(_ context.Context, s state) (state, Directive, error) {
			return s, End(), nil
		}).
		SetEntryPoint("n").
		AllowNoOutgoingRoute("n").
		Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	_, err = g.NewRunner(newMemoryCP[state, NoEffect]()).Resume(context.Background(), ResumeToken{})
	if !errors.Is(err, ErrInvalidResumeToken) {
		t.Fatalf("expected ErrInvalidResumeToken, got %v", err)
	}
}
func TestResumeStaleToken(t *testing.T) {
	t.Parallel()

	type state struct{ N int }

	b := NewGraph[state, NoEffect](func(_ state, u state) state { return u })
	b.AddNode("step", func(_ context.Context, s state) (state, Directive, error) {
		s.N++
		if s.N < 3 {
			return s, Suspend("more"), nil
		}
		return s, End(), nil
	})
	b.AllowNoOutgoingRoute("step")
	b.SetEntryPoint("step")
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	cp := newMemoryCP[state, NoEffect]()
	runner := g.NewRunner(cp)
	first, err := runner.Start(context.Background(), "stale-th", state{})
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	staleToken := first.ResumeToken

	second, err := runner.Resume(context.Background(), staleToken)
	if err != nil {
		t.Fatalf("first resume: %v", err)
	}
	if second.Status != RunStatusSuspended {
		t.Fatalf("expected second suspend, got %s", second.Status)
	}

	_, err = runner.Resume(context.Background(), staleToken)
	if !errors.Is(err, ErrConcurrencyConflict) {
		t.Fatalf("expected ErrConcurrencyConflict on stale token, got %v", err)
	}
}
func TestResumeViaRunResultToken(t *testing.T) {
	t.Parallel()

	type state struct {
		Approved  bool
		Confirmed bool
	}

	b := NewGraph[state, NoEffect](func(_ state, u state) state { return u })
	b.AddNode("wait", func(_ context.Context, s state) (state, Directive, error) {
		if !s.Approved {
			return s, Suspend("input"), nil
		}
		if !s.Confirmed {
			return s, Suspend("confirm"), nil
		}
		return s, End(), nil
	})
	b.AllowNoOutgoingRoute("wait")
	b.SetEntryPoint("wait")
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	cp := newMemoryCP[state, NoEffect]()
	runner := g.NewRunner(cp)
	suspended, err := runner.Start(context.Background(), "hitl-th", state{})
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	if suspended.ResumeToken.ThreadID == "" {
		t.Fatal("expected ResumeToken on suspend result")
	}

	resumed, err := runner.Resume(context.Background(), suspended.ResumeToken,
		WithStateOverlay[state, NoEffect](state{Approved: true}, func(base, overlay state) state {
			base.Approved = overlay.Approved
			return base
		}),
	)
	if err != nil {
		t.Fatalf("resume: %v", err)
	}
	if resumed.Status != RunStatusSuspended || !resumed.State.Approved {
		t.Fatalf("expected second suspend after approval, got %+v", resumed)
	}

	_, err = runner.Resume(context.Background(), suspended.ResumeToken)
	if !errors.Is(err, ErrConcurrencyConflict) {
		t.Fatalf("expected ErrConcurrencyConflict after revision bump, got %v", err)
	}
}
func TestResumeZeroSnapshotRevisionRejected(t *testing.T) {
	t.Parallel()

	type state struct{}
	cp := newMemoryCP[state, NoEffect]()
	if _, err := cp.Save(context.Background(), 0, Snapshot[state, NoEffect]{
		ThreadID:         "zero-rev-th",
		ExecutionPointer: "wait",
		State:            state{},
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	b := NewGraph[state, NoEffect](func(_ state, u state) state { return u })
	b.AddNode("wait", func(_ context.Context, s state) (state, Directive, error) {
		return s, End(), nil
	})
	b.AllowNoOutgoingRoute("wait")
	b.SetEntryPoint("wait")
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	_, err = g.NewRunner(cp).Resume(context.Background(), ResumeToken{
		ThreadID:         "zero-rev-th",
		SnapshotRevision: 0,
	})
	if !errors.Is(err, ErrConcurrencyConflict) {
		t.Fatalf("expected ErrConcurrencyConflict for zero revision token, got %v", err)
	}
}

func TestSuspendResumeTokenSnapshotRevisionMatchesRevision(t *testing.T) {
	t.Parallel()

	type state struct{}

	b := NewGraph[state, NoEffect](func(_ state, u state) state { return u })
	b.AddNode("wait", func(_ context.Context, s state) (state, Directive, error) {
		return s, Suspend("hold"), nil
	})
	b.AllowNoOutgoingRoute("wait")
	b.SetEntryPoint("wait")
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	cp := newMemoryCP[state, NoEffect]()
	res, err := g.NewRunner(cp).Start(context.Background(), "suspend-occ-th", state{})
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	if res.ResumeToken.SnapshotRevision <= 0 {
		t.Fatalf("expected positive revision, got %+v", res.ResumeToken)
	}
	snap, _, err := cp.Load(context.Background(), "suspend-occ-th")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if res.ResumeToken.SnapshotRevision != snap.Revision {
		t.Fatalf("snapshot revision %d != snapshot revision %d", res.ResumeToken.SnapshotRevision, snap.Revision)
	}
}
func TestResumeStreamEmptyToken(t *testing.T) {
	t.Parallel()

	type state struct{}
	g, err := NewGraph[state, NoEffect](func(_ state, u state) state { return u }).
		AddNode("n", func(_ context.Context, s state) (state, Directive, error) {
			return s, End(), nil
		}).
		SetEntryPoint("n").
		AllowNoOutgoingRoute("n").
		Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	_, err = g.NewRunner(newMemoryCP[state, NoEffect]()).ResumeStream(context.Background(), ResumeToken{})
	if !errors.Is(err, ErrInvalidResumeToken) {
		t.Fatalf("expected ErrInvalidResumeToken, got %v", err)
	}
}
func TestResumeStreamStaleToken(t *testing.T) {
	t.Parallel()

	type state struct{ N int }

	b := NewGraph[state, NoEffect](func(_ state, u state) state { return u })
	b.AddNode("step", func(_ context.Context, s state) (state, Directive, error) {
		s.N++
		if s.N < 3 {
			return s, Suspend("more"), nil
		}
		return s, End(), nil
	})
	b.AllowNoOutgoingRoute("step")
	b.SetEntryPoint("step")
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	cp := newMemoryCP[state, NoEffect]()
	runner := g.NewRunner(cp)
	first, err := runner.Start(context.Background(), "stream-stale-th", state{})
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	staleToken := first.ResumeToken

	second, err := runner.Resume(context.Background(), staleToken)
	if err != nil {
		t.Fatalf("resume: %v", err)
	}
	if second.Status != RunStatusSuspended {
		t.Fatalf("expected suspended, got %s", second.Status)
	}

	_, err = runner.ResumeStream(context.Background(), staleToken)
	if !errors.Is(err, ErrConcurrencyConflict) {
		t.Fatalf("expected ErrConcurrencyConflict on ResumeStream stale token, got %v", err)
	}
}

func TestResumeRejectsHandoffPending(t *testing.T) {
	t.Parallel()

	type state struct{}
	cp := newMemoryCP[state, NoEffect]()
	if _, err := cp.Save(context.Background(), 0, Snapshot[state, NoEffect]{
		ThreadID:         "resume-pending-th",
		ExecutionPointer: "work",
		State:            state{},
		RunMeta: RunMetadata{
			HandoffStatus:    HandoffStatusPending,
			HandoffPendingAt: time.Now().UTC(),
		},
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

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

	_, rev, err := cp.Load(context.Background(), "resume-pending-th")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	_, err = g.NewRunner(cp).Resume(context.Background(), ResumeToken{
		ThreadID:         "resume-pending-th",
		SnapshotRevision: rev,
	})
	if !errors.Is(err, ErrHandoffPending) {
		t.Fatalf("expected ErrHandoffPending, got %v", err)
	}
}

func TestResumeRejectsHandoffOrphaned(t *testing.T) {
	t.Parallel()

	type state struct{}
	cp := newMemoryCP[state, NoEffect]()
	if _, err := cp.Save(context.Background(), 0, Snapshot[state, NoEffect]{
		ThreadID:         "resume-orphaned-th",
		ExecutionPointer: "work",
		State:            state{},
		RunMeta: RunMetadata{
			HandoffStatus: HandoffStatusOrphaned,
		},
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

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

	_, rev, err := cp.Load(context.Background(), "resume-orphaned-th")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	_, err = g.NewRunner(cp).Resume(context.Background(), ResumeToken{
		ThreadID:         "resume-orphaned-th",
		SnapshotRevision: rev,
	})
	if !errors.Is(err, ErrHandoffOrphaned) {
		t.Fatalf("expected ErrHandoffOrphaned, got %v", err)
	}
}

func TestResumeStreamRejectsHandoffPending(t *testing.T) {
	t.Parallel()

	type state struct{}
	cp := newMemoryCP[state, NoEffect]()
	if _, err := cp.Save(context.Background(), 0, Snapshot[state, NoEffect]{
		ThreadID:         "stream-pending-th",
		ExecutionPointer: "work",
		State:            state{},
		RunMeta: RunMetadata{
			HandoffStatus:    HandoffStatusPending,
			HandoffPendingAt: time.Now().UTC(),
		},
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

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

	_, rev, err := cp.Load(context.Background(), "stream-pending-th")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	_, err = g.NewRunner(cp).ResumeStream(context.Background(), ResumeToken{
		ThreadID:         "stream-pending-th",
		SnapshotRevision: rev,
	})
	if !errors.Is(err, ErrHandoffPending) {
		t.Fatalf("expected ErrHandoffPending on ResumeStream, got %v", err)
	}
}

func TestResumeStreamRejectsHandoffOrphaned(t *testing.T) {
	t.Parallel()

	type state struct{}
	cp := newMemoryCP[state, NoEffect]()
	if _, err := cp.Save(context.Background(), 0, Snapshot[state, NoEffect]{
		ThreadID:         "stream-orphaned-th",
		ExecutionPointer: "work",
		State:            state{},
		RunMeta: RunMetadata{
			HandoffStatus: HandoffStatusOrphaned,
		},
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

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

	_, rev, err := cp.Load(context.Background(), "stream-orphaned-th")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	_, err = g.NewRunner(cp).ResumeStream(context.Background(), ResumeToken{
		ThreadID:         "stream-orphaned-th",
		SnapshotRevision: rev,
	})
	if !errors.Is(err, ErrHandoffOrphaned) {
		t.Fatalf("expected ErrHandoffOrphaned on ResumeStream, got %v", err)
	}
}

func TestResumeRejectsInvalidHandoffStatus(t *testing.T) {
	t.Parallel()

	type state struct{}
	cp := newMemoryCP[state, NoEffect]()
	if _, err := cp.Save(context.Background(), 0, Snapshot[state, NoEffect]{
		ThreadID:         "resume-corrupt-th",
		ExecutionPointer: "work",
		State:            state{},
		RunMeta: RunMetadata{
			HandoffStatus: HandoffStatus("corrupt"),
		},
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

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

	_, rev, err := cp.Load(context.Background(), "resume-corrupt-th")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	_, err = g.NewRunner(cp).Resume(context.Background(), ResumeToken{
		ThreadID:         "resume-corrupt-th",
		SnapshotRevision: rev,
	})
	if !errors.Is(err, ErrInvalidSnapshot) {
		t.Fatalf("expected ErrInvalidSnapshot for corrupt handoff status, got %v", err)
	}
}

func TestResumeStreamRejectsInvalidHandoffStatus(t *testing.T) {
	t.Parallel()

	type state struct{}
	cp := newMemoryCP[state, NoEffect]()
	if _, err := cp.Save(context.Background(), 0, Snapshot[state, NoEffect]{
		ThreadID:         "stream-corrupt-th",
		ExecutionPointer: "work",
		State:            state{},
		RunMeta: RunMetadata{
			HandoffStatus: HandoffStatus("corrupt"),
		},
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

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

	_, rev, err := cp.Load(context.Background(), "stream-corrupt-th")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	_, err = g.NewRunner(cp).ResumeStream(context.Background(), ResumeToken{
		ThreadID:         "stream-corrupt-th",
		SnapshotRevision: rev,
	})
	if !errors.Is(err, ErrInvalidSnapshot) {
		t.Fatalf("expected ErrInvalidSnapshot on ResumeStream, got %v", err)
	}
}

func TestResumeTokenFromSnapshotEquivalentToRunResultToken(t *testing.T) {
	t.Parallel()

	type state struct{}

	b := NewGraph[state, NoEffect](func(_ state, u state) state { return u })
	b.AddNode("wait", func(_ context.Context, s state) (state, Directive, error) {
		return s, Suspend("hold"), nil
	})
	b.AllowNoOutgoingRoute("wait")
	b.SetEntryPoint("wait")
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	cp := newMemoryCP[state, NoEffect]()
	runner := g.NewRunner(cp)
	res, err := runner.Start(context.Background(), "token-equiv-th", state{})
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	snap, _, loadErr := cp.Load(context.Background(), "token-equiv-th")
	if loadErr != nil {
		t.Fatalf("load: %v", loadErr)
	}
	fromSnap := ResumeTokenFromSnapshot(snap)
	if fromSnap != res.ResumeToken {
		t.Fatalf("ResumeTokenFromSnapshot %+v != result token %+v", fromSnap, res.ResumeToken)
	}
	_, err = runner.Resume(context.Background(), fromSnap)
	if err != nil {
		t.Fatalf("resume via snapshot token: %v", err)
	}
}
