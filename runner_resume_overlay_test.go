package flowy

import (
	"context"
	"errors"
	"strings"
	"testing"
)

type rewindOverlayState struct {
	HitWait  bool
	HitRoute bool
	Rewound  bool
}

func (s *rewindOverlayState) ReconcileResume(currentPtr ExecutionPointer) (ExecutionPointer, error) {
	if currentPtr == "wait_user" {
		s.Rewound = true
		return "router", nil
	}
	return currentPtr, nil
}

func TestResumeReconcilerPointerRewind(t *testing.T) {
	t.Parallel()

	b := NewGraph[rewindOverlayState, NoEffect](func(cur, upd rewindOverlayState) rewindOverlayState {
		cur.HitWait = upd.HitWait
		cur.HitRoute = upd.HitRoute
		cur.Rewound = upd.Rewound
		return cur
	})
	b.AddNode("wait_user", func(_ context.Context, s rewindOverlayState) (rewindOverlayState, Directive, error) {
		s.HitWait = true
		return s, Suspend("hold"), nil
	})
	b.AddNode("router", func(_ context.Context, s rewindOverlayState) (rewindOverlayState, Directive, error) {
		s.HitRoute = true
		return s, Completed(), nil
	})
	b.AddNode("done", func(_ context.Context, s rewindOverlayState) (rewindOverlayState, Directive, error) {
		return s, End(), nil
	})
	b.AddEdge("router", "done")
	b.AllowNoOutgoingRoute("wait_user")
	b.AllowNoOutgoingRoute("done")
	b.SetEntryPoint("wait_user")
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	cp := newMemoryCP[rewindOverlayState, NoEffect]()
	runner := g.NewRunner(cp)
	_, err = runner.Start(context.Background(), "rewind-th", rewindOverlayState{})
	if err != nil {
		t.Fatalf("start: %v", err)
	}

	res, err := resumeLoaded(context.Background(), runner, cp, "rewind-th",
		WithStateOverlay[rewindOverlayState, NoEffect](
			rewindOverlayState{},
			func(base, _ rewindOverlayState) rewindOverlayState {
				// Clear marker from initial Start so HitWait=true only if wait_user runs again on resume.
				base.HitWait = false
				return base
			},
		),
	)
	if err != nil {
		t.Fatalf("resume: %v", err)
	}
	if res.Status != RunStatusCompleted {
		t.Fatalf("expected completed, got %s", res.Status)
	}
	if res.State.HitWait {
		t.Fatal("wait_user must be skipped after pointer rewind")
	}
	if !res.State.HitRoute {
		t.Fatal("router must run after pointer rewind")
	}
	if !res.State.Rewound {
		t.Fatal("ReconcileResume side effect must propagate")
	}
}

type invalidPtrState struct{}

func (s *invalidPtrState) ReconcileResume(_ ExecutionPointer) (ExecutionPointer, error) {
	return "ghost", nil
}

func TestResumeReconcilerInvalidPointer(t *testing.T) {
	t.Parallel()

	b := NewGraph[invalidPtrState, NoEffect](func(_ invalidPtrState, u invalidPtrState) invalidPtrState {
		return u
	})
	b.AddNode("wait", func(_ context.Context, s invalidPtrState) (invalidPtrState, Directive, error) {
		return s, Suspend("hold"), nil
	})
	b.AllowNoOutgoingRoute("wait")
	b.SetEntryPoint("wait")
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	cp := newMemoryCP[invalidPtrState, NoEffect]()
	runner := g.NewRunner(cp)
	_, err = runner.Start(context.Background(), "invalid-ptr-th", invalidPtrState{})
	if err != nil {
		t.Fatalf("start: %v", err)
	}

	_, err = resumeLoaded(context.Background(), runner, cp, "invalid-ptr-th")
	if err == nil || !errors.Is(err, ErrResumeStartNodeNotFound) ||
		!strings.Contains(err.Error(), "ghost") {
		t.Fatalf("expected ErrResumeStartNodeNotFound for ghost node, got %v", err)
	}
}

type emptyReconcilePtrState struct{}

func (s *emptyReconcilePtrState) ReconcileResume(_ ExecutionPointer) (ExecutionPointer, error) {
	return "", nil
}

func TestResumeReconcilerEmptyPointer(t *testing.T) {
	t.Parallel()

	b := NewGraph[emptyReconcilePtrState, NoEffect](
		func(_ emptyReconcilePtrState, u emptyReconcilePtrState) emptyReconcilePtrState {
			return u
		},
	)
	b.AddNode("wait", func(_ context.Context, s emptyReconcilePtrState) (emptyReconcilePtrState, Directive, error) {
		return s, Suspend("hold"), nil
	})
	b.AllowNoOutgoingRoute("wait")
	b.SetEntryPoint("wait")
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	cp := newMemoryCP[emptyReconcilePtrState, NoEffect]()
	runner := g.NewRunner(cp)
	_, err = runner.Start(context.Background(), "empty-rec-ptr-th", emptyReconcilePtrState{})
	if err != nil {
		t.Fatalf("start: %v", err)
	}

	_, err = resumeLoaded(context.Background(), runner, cp, "empty-rec-ptr-th")
	if !errors.Is(err, ErrInvalidSnapshot) {
		t.Fatalf("expected ErrInvalidSnapshot, got %v", err)
	}
}
