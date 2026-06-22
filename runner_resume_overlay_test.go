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

func TestResumeTargetPolicyRewritesPointer(t *testing.T) {
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
		WithResumeTargetPolicy[rewindOverlayState, NoEffect](
			func(_ context.Context, state rewindOverlayState, current ExecutionPointer) (
				rewindOverlayState,
				ResumePlan,
				error,
			) {
				if current == "wait_user" {
					state.Rewound = true
					return state, ResumeTo("router"), nil
				}
				return state, ResumeCurrent(), nil
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
		t.Fatal("wait_user must be skipped after resume target policy rewrite")
	}
	if !res.State.HitRoute {
		t.Fatal("router must run after resume target policy rewrite")
	}
	if !res.State.Rewound {
		t.Fatal("resume target policy state update must propagate")
	}
}

type invalidPtrState struct{}

func TestResumeTargetPolicyInvalidPointer(t *testing.T) {
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

	_, err = resumeLoaded(context.Background(), runner, cp, "invalid-ptr-th",
		WithResumeTargetPolicy[invalidPtrState, NoEffect](
			func(_ context.Context, state invalidPtrState, _ ExecutionPointer) (
				invalidPtrState,
				ResumePlan,
				error,
			) {
				return state, ResumeTo("ghost"), nil
			},
		),
	)
	if err == nil || !errors.Is(err, ErrResumeStartNodeNotFound) || !errors.Is(err, ErrInvalidSnapshot) ||
		!strings.Contains(err.Error(), "ghost") {
		t.Fatalf("expected ErrResumeStartNodeNotFound for ghost node, got %v", err)
	}
}

type emptyTargetPolicyPtrState struct{}

func TestResumeTargetPolicyEmptyPointer(t *testing.T) {
	t.Parallel()

	b := NewGraph[emptyTargetPolicyPtrState, NoEffect](
		func(_ emptyTargetPolicyPtrState, u emptyTargetPolicyPtrState) emptyTargetPolicyPtrState {
			return u
		},
	)
	b.AddNode(
		"wait",
		func(_ context.Context, s emptyTargetPolicyPtrState) (emptyTargetPolicyPtrState, Directive, error) {
			return s, Suspend("hold"), nil
		},
	)
	b.AllowNoOutgoingRoute("wait")
	b.SetEntryPoint("wait")
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	cp := newMemoryCP[emptyTargetPolicyPtrState, NoEffect]()
	runner := g.NewRunner(cp)
	_, err = runner.Start(context.Background(), "empty-target-policy-ptr-th", emptyTargetPolicyPtrState{})
	if err != nil {
		t.Fatalf("start: %v", err)
	}

	_, err = resumeLoaded(context.Background(), runner, cp, "empty-target-policy-ptr-th",
		WithResumeTargetPolicy[emptyTargetPolicyPtrState, NoEffect](
			func(_ context.Context, state emptyTargetPolicyPtrState, _ ExecutionPointer) (
				emptyTargetPolicyPtrState,
				ResumePlan,
				error,
			) {
				return state, ResumePlan{}, nil
			},
		),
	)
	if !errors.Is(err, ErrInvalidResumePlan) {
		t.Fatalf("expected ErrInvalidResumePlan, got %v", err)
	}
}
