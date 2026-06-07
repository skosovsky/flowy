package flowy

import (
	"context"
	"errors"
	"testing"
)

func TestSuspendPointerResolverRewritesSave(t *testing.T) {
	t.Parallel()

	type state struct{ N int }

	b := NewGraph[state, NoEffect](func(_ state, u state) state { return u })
	b.AddNode("wait", func(_ context.Context, s state) (state, Directive, error) {
		return s, Suspend("hold"), nil
	})
	b.AddNode("router", func(_ context.Context, s state) (state, Directive, error) {
		return s, Suspend("hold"), nil
	})
	b.AllowNoOutgoingRoute("wait")
	b.AllowNoOutgoingRoute("router")
	b.SetEntryPoint("wait")
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	cp := newMemoryCP[state, NoEffect]()
	runner := g.NewRunner(cp)
	_, err = runner.Start(context.Background(), "resolver-th", state{},
		WithSuspendPointerResolver[state, NoEffect](func(_ state, _ ExecutionPointer) (ExecutionPointer, error) {
			return "router", nil
		}),
	)
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	if cp.last.ExecutionPointer != "router" {
		t.Fatalf("expected saved pointer router, got %q", cp.last.ExecutionPointer)
	}
}

func TestSuspendPointerResolverInvalidPointer(t *testing.T) {
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
	res, err := g.NewRunner(cp).Start(context.Background(), "invalid-ptr-th", state{},
		WithSuspendPointerResolver[state, NoEffect](func(_ state, _ ExecutionPointer) (ExecutionPointer, error) {
			return "ghost", nil
		}),
	)
	if err == nil {
		t.Fatal("expected error for invalid suspend pointer")
	}
	if res == nil || res.Status != RunStatusFailed {
		t.Fatalf("expected RunStatusFailed, got res=%+v", res)
	}
	if _, loadErr := cp.Load(context.Background(), "invalid-ptr-th"); !errors.Is(loadErr, ErrThreadNotFound) {
		t.Fatalf("snapshot must not be saved on invalid pointer, load err=%v", loadErr)
	}
}

func TestSuspendPointerResolverOnHandoff(t *testing.T) {
	t.Parallel()

	type state struct{}

	b := NewGraph[state, NoEffect](func(_ state, u state) state { return u })
	b.AddNode("work", func(_ context.Context, s state) (state, Directive, error) {
		return s, Handoff("bg"), nil
	})
	b.AddNode("router", func(_ context.Context, s state) (state, Directive, error) {
		return s, Completed(), nil
	})
	b.AllowNoOutgoingRoute("work")
	b.AllowNoOutgoingRoute("router")
	b.SetEntryPoint("work")
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	cp := newMemoryCP[state, NoEffect]()
	_, err = g.NewRunner(cp).Start(context.Background(), "handoff-resolver-th", state{},
		WithSuspendPointerResolver[state, NoEffect](func(_ state, _ ExecutionPointer) (ExecutionPointer, error) {
			return "router", nil
		}),
	)
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	if cp.last.ExecutionPointer != "router" {
		t.Fatalf("expected saved pointer router, got %q", cp.last.ExecutionPointer)
	}
}

func TestStreamHardFailSaveUsesResolvedPointer(t *testing.T) {
	t.Parallel()

	type state struct{}

	cp := &failingMemoryCP[state, NoEffect]{failSave: true}
	b := NewGraph[state, NoEffect](func(_ state, u state) state { return u })
	b.AddNode("wait", func(_ context.Context, s state) (state, Directive, error) {
		return s, Suspend("hold"), nil
	})
	b.AddNode("router", func(_ context.Context, s state) (state, Directive, error) {
		return s, Suspend("hold"), nil
	})
	b.AllowNoOutgoingRoute("wait")
	b.AllowNoOutgoingRoute("router")
	b.SetEntryPoint("wait")
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	resolver := WithSuspendPointerResolver[state, NoEffect](
		func(_ state, _ ExecutionPointer) (ExecutionPointer, error) {
			return "router", nil
		},
	)
	handle, err := g.NewRunner(cp).Stream(context.Background(), "stream-hardfail-ptr-th", state{}, resolver)
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
	events, waitErr := CollectEventsAndWait(context.Background(), handle)
	if waitErr == nil {
		t.Fatal("expected save failure")
	}

	syncRes, syncErr := g.NewRunner(cp).Start(context.Background(), "stream-hardfail-ptr-sync-th", state{}, resolver)
	if syncErr == nil {
		t.Fatalf("expected sync save failure, got %+v", syncRes)
	}
	if string(syncRes.ExecutionPointer) != "router" {
		t.Fatalf("sync pointer: want router, got %q", syncRes.ExecutionPointer)
	}

	var failedPtr string
	for _, ev := range events {
		if ev.Type == EventFailed {
			failedPtr = string(ev.ExecutionPointer)
		}
	}
	if failedPtr != "router" {
		t.Fatalf("stream EventFailed pointer: want router, got %q events=%+v", failedPtr, events)
	}
	if syncRes.Reason != ReasonSuspendSaveFailed {
		t.Fatalf("sync reason: want %q, got %q", ReasonSuspendSaveFailed, syncRes.Reason)
	}
	assertEventFailedReasonMatchesSync(t, events, ReasonSuspendSaveFailed)
}

func TestSuspendPointerResolverReturnsError(t *testing.T) {
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
	res, err := g.NewRunner(cp).Start(context.Background(), "resolver-err-th", state{},
		WithSuspendPointerResolver[state, NoEffect](func(_ state, _ ExecutionPointer) (ExecutionPointer, error) {
			return "", errors.New("resolver rejected")
		}),
	)
	if err == nil {
		t.Fatal("expected resolver error")
	}
	if res == nil || res.Status != RunStatusFailed {
		t.Fatalf("expected RunStatusFailed, got res=%+v", res)
	}
	if res.Reason != ReasonSuspendPointerResolveFailed {
		t.Fatalf("expected reason %q, got %q", ReasonSuspendPointerResolveFailed, res.Reason)
	}
	if _, loadErr := cp.Load(context.Background(), "resolver-err-th"); !errors.Is(loadErr, ErrThreadNotFound) {
		t.Fatalf("snapshot must not be saved, load err=%v", loadErr)
	}
}

func TestSuspendSaveFailUsesResolvedPointer(t *testing.T) {
	t.Parallel()

	type state struct{}

	cp := &failingMemoryCP[state, NoEffect]{failSave: true}
	b := NewGraph[state, NoEffect](func(_ state, u state) state { return u })
	b.AddNode("wait", func(_ context.Context, s state) (state, Directive, error) {
		return s, Suspend("hold"), nil
	})
	b.AddNode("router", func(_ context.Context, s state) (state, Directive, error) {
		return s, Suspend("hold"), nil
	})
	b.AllowNoOutgoingRoute("wait")
	b.AllowNoOutgoingRoute("router")
	b.SetEntryPoint("wait")
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	res, err := g.NewRunner(cp).Start(context.Background(), "suspend-ptr-th", state{},
		WithSuspendPointerResolver[state, NoEffect](func(_ state, _ ExecutionPointer) (ExecutionPointer, error) {
			return "router", nil
		}),
	)
	if err == nil {
		t.Fatalf("expected save failure, got %+v", res)
	}
	if string(res.ExecutionPointer) != "router" {
		t.Fatalf("expected failed result pointer router, got %q", res.ExecutionPointer)
	}
}

func TestContextCancelSaveIgnoresSuspendPointerResolver(t *testing.T) {
	t.Parallel()

	type state struct{}

	b := NewGraph[state, NoEffect](func(_ state, u state) state { return u })
	b.AddNode("loop", func(_ context.Context, s state) (state, Directive, error) {
		return s, Completed(), nil
	})
	b.AddEdge("loop", "loop")
	b.AddNode("router", func(_ context.Context, s state) (state, Directive, error) {
		return s, End(), nil
	})
	b.AllowNoOutgoingRoute("router")
	b.SetEntryPoint("loop")
	g, err := b.Compile(WithMaxSteps(5))
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	cp := newMemoryCP[state, NoEffect]()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	res, err := g.NewRunner(cp).Start(ctx, "cancel-resolver-th", state{},
		WithSuspendPointerResolver[state, NoEffect](func(_ state, _ ExecutionPointer) (ExecutionPointer, error) {
			return "router", nil
		}),
	)
	if err == nil {
		t.Fatalf("expected cancel error, got res=%+v", res)
	}
	snap, loadErr := cp.Load(context.Background(), "cancel-resolver-th")
	if loadErr != nil {
		t.Fatalf("load: %v", loadErr)
	}
	if snap.ExecutionPointer != "loop" {
		t.Fatalf("cancel save must keep suspend-node pointer %q, got %q", "loop", snap.ExecutionPointer)
	}
}

func TestCheckpointerStoresResolvedPointerOnly(t *testing.T) {
	t.Parallel()

	type state struct{}

	b := NewGraph[state, NoEffect](func(_ state, u state) state { return u })
	b.AddNode("wait", func(_ context.Context, s state) (state, Directive, error) {
		return s, Suspend("hold"), nil
	})
	b.AddNode("router", func(_ context.Context, s state) (state, Directive, error) {
		return s, Suspend("hold"), nil
	})
	b.AllowNoOutgoingRoute("wait")
	b.AllowNoOutgoingRoute("router")
	b.SetEntryPoint("wait")
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	cp := &pointerSpyCP[state, NoEffect]{memoryCP: *newMemoryCP[state, NoEffect]()}
	_, err = g.NewRunner(cp).Start(context.Background(), "spy-pointer-th", state{},
		WithSuspendPointerResolver[state, NoEffect](
			func(_ state, suspendNode ExecutionPointer) (ExecutionPointer, error) {
				if suspendNode != "wait" {
					t.Fatalf("resolver saw suspend node %q, want wait", suspendNode)
				}
				return "router", nil
			},
		),
	)
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	if len(cp.savedPointers) != 1 || cp.savedPointers[0] != "router" {
		t.Fatalf("checkpointer must receive resolved pointer only, got %+v", cp.savedPointers)
	}
}
