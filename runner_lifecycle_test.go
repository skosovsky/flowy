package flowy

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestLegacyNextDirectiveFails(t *testing.T) {
	t.Parallel()

	type state struct{ N int }

	b := NewGraph[state, NoEffect](func(_ state, u state) state { return u })
	b.AddNode("a", func(_ context.Context, s state) (state, Directive, error) {
		return s, directiveWithKind(directiveNext), nil
	})
	b.AddNode("b", func(_ context.Context, s state) (state, Directive, error) {
		return s, End(), nil
	})
	b.AllowNoOutgoingRoute("b")
	b.AddEdge("a", "b")
	b.SetEntryPoint("a")
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	res, err := g.NewRunner(newCaptureCheckpointer[state, NoEffect]()).Start(context.Background(), "t", state{})
	if err == nil || !errors.Is(err, ErrLegacyNext) {
		t.Fatalf("expected ErrLegacyNext, got res=%+v err=%v", res, err)
	}
}

func TestResumeOverlayThenReconcileOrder(t *testing.T) {
	t.Parallel()

	type state struct {
		Base    string
		Overlay string
	}

	var reconcileSeen state
	b := NewGraph[state, NoEffect](func(cur, upd state) state {
		cur.Base = upd.Base
		if upd.Overlay != "" {
			cur.Overlay = upd.Overlay
		}
		return cur
	})
	b.AddNode("wait", func(_ context.Context, s state) (state, Directive, error) {
		return s, Suspend("input"), nil
	})
	b.AllowNoOutgoingRoute("wait")
	b.SetEntryPoint("wait")
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	cp := newMemoryCP[state, NoEffect]()
	runner := g.NewRunner(cp)
	_, err = runner.Start(context.Background(), "th", state{Base: "from-checkpoint"})
	if err != nil {
		t.Fatalf("start: %v", err)
	}

	_, err = runner.Resume(context.Background(), "th",
		WithStateOverlay[state, NoEffect](state{Overlay: "user-input"}, func(base, overlay state) state {
			base.Overlay = overlay.Overlay
			return base
		}),
		WithResumeReconciler(func(snap Snapshot[state, NoEffect]) (string, error) {
			reconcileSeen = snap.State
			return snap.NodeID, nil
		}),
	)
	if err != nil {
		t.Fatalf("resume: %v", err)
	}
	if reconcileSeen.Base != "from-checkpoint" || reconcileSeen.Overlay != "user-input" {
		t.Fatalf("reconcile saw wrong state: %+v", reconcileSeen)
	}
}

func TestBindingsNotPersisted(t *testing.T) {
	t.Parallel()

	type state struct{ V int }

	b := NewGraph[state, NoEffect](func(_ state, u state) state { return u })
	b.AddNode("n", func(ctx context.Context, s state) (state, Directive, error) {
		v, ok := BindingFromContext[int](ctx, "counter")
		if !ok {
			return s, Fail("no binding"), nil
		}
		s.V = v
		return s, Suspend("pause"), nil
	})
	b.AllowNoOutgoingRoute("n")
	b.SetEntryPoint("n")
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	cp := newMemoryCP[state, NoEffect]()
	bindings := NewRunBindings()
	bindings.Set("counter", 42)
	runner := g.NewRunner(cp)
	_, err = runner.Start(context.Background(), "th", state{}, WithBindings[state, NoEffect](bindings))
	if err != nil {
		t.Fatalf("start: %v", err)
	}

	snap, err := cp.Load(context.Background(), "th")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if snap.State.V != 42 {
		t.Fatalf("expected state from binding, got %+v", snap.State)
	}

	// Bindings map must not appear in serialized snapshot (only state fields).
	if _, hasBinding := bindings.Get("counter"); !hasBinding {
		t.Fatal("binding still in runtime container")
	}
}

func TestNamedBudgetExceeded(t *testing.T) {
	t.Parallel()

	type state struct{}

	b := NewGraph[state, NoEffect](func(_ state, u state) state { return u })
	b.AddNode("loop", func(ctx context.Context, s state) (state, Directive, error) {
		_ = UseBudget(ctx, "api", 1)
		return s, Completed(), nil
	})
	b.AddEdge("loop", "loop")
	b.SetEntryPoint("loop")
	g, err := b.Compile(WithNamedBudget("api", 2))
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	_, err = g.NewRunner(newCaptureCheckpointer[state, NoEffect]()).Start(context.Background(), "t", state{})
	if !errors.Is(err, ErrBudgetExceeded) {
		t.Fatalf("expected ErrBudgetExceeded, got %v", err)
	}
}

func TestInvariantValidatorBlocksSave(t *testing.T) {
	t.Parallel()

	type state struct{ OK bool }

	b := NewGraph[state, NoEffect](func(_ state, u state) state { return u })
	b.AddNode("bad", func(_ context.Context, s state) (state, Directive, error) {
		s.OK = false
		return s, Suspend("pause"), nil
	})
	b.AllowNoOutgoingRoute("bad")
	b.SetEntryPoint("bad")
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	cp := newMemoryCP[state, NoEffect]()
	_, err = g.NewRunner(cp).Start(context.Background(), "th", state{OK: true},
		WithInvariantValidator[state, NoEffect](func(s state) error {
			if !s.OK {
				return errors.New("invariant violated")
			}
			return nil
		}),
	)
	if err == nil || err.Error() != "invariant violated" {
		t.Fatalf("expected invariant error, got %v", err)
	}
	if _, loadErr := cp.Load(context.Background(), "th"); !errors.Is(loadErr, ErrThreadNotFound) {
		t.Fatalf("snapshot must not be saved on invariant violation, load err=%v", loadErr)
	}
}

func TestLeaseContention(t *testing.T) {
	t.Parallel()

	lease := NewMemoryLeaseManager()
	ctx := context.Background()
	if err := lease.Acquire(ctx, "th", "worker-a", time.Minute); err != nil {
		t.Fatalf("acquire a: %v", err)
	}
	err := lease.Acquire(ctx, "th", "worker-b", time.Minute)
	if !errors.Is(err, ErrLeaseHeld) {
		t.Fatalf("expected ErrLeaseHeld, got %v", err)
	}
}

func TestHandoffToBackgroundDuringActiveRun(t *testing.T) {
	t.Parallel()

	type state struct{ N int }

	b := NewGraph[state, NoEffect](func(_ state, u state) state { return u })
	b.AddNode("work", func(ctx context.Context, s state) (state, Directive, error) {
		s.N++
		<-ctx.Done()
		return s, Completed(), nil
	})
	b.AddEdge("work", EndNode)
	b.SetEntryPoint("work")
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	cp := newMemoryCP[state, NoEffect]()
	runner := g.NewRunner(cp)

	done := make(chan *RunResult[state, NoEffect], 1)
	go func() {
		res, runErr := runner.Start(context.Background(), "th", state{})
		if runErr != nil {
			t.Errorf("start: %v", runErr)
		}
		done <- res
	}()

	time.Sleep(10 * time.Millisecond)
	if handoffErr := runner.HandoffToBackground(context.Background(), "th"); handoffErr != nil {
		t.Fatalf("handoff: %v", handoffErr)
	}

	res := <-done
	if res == nil {
		t.Fatal("expected result")
	}
	if res.Status != RunStatusHandoff {
		t.Fatalf("expected handoff status, got %s", res.Status)
	}
	snap, err := cp.Load(context.Background(), "th")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if snap.RunMeta.Segment.EndReason != SegmentEndHandoff {
		t.Fatalf("expected handoff segment reason, got %q", snap.RunMeta.Segment.EndReason)
	}
	if snap.State.N != 1 {
		t.Fatalf("expected checkpointed progress, got %+v", snap.State)
	}
}

func TestHandoffToBackgroundThenImmediateResume(t *testing.T) {
	t.Parallel()

	type state struct{ N int }

	b := NewGraph[state, NoEffect](func(_ state, u state) state { return u })
	b.AddNode("work", func(ctx context.Context, s state) (state, Directive, error) {
		s.N++
		if s.N == 1 {
			<-ctx.Done()
			return s, Completed(), nil
		}
		return s, End(), nil
	})
	b.AllowNoOutgoingRoute("work")
	b.SetEntryPoint("work")
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	cp := newMemoryCP[state, NoEffect]()
	runner := g.NewRunner(cp)

	done := make(chan *RunResult[state, NoEffect], 1)
	go func() {
		res, runErr := runner.Start(context.Background(), "handoff-resume-th", state{})
		if runErr != nil {
			t.Errorf("start: %v", runErr)
		}
		done <- res
	}()

	time.Sleep(10 * time.Millisecond)
	if handoffErr := runner.HandoffToBackground(context.Background(), "handoff-resume-th"); handoffErr != nil {
		t.Fatalf("handoff: %v", handoffErr)
	}

	res, err := runner.Resume(context.Background(), "handoff-resume-th")
	if err != nil {
		t.Fatalf("resume after handoff: %v", err)
	}
	if res.Status != RunStatusCompleted {
		t.Fatalf("expected completed after resume, got %s", res.Status)
	}
	if res.State.N != 2 {
		t.Fatalf("expected resumed progress N=2, got %d", res.State.N)
	}
	startRes := <-done
	if startRes != nil && startRes.Status != RunStatusHandoff {
		t.Fatalf("expected foreground handoff, got %s", startRes.Status)
	}
}

func TestRetentionLimitOnHandoff(t *testing.T) {
	t.Parallel()

	type state struct{ N int }

	b := NewGraph[state, NoEffect](func(_ state, u state) state { return u })
	b.AddNode("n", func(_ context.Context, s state) (state, Directive, error) {
		s.N++
		return s, Handoff("bg"), nil
	})
	b.AllowNoOutgoingRoute("n")
	b.SetEntryPoint("n")
	g, err := b.Compile(WithRetentionLimit(2))
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	cp := newMemoryCP[state, NoEffect]()
	runner := g.NewRunner(cp)
	for i := range 3 {
		if i == 0 {
			_, err = runner.Start(context.Background(), "handoff-retention-th", state{})
		} else {
			_, err = runner.Resume(context.Background(), "handoff-retention-th")
		}
		if err != nil {
			t.Fatalf("run %d: %v", i, err)
		}
	}
	history, err := cp.GetHistory(context.Background(), "handoff-retention-th", 10)
	if err != nil {
		t.Fatalf("history: %v", err)
	}
	if len(history) > 2 {
		t.Fatalf("expected retention limit 2, got %d snapshots", len(history))
	}
}

func TestHandoffDirectiveAndBackgroundShareCheckpointSemantics(t *testing.T) {
	t.Parallel()

	type state struct{ N int }

	build := func(blockUntilHandoff bool) (*Graph[state, NoEffect], *memoryCP[state, NoEffect]) {
		b := NewGraph[state, NoEffect](func(_ state, u state) state { return u })
		b.AddNode("work", func(ctx context.Context, s state) (state, Directive, error) {
			s.N = 7
			if blockUntilHandoff {
				<-ctx.Done()
				return s, Completed(), nil
			}
			return s, Handoff("job"), nil
		})
		b.AllowNoOutgoingRoute("work")
		b.SetEntryPoint("work")
		g, err := b.Compile()
		if err != nil {
			t.Fatalf("compile: %v", err)
		}
		return g, newMemoryCP[state, NoEffect]()
	}

	assertHandoffSnap := func(t *testing.T, snap Snapshot[state, NoEffect], label string) {
		t.Helper()
		if snap.RunMeta.Segment.EndReason != SegmentEndHandoff {
			t.Fatalf("%s: segment end=%q want %q", label, snap.RunMeta.Segment.EndReason, SegmentEndHandoff)
		}
		if snap.State.N != 7 {
			t.Fatalf("%s: state.N=%d want 7", label, snap.State.N)
		}
	}

	gDir, cpDir := build(false)
	res, err := gDir.NewRunner(cpDir).Start(context.Background(), "dir-th", state{})
	if err != nil {
		t.Fatalf("directive start: %v", err)
	}
	if res.Status != RunStatusHandoff || res.Reason != "job" {
		t.Fatalf("directive: status=%s reason=%q", res.Status, res.Reason)
	}
	snapDir, err := cpDir.Load(context.Background(), "dir-th")
	if err != nil {
		t.Fatalf("directive load: %v", err)
	}
	assertHandoffSnap(t, snapDir, "directive")

	gBg, cpBg := build(true)
	runner := gBg.NewRunner(cpBg)
	done := make(chan *RunResult[state, NoEffect], 1)
	go func() {
		res, runErr := runner.Start(context.Background(), "bg-th", state{})
		if runErr != nil {
			t.Errorf("background start: %v", runErr)
		}
		done <- res
	}()
	time.Sleep(10 * time.Millisecond)
	if handoffErr := runner.HandoffToBackground(context.Background(), "bg-th"); handoffErr != nil {
		t.Fatalf("background handoff: %v", handoffErr)
	}
	bgRes := <-done
	if bgRes == nil || bgRes.Status != RunStatusHandoff {
		t.Fatalf("background result: %+v", bgRes)
	}
	snapBg, err := cpBg.Load(context.Background(), "bg-th")
	if err != nil {
		t.Fatalf("background load: %v", err)
	}
	assertHandoffSnap(t, snapBg, "background")
	if snapDir.NodeID != snapBg.NodeID {
		t.Fatalf("node mismatch: dir=%q bg=%q", snapDir.NodeID, snapBg.NodeID)
	}
}

func TestHandoffDirectiveFromNode(t *testing.T) {
	t.Parallel()

	type state struct{}

	b := NewGraph[state, NoEffect](func(_ state, u state) state { return u })
	b.AddNode("n", func(_ context.Context, s state) (state, Directive, error) {
		return s, Handoff("long_job"), nil
	})
	b.AllowNoOutgoingRoute("n")
	b.SetEntryPoint("n")
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	cp := newMemoryCP[state, NoEffect]()
	res, err := g.NewRunner(cp).Start(context.Background(), "th", state{})
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	if res.Status != RunStatusHandoff {
		t.Fatalf("expected handoff, got %s", res.Status)
	}
}

func TestRetentionLimitOnContextCancel(t *testing.T) {
	t.Parallel()

	type state struct{ N int }

	b := NewGraph[state, NoEffect](func(_ state, u state) state { return u })
	b.AddNode("n", func(ctx context.Context, s state) (state, Directive, error) {
		s.N++
		if s.N < 3 {
			return s, Suspend("pause"), nil
		}
		<-ctx.Done()
		return s, Suspend("canceled"), nil
	})
	b.AllowNoOutgoingRoute("n")
	b.SetEntryPoint("n")
	g, err := b.Compile(WithRetentionLimit(2))
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	cp := newMemoryCP[state, NoEffect]()
	runner := g.NewRunner(cp)
	for i := range 2 {
		if i == 0 {
			_, err = runner.Start(context.Background(), "th", state{})
		} else {
			_, err = runner.Resume(context.Background(), "th")
		}
		if err != nil {
			t.Fatalf("run %d: %v", i, err)
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = runner.Resume(ctx, "th")
	}()
	time.Sleep(10 * time.Millisecond)
	cancel()
	<-done

	history, err := cp.GetHistory(context.Background(), "th", 10)
	if err != nil {
		t.Fatalf("history: %v", err)
	}
	if len(history) > 2 {
		t.Fatalf("expected retention limit 2 after cancel, got %d snapshots", len(history))
	}
}

func TestResumeResetsStepCount(t *testing.T) {
	t.Parallel()

	type state struct{ N int }

	b := NewGraph[state, NoEffect](func(_ state, u state) state { return u })
	b.AddNode("n", func(_ context.Context, s state) (state, Directive, error) {
		s.N++
		return s, Suspend("pause"), nil
	})
	b.AllowNoOutgoingRoute("n")
	b.SetEntryPoint("n")
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	cp := newMemoryCP[state, NoEffect]()
	runner := g.NewRunner(cp)
	_, err = runner.Start(context.Background(), "th", state{})
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	snap, err := cp.Load(context.Background(), "th")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if snap.RunMeta.StepCount != 1 {
		t.Fatalf("expected step count 1 after start, got %d", snap.RunMeta.StepCount)
	}

	_, err = runner.Resume(context.Background(), "th")
	if err != nil {
		t.Fatalf("resume: %v", err)
	}
	snap, err = cp.Load(context.Background(), "th")
	if err != nil {
		t.Fatalf("load after resume: %v", err)
	}
	if snap.RunMeta.StepCount != 1 {
		t.Fatalf("expected reset step count 1 after resume segment, got %d", snap.RunMeta.StepCount)
	}
}

func TestInvariantBlocksContextCancelSave(t *testing.T) {
	t.Parallel()

	type state struct{ OK bool }

	b := NewGraph[state, NoEffect](func(_ state, u state) state { return u })
	b.AddNode("slow", func(ctx context.Context, s state) (state, Directive, error) {
		s.OK = false
		<-ctx.Done()
		return s, Completed(), nil
	})
	b.AllowNoOutgoingRoute("slow")
	b.SetEntryPoint("slow")
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	cp := newMemoryCP[state, NoEffect]()
	runner := g.NewRunner(cp)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = runner.Start(ctx, "th", state{OK: true}, WithInvariantValidator[state, NoEffect](func(s state) error {
			if !s.OK {
				return errors.New("invariant violated")
			}
			return nil
		}))
	}()
	time.Sleep(10 * time.Millisecond)
	cancel()
	<-done

	if _, loadErr := cp.Load(context.Background(), "th"); !errors.Is(loadErr, ErrThreadNotFound) {
		t.Fatalf("invalid state must not be checkpointed on cancel, err=%v", loadErr)
	}
}

func TestRetentionLimitOnSuspend(t *testing.T) {
	t.Parallel()

	type state struct{ N int }

	b := NewGraph[state, NoEffect](func(_ state, u state) state { return u })
	b.AddNode("n", func(_ context.Context, s state) (state, Directive, error) {
		s.N++
		return s, Suspend("pause"), nil
	})
	b.AllowNoOutgoingRoute("n")
	b.SetEntryPoint("n")
	g, err := b.Compile(WithRetentionLimit(2))
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	cp := newMemoryCP[state, NoEffect]()
	runner := g.NewRunner(cp)
	for i := range 3 {
		if i == 0 {
			_, err = runner.Start(context.Background(), "th", state{})
		} else {
			_, err = runner.Resume(context.Background(), "th")
		}
		if err != nil {
			t.Fatalf("run %d: %v", i, err)
		}
	}
	history, err := cp.GetHistory(context.Background(), "th", 10)
	if err != nil {
		t.Fatalf("history: %v", err)
	}
	if len(history) > 2 {
		t.Fatalf("expected retention limit 2, got %d snapshots", len(history))
	}
}

func TestOverlayWithoutMergerFails(t *testing.T) {
	t.Parallel()

	type state struct{ V string }

	b := NewGraph[state, NoEffect](func(_ state, u state) state { return u })
	b.AddNode("wait", func(_ context.Context, s state) (state, Directive, error) {
		return s, Suspend("input"), nil
	})
	b.AllowNoOutgoingRoute("wait")
	b.SetEntryPoint("wait")
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	cp := newMemoryCP[state, NoEffect]()
	runner := g.NewRunner(cp)
	_, err = runner.Start(context.Background(), "th", state{V: "base"})
	if err != nil {
		t.Fatalf("start: %v", err)
	}

	_, err = runner.Resume(context.Background(), "th", WithStateOverlay[state, NoEffect](state{V: "overlay"}, nil))
	if !errors.Is(err, ErrOverlayMergerRequired) {
		t.Fatalf("expected ErrOverlayMergerRequired, got %v", err)
	}
}

func TestDeleteOnSuccessViaCompletedEndNodeEdge(t *testing.T) {
	t.Parallel()

	type state struct{}

	b := NewGraph[state, NoEffect](func(_ state, u state) state { return u })
	b.AddNode("work", func(_ context.Context, s state) (state, Directive, error) {
		return s, Completed(), nil
	})
	b.AddEdge("work", EndNode)
	b.SetEntryPoint("work")
	g, err := b.Compile(WithDeleteOnSuccess(true))
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	cp := newMemoryCP[state, NoEffect]()
	_, err = g.NewRunner(cp).Start(context.Background(), "th", state{})
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	if _, err := cp.Load(context.Background(), "th"); !errors.Is(err, ErrThreadNotFound) {
		t.Fatalf("expected deleted snapshot after EndNode completion, load err=%v", err)
	}
}

func TestDeleteOnSuccessPolicy(t *testing.T) {
	t.Parallel()

	type state struct{}

	b := NewGraph[state, NoEffect](func(_ state, u state) state { return u })
	b.AddNode("done", func(_ context.Context, s state) (state, Directive, error) {
		return s, End(), nil
	})
	b.AllowNoOutgoingRoute("done")
	b.SetEntryPoint("done")
	g, err := b.Compile(WithDeleteOnSuccess(true))
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	cp := newMemoryCP[state, NoEffect]()
	_, err = g.NewRunner(cp).Start(context.Background(), "th", state{})
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	if _, err := cp.Load(context.Background(), "th"); !errors.Is(err, ErrThreadNotFound) {
		t.Fatalf("expected deleted snapshot, load err=%v", err)
	}
}

func TestConcurrentStartSameThreadReturnsThreadBusy(t *testing.T) {
	t.Parallel()

	type state struct{}

	b := NewGraph[state, NoEffect](func(_ state, u state) state { return u })
	b.AddNode("work", func(ctx context.Context, s state) (state, Directive, error) {
		<-ctx.Done()
		return s, Suspend("wait"), nil
	})
	b.AllowNoOutgoingRoute("work")
	b.SetEntryPoint("work")
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	lease := NewMemoryLeaseManager()
	runner := g.NewRunnerWithOptions(nil, []RunnerOption[state, NoEffect]{WithLeaseManager[state, NoEffect](lease)})
	leaseOpts := []RunOption[state, NoEffect]{WithRunLease[state, NoEffect]("worker-a", time.Minute)}

	ctx, cancel := context.WithCancel(context.Background())
	firstDone := make(chan error, 1)
	go func() {
		_, startErr := runner.Start(ctx, "busy-th", state{}, leaseOpts...)
		firstDone <- startErr
	}()

	time.Sleep(10 * time.Millisecond)
	_, err = runner.Start(context.Background(), "busy-th", state{}, leaseOpts...)
	if !errors.Is(err, ErrThreadBusy) {
		t.Fatalf("expected ErrThreadBusy, got %v", err)
	}
	cancel()
	<-firstDone
}

func TestLeaseRenewFailureStopsRun(t *testing.T) {
	t.Parallel()

	type state struct{}

	b := NewGraph[state, NoEffect](func(_ state, u state) state { return u })
	b.AddNode("work", func(ctx context.Context, s state) (state, Directive, error) {
		for {
			if err := ctx.Err(); err != nil {
				return s, Completed(), err
			}
			time.Sleep(time.Millisecond)
		}
	})
	b.AddEdge("work", EndNode)
	b.SetEntryPoint("work")
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	lease := &renewFailLease{inner: NewMemoryLeaseManager(), failAfter: 1}
	runner := g.NewRunnerWithOptions(nil, []RunnerOption[state, NoEffect]{WithLeaseManager[state, NoEffect](lease)})

	_, err = runner.Start(
		context.Background(),
		"renew-th",
		state{},
		WithRunLease[state, NoEffect]("worker-a", 30*time.Millisecond),
	)
	if !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("expected ErrLeaseLost, got %v", err)
	}
}

type renewFailLease struct {
	inner     LeaseManager
	renewals  int
	failAfter int
}

func (r *renewFailLease) Acquire(ctx context.Context, threadID, owner string, ttl time.Duration) error {
	return r.inner.Acquire(ctx, threadID, owner, ttl)
}

func (r *renewFailLease) Renew(ctx context.Context, threadID, owner string, ttl time.Duration) error {
	r.renewals++
	if r.failAfter > 0 && r.renewals >= r.failAfter {
		return ErrLeaseLost
	}
	return r.inner.Renew(ctx, threadID, owner, ttl)
}

func (r *renewFailLease) Release(ctx context.Context, threadID, owner string) error {
	return r.inner.Release(ctx, threadID, owner)
}

func TestLeaseHeartbeatIntervalFitsTTL(t *testing.T) {
	t.Parallel()

	ttl := 40 * time.Millisecond
	interval := leaseHeartbeatInterval(ttl)
	if interval > ttl/2 {
		t.Fatalf("heartbeat interval %v exceeds ttl/2 (%v)", interval, ttl/2)
	}
	if interval >= ttl {
		t.Fatalf("heartbeat interval %v must be less than ttl %v", interval, ttl)
	}
}

func TestLeaseHeartbeatPreventsTakeoverDuringActiveRun(t *testing.T) {
	t.Parallel()

	type state struct{}

	b := NewGraph[state, NoEffect](func(_ state, u state) state { return u })
	b.AddNode("work", func(ctx context.Context, s state) (state, Directive, error) {
		deadline := time.Now().Add(200 * time.Millisecond)
		for time.Now().Before(deadline) {
			if err := ctx.Err(); err != nil {
				return s, Completed(), err
			}
			time.Sleep(2 * time.Millisecond)
		}
		return s, End(), nil
	})
	b.AllowNoOutgoingRoute("work")
	b.SetEntryPoint("work")
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	lease := NewMemoryLeaseManager()
	runner := g.NewRunnerWithOptions(nil, []RunnerOption[state, NoEffect]{WithLeaseManager[state, NoEffect](lease)})
	const ttl = 50 * time.Millisecond

	takeoverErr := make(chan error, 1)
	go func() {
		time.Sleep(15 * time.Millisecond)
		takeoverErr <- lease.Acquire(context.Background(), "hb-th", "worker-b", ttl)
	}()

	_, err = runner.Start(
		context.Background(),
		"hb-th",
		state{},
		WithRunLease[state, NoEffect]("worker-a", ttl),
	)
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	if err := <-takeoverErr; !errors.Is(err, ErrLeaseHeld) && !errors.Is(err, ErrThreadBusy) {
		t.Fatalf("expected takeover to be denied while heartbeat renews, got %v", err)
	}
}

func TestLeaseLostWhenTakeoverAfterTTLExpiry(t *testing.T) {
	t.Parallel()

	type state struct{}

	now := time.Date(2026, 5, 28, 14, 0, 0, 0, time.UTC)
	var nowMu sync.Mutex
	currentNow := func() time.Time {
		nowMu.Lock()
		defer nowMu.Unlock()
		return now
	}

	lease := NewMemoryLeaseManager()
	lease.nowFunc = currentNow

	b := NewGraph[state, NoEffect](func(_ state, u state) state { return u })
	b.AddNode("work", func(ctx context.Context, s state) (state, Directive, error) {
		for {
			if err := ctx.Err(); err != nil {
				return s, Completed(), err
			}
			time.Sleep(2 * time.Millisecond)
		}
	})
	b.AllowNoOutgoingRoute("work")
	b.SetEntryPoint("work")
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	runner := g.NewRunnerWithOptions(nil, []RunnerOption[state, NoEffect]{WithLeaseManager[state, NoEffect](lease)})
	const ttl = 40 * time.Millisecond

	done := make(chan error, 1)
	go func() {
		_, startErr := runner.Start(
			context.Background(),
			"takeover-th",
			state{},
			WithRunLease[state, NoEffect]("worker-a", ttl),
		)
		done <- startErr
	}()

	time.Sleep(15 * time.Millisecond)
	nowMu.Lock()
	now = now.Add(ttl + 20*time.Millisecond)
	nowMu.Unlock()
	if acquireErr := lease.Acquire(context.Background(), "takeover-th", "worker-b", ttl); acquireErr != nil {
		t.Fatalf("worker-b acquire after expiry: %v", acquireErr)
	}

	err = <-done
	if !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("expected ErrLeaseLost after takeover, got %v", err)
	}
}

func TestLeaseTTLExpiryAllowsResumeByAnotherOwner(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 5, 28, 12, 0, 0, 0, time.UTC)
	lease := NewMemoryLeaseManager()
	lease.nowFunc = func() time.Time { return now }

	ctx := context.Background()
	const ttl = time.Minute
	if err := lease.Acquire(ctx, "th", "worker-a", ttl); err != nil {
		t.Fatalf("acquire a: %v", err)
	}
	now = now.Add(ttl + time.Second)
	if err := lease.Acquire(ctx, "th", "worker-b", ttl); err != nil {
		t.Fatalf("acquire b after expiry: %v", err)
	}
	if err := lease.Renew(ctx, "th", "worker-a", ttl); !errors.Is(err, ErrLeaseHeld) {
		t.Fatalf("expected ErrLeaseHeld for expired owner, got %v", err)
	}
}

func TestRuntimeCompletedOnExemptNode(t *testing.T) {
	t.Parallel()

	type state struct{}

	b := NewGraph[state, NoEffect](func(_ state, u state) state { return u })
	b.AddNode("bad", func(_ context.Context, s state) (state, Directive, error) {
		return s, Completed(), nil
	})
	b.AllowNoOutgoingRoute("bad")
	b.SetEntryPoint("bad")
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	_, err = g.NewRunner(newMemoryCP[state, NoEffect]()).Start(context.Background(), "th", state{})
	if err == nil || !strings.Contains(err.Error(), "no outgoing edge") {
		t.Fatalf("expected no outgoing edge error, got %v", err)
	}
}

func TestRuntimeConditionalUndeclaredTarget(t *testing.T) {
	t.Parallel()

	type state struct{ Route string }

	b := NewGraph[state, NoEffect](func(_ state, u state) state { return u })
	b.AddNode("router", func(_ context.Context, s state) (state, Directive, error) {
		return s, Completed(), nil
	})
	b.AddNode("allowed", func(_ context.Context, s state) (state, Directive, error) {
		return s, End(), nil
	})
	b.AddConditionalEdge("router", func(_ context.Context, s state) (string, error) {
		return s.Route, nil
	}, "allowed", EndNode)
	b.AllowNoOutgoingRoute("allowed")
	b.SetEntryPoint("router")
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	_, err = g.NewRunner(newMemoryCP[state, NoEffect]()).Start(context.Background(), "th", state{Route: "ghost"})
	if err == nil || !strings.Contains(err.Error(), "undeclared target") {
		t.Fatalf("expected undeclared target error, got %v", err)
	}
}

func TestRuntimeConditionalEmptyTarget(t *testing.T) {
	t.Parallel()

	type state struct{}

	b := NewGraph[state, NoEffect](func(_ state, u state) state { return u })
	b.AddNode("router", func(_ context.Context, s state) (state, Directive, error) {
		return s, Completed(), nil
	})
	b.AddConditionalEdge("router", func(_ context.Context, _ state) (string, error) {
		return "", nil
	}, EndNode)
	b.SetEntryPoint("router")
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	_, err = g.NewRunner(newMemoryCP[state, NoEffect]()).Start(context.Background(), "th", state{})
	if err == nil || !strings.Contains(err.Error(), "empty target") {
		t.Fatalf("expected empty target error, got %v", err)
	}
}

func TestRuntimeConditionalRouterError(t *testing.T) {
	t.Parallel()

	type state struct{}
	routerErr := errors.New("router failed")

	b := NewGraph[state, NoEffect](func(_ state, u state) state { return u })
	b.AddNode("router", func(_ context.Context, s state) (state, Directive, error) {
		return s, Completed(), nil
	})
	b.AddConditionalEdge("router", func(_ context.Context, _ state) (string, error) {
		return "", routerErr
	}, EndNode)
	b.SetEntryPoint("router")
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	_, err = g.NewRunner(newMemoryCP[state, NoEffect]()).Start(context.Background(), "th", state{})
	if err == nil || !errors.Is(err, routerErr) {
		t.Fatalf("expected router error, got %v", err)
	}
}

func assertStreamTerminalEvent[T, E any](
	t *testing.T,
	handle StreamHandle[T, E],
	wantType EventType,
	wantReason string,
) {
	t.Helper()
	events := collectEvents(t, handle.Events(), 2*time.Second)
	if len(events) == 0 {
		t.Fatal("expected events")
	}
	last := events[len(events)-1]
	if last.Type != wantType {
		t.Fatalf("expected %s, got %s", wantType, last.Type)
	}
	if last.Reason != wantReason {
		t.Fatalf("expected reason %q, got %q", wantReason, last.Reason)
	}
}

func TestLifecycleTerminalStreamEvents(t *testing.T) {
	t.Parallel()

	type state struct{ N int }

	cases := []struct {
		name       string
		build      func() *Graph[state, NoEffect]
		run        func(t *testing.T, g *Graph[state, NoEffect]) StreamHandle[state, NoEffect]
		wantType   EventType
		wantReason string
	}{
		{
			name: "suspend",
			build: func() *Graph[state, NoEffect] {
				b := NewGraph[state, NoEffect](func(_ state, u state) state { return u })
				b.AddNode("n", func(_ context.Context, s state) (state, Directive, error) {
					return s, Suspend("wait"), nil
				})
				b.AllowNoOutgoingRoute("n")
				b.SetEntryPoint("n")
				g, err := b.Compile()
				if err != nil {
					t.Fatalf("compile: %v", err)
				}
				return g
			},
			run: func(t *testing.T, g *Graph[state, NoEffect]) StreamHandle[state, NoEffect] {
				t.Helper()
				h, err := g.NewRunner(newMemoryCP[state, NoEffect]()).
					Stream(context.Background(), "suspend-th", state{})
				if err != nil {
					t.Fatalf("stream: %v", err)
				}
				return h
			},
			wantType:   EventSuspended,
			wantReason: "wait",
		},
		{
			name: "handoff_directive",
			build: func() *Graph[state, NoEffect] {
				b := NewGraph[state, NoEffect](func(_ state, u state) state { return u })
				b.AddNode("n", func(_ context.Context, s state) (state, Directive, error) {
					return s, Handoff("bg"), nil
				})
				b.AllowNoOutgoingRoute("n")
				b.SetEntryPoint("n")
				g, err := b.Compile()
				if err != nil {
					t.Fatalf("compile: %v", err)
				}
				return g
			},
			run: func(t *testing.T, g *Graph[state, NoEffect]) StreamHandle[state, NoEffect] {
				t.Helper()
				h, err := g.NewRunner(newMemoryCP[state, NoEffect]()).
					Stream(context.Background(), "handoff-th", state{})
				if err != nil {
					t.Fatalf("stream: %v", err)
				}
				return h
			},
			wantType:   EventHandoff,
			wantReason: "bg",
		},
		{
			name: "context_cancel",
			build: func() *Graph[state, NoEffect] {
				b := NewGraph[state, NoEffect](func(_ state, u state) state { return u })
				b.AddNode("loop", func(_ context.Context, s state) (state, Directive, error) {
					s.N++
					return s, Completed(), nil
				})
				b.AddEdge("loop", "loop")
				b.SetEntryPoint("loop")
				g, err := b.Compile(WithMaxSteps(100))
				if err != nil {
					t.Fatalf("compile: %v", err)
				}
				return g
			},
			run: func(t *testing.T, g *Graph[state, NoEffect]) StreamHandle[state, NoEffect] {
				t.Helper()
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				h, err := g.NewRunner(newMemoryCP[state, NoEffect]()).Stream(ctx, "cancel-th", state{})
				if err != nil {
					t.Fatalf("stream: %v", err)
				}
				return h
			},
			wantType:   EventContextCanceled,
			wantReason: "context_canceled",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			g := tc.build()
			handle := tc.run(t, g)
			assertStreamTerminalEvent(t, handle, tc.wantType, tc.wantReason)
		})
	}
}

func TestRuntimeRetryWithoutAddRetryRoute(t *testing.T) {
	t.Parallel()

	type state struct{ N int }

	b := NewGraph[state, NoEffect](func(_ state, u state) state { return u })
	b.AddNode("worker", func(_ context.Context, s state) (state, Directive, error) {
		return s, Retry(1), nil
	})
	b.AllowNoOutgoingRoute("worker")
	b.SetEntryPoint("worker")
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	_, err = g.NewRunner(newMemoryCP[state, NoEffect]()).Start(context.Background(), "retry-th", state{})
	if err == nil || !strings.Contains(err.Error(), "AddRetryRoute") {
		t.Fatalf("expected AddRetryRoute runtime error, got %v", err)
	}
}
