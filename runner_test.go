package flowy

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
)

type memoryCP[T, E any] struct {
	mu    sync.Mutex
	last  Snapshot[T, E]
	reads map[string]Snapshot[T, E]
	hist  map[string][]Snapshot[T, E]
}

func newMemoryCP[T, E any]() *memoryCP[T, E] {
	return &memoryCP[T, E]{reads: map[string]Snapshot[T, E]{}, hist: map[string][]Snapshot[T, E]{}}
}

func (m *memoryCP[T, E]) Save(
	_ context.Context,
	expectedRevision uint64,
	snapshot Snapshot[T, E],
) (uint64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	items := m.hist[snapshot.ThreadID]
	var current uint64
	if len(items) > 0 {
		current = items[len(items)-1].Revision
	}
	if current != expectedRevision {
		return 0, ErrConcurrencyConflict
	}
	newRevision := expectedRevision + 1
	snapshot.Revision = newRevision
	m.last = snapshot
	m.reads[snapshot.ThreadID] = snapshot
	m.hist[snapshot.ThreadID] = append(items, snapshot)
	return newRevision, nil
}

func (m *memoryCP[T, E]) Load(_ context.Context, threadID string) (Snapshot[T, E], uint64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	s, ok := m.reads[threadID]
	if !ok {
		var zero Snapshot[T, E]
		return zero, 0, ErrThreadNotFound
	}
	return s, s.Revision, nil
}

func (m *memoryCP[T, E]) GetHistory(_ context.Context, threadID string, limit int) ([]Snapshot[T, E], error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	items := m.hist[threadID]
	if limit <= 0 || limit > len(items) {
		limit = len(items)
	}
	out := make([]Snapshot[T, E], 0, limit)
	for i := len(items) - 1; i >= len(items)-limit; i-- {
		out = append(out, items[i])
	}
	return out, nil
}

func (m *memoryCP[T, E]) Prune(_ context.Context, threadID string, retainCount int) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	items := m.hist[threadID]
	if len(items) == 0 {
		return nil
	}
	if retainCount <= 0 {
		delete(m.hist, threadID)
		delete(m.reads, threadID)
		var zero Snapshot[T, E]
		m.last = zero
		return nil
	}
	if len(items) <= retainCount {
		return nil
	}
	trimmed := append([]Snapshot[T, E](nil), items[len(items)-retainCount:]...)
	m.hist[threadID] = trimmed
	m.reads[threadID] = trimmed[len(trimmed)-1]
	m.last = trimmed[len(trimmed)-1]
	return nil
}

func (m *memoryCP[T, E]) Delete(_ context.Context, threadID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.reads, threadID)
	delete(m.hist, threadID)
	return nil
}

// DeleteIfIdle delegates to Delete (dev/test stub; no lease awareness).
func (m *memoryCP[T, E]) DeleteIfIdle(ctx context.Context, threadID string) error {
	return m.Delete(ctx, threadID)
}

func TestRunnerSuspendSavesAndResumeWithPatch(t *testing.T) {
	t.Parallel()
	type state struct {
		Value int
	}

	b := NewGraph[state, string](func(_ state, u state) state { return u })
	b.AddNode("start", func(_ context.Context, s state) (state, Directive, error) {
		s.Value++
		return s, Effect[string](Suspend("wait_input"), "notify-ui"), nil
	})
	b.SetEntryPoint("start")
	b.AllowNoOutgoingRoute("start")
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	cp := newMemoryCP[state, string]()
	runner := g.NewRunner(cp)
	res, err := runner.Start(context.Background(), "th-1", state{Value: 1})
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	if res.Status != RunStatusSuspended {
		t.Fatalf("expected suspended, got %s", res.Status)
	}
	if cp.last.ThreadID != "th-1" {
		t.Fatalf("snapshot not saved")
	}
	if len(res.Effects) != 1 {
		t.Fatalf("expected one effect, got %d", len(res.Effects))
	}

	resumed, err := runner.Resume(
		context.Background(),
		res.ResumeToken,
		WithStateOverlay[state, string](state{Value: 10}, func(base, overlay state) state {
			base.Value += overlay.Value
			return base
		}),
	)
	if err != nil {
		t.Fatalf("resume: %v", err)
	}
	if resumed.State.Value != 13 {
		t.Fatalf("unexpected resumed state (overlay + re-entry): %+v", resumed.State)
	}
}

func TestRunnerContextCancelSavesSnapshot(t *testing.T) {
	t.Parallel()
	type state struct{ N int }
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
	cp := newMemoryCP[state, NoEffect]()
	runner := g.NewRunner(cp)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	res, err := runner.Start(ctx, "ctx-1", state{})
	if err == nil {
		t.Fatal("expected context error")
	}
	if res == nil || res.Status != RunStatusContextCanceled {
		t.Fatalf("expected context_canceled status, got res=%+v", res)
	}
	if res.ResumeToken.ThreadID != "ctx-1" || res.ResumeToken.SnapshotRevision == 0 {
		t.Fatalf("cancel path must populate ResumeToken after save, got %+v", res.ResumeToken)
	}
	if cp.last.ThreadID != "ctx-1" {
		t.Fatal("expected saved snapshot on context cancellation")
	}
}

func TestRunnerRetryWithFallback(t *testing.T) {
	t.Parallel()
	type state struct{ Attempts int }
	b := NewGraph[state, NoEffect](func(_ state, u state) state { return u })
	b.AddNode("work", func(_ context.Context, s state) (state, Directive, error) {
		s.Attempts++
		return s, Retry(1), nil
	})
	b.AddNode("fallback", func(_ context.Context, s state) (state, Directive, error) {
		return s, End(), nil
	})
	b.AddRetryRoute("work", "fallback")
	b.SetEntryPoint("work")
	b.AllowNoOutgoingRoute("work")
	b.AllowNoOutgoingRoute("fallback")
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	res, err := g.NewRunner(newMemoryCP[state, NoEffect]()).Start(context.Background(), "retry-1", state{})
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	if res.State.Attempts != 1 {
		t.Fatalf("expected 1 attempt, got %d", res.State.Attempts)
	}
}

func TestRunnerRetryBudgetExceeded(t *testing.T) {
	t.Parallel()

	type state struct {
		Attempts int
	}

	b := NewGraph[state, NoEffect](func(_ state, u state) state { return u })
	b.AddNode("work", func(_ context.Context, s state) (state, Directive, error) {
		s.Attempts++
		return s, Retry(1), nil
	})
	b.AddNode("fallback", func(_ context.Context, s state) (state, Directive, error) {
		return s, Completed(), nil
	})
	b.AddRetryRoute("work", "fallback")
	b.AddEdge("fallback", "work")
	b.SetEntryPoint("work")
	b.AllowNoOutgoingRoute("work")
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	runner := g.NewRunner(newMemoryCP[state, NoEffect]())
	res, err := runner.Start(context.Background(), "retry-budget", state{})
	if !errors.Is(err, ErrRetryBudgetExceeded) {
		t.Fatalf("expected ErrRetryBudgetExceeded, got %v", err)
	}
	if res == nil || res.Reason != ErrRetryBudgetExceeded.Error() {
		t.Fatalf("sync reason: want %q, got res=%+v", ErrRetryBudgetExceeded.Error(), res)
	}
}

func TestRunnerRetryZeroAttemptsFails(t *testing.T) {
	t.Parallel()

	type state struct{}
	b := NewGraph[state, NoEffect](func(_ state, u state) state { return u })
	b.AddNode("work", func(_ context.Context, s state) (state, Directive, error) {
		return s, Retry(0), nil
	})
	b.AddNode("fallback", func(_ context.Context, s state) (state, Directive, error) {
		return s, End(), nil
	})
	b.AddRetryRoute("work", "fallback")
	b.AllowNoOutgoingRoute("work")
	b.AllowNoOutgoingRoute("fallback")
	b.SetEntryPoint("work")
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	runner := g.NewRunner(newMemoryCP[state, NoEffect]())
	res, err := runner.Start(context.Background(), "retry-zero", state{})
	if err == nil || !strings.Contains(err.Error(), "maxAttempts > 0") {
		t.Fatalf("expected retry validation error, got %v", err)
	}
	if res == nil || res.Reason == "" || !strings.Contains(res.Reason, "maxAttempts > 0") {
		t.Fatalf("sync reason must contain maxAttempts > 0, got res=%+v", res)
	}

	handle, streamErr := runner.Stream(context.Background(), "retry-zero-stream", state{})
	if streamErr != nil {
		t.Fatalf("stream: %v", streamErr)
	}
	events, waitErr := CollectEventsAndWait(context.Background(), handle)
	if waitErr == nil || !strings.Contains(waitErr.Error(), "maxAttempts > 0") {
		t.Fatalf("stream Wait: expected retry validation error, got %v", waitErr)
	}
	assertEventFailedReasonMatchesSync(t, events, res.Reason)
}

func TestUnsupportedDirectiveFailsWithReasonParity(t *testing.T) {
	t.Parallel()

	type state struct{}
	const unknownKind directiveKind = 42

	b := NewGraph[state, NoEffect](func(_ state, u state) state { return u })
	b.AddNode("bad", func(_ context.Context, s state) (state, Directive, error) {
		return s, directiveWithKind(unknownKind), nil
	})
	b.AllowNoOutgoingRoute("bad")
	b.SetEntryPoint("bad")
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	wantReason := "flowy: node returned unsupported directive"
	runner := g.NewRunner(newMemoryCP[state, NoEffect]())
	syncRes, syncErr := runner.Start(context.Background(), "unsupported-sync", state{})
	if syncErr == nil || syncErr.Error() != wantReason {
		t.Fatalf("expected %q, got res=%+v err=%v", wantReason, syncRes, syncErr)
	}
	if syncRes == nil || syncRes.Reason != wantReason {
		t.Fatalf("sync reason: want %q, got res=%+v", wantReason, syncRes)
	}

	handle, streamErr := runner.Stream(context.Background(), "unsupported-stream", state{})
	if streamErr != nil {
		t.Fatalf("stream: %v", streamErr)
	}
	events, waitErr := CollectEventsAndWait(context.Background(), handle)
	if waitErr == nil || waitErr.Error() != wantReason {
		t.Fatalf("stream Wait: want %q, got %v", wantReason, waitErr)
	}
	assertEventFailedReasonMatchesSync(t, events, wantReason)
}

func TestRunnerMaxStepsExceeded(t *testing.T) {
	t.Parallel()
	type state struct{}
	b := NewGraph[state, NoEffect](func(_ state, u state) state { return u })
	b.AddNode("loop", func(_ context.Context, s state) (state, Directive, error) {
		return s, Completed(), nil
	})
	b.AddEdge("loop", "loop")
	b.SetEntryPoint("loop")
	g, err := b.Compile(WithMaxSteps(1))
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	res, err := g.NewRunner(newMemoryCP[state, NoEffect]()).Start(context.Background(), "loop-1", state{})
	if !errors.Is(err, ErrMaxStepsExceeded) {
		t.Fatalf("expected ErrMaxStepsExceeded, got %v", err)
	}
	if res == nil || res.Reason != ErrMaxStepsExceeded.Error() {
		t.Fatalf("sync reason: want %q, got res=%+v", ErrMaxStepsExceeded.Error(), res)
	}
}

type interceptState struct {
	Value string
}

type testInterceptor struct{}

func (testInterceptor) BeforeSave(_ context.Context, state *interceptState) error {
	state.Value = "saved:" + state.Value
	return nil
}

func (testInterceptor) AfterLoad(_ context.Context, state *interceptState) error {
	state.Value = "loaded:" + state.Value
	return nil
}

func TestInterceptorsAreApplied(t *testing.T) {
	t.Parallel()
	b := NewGraph[interceptState, NoEffect](func(_ interceptState, u interceptState) interceptState { return u })
	b.AddNode("n", func(_ context.Context, s interceptState) (interceptState, Directive, error) {
		if strings.HasPrefix(s.Value, "loaded:") {
			return s, End(), nil
		}
		return s, Suspend("x"), nil
	})
	b.SetEntryPoint("n")
	b.AllowNoOutgoingRoute("n")
	g, _ := b.Compile()
	cp := newMemoryCP[interceptState, NoEffect]()
	r := g.NewRunner(cp, testInterceptor{})
	_, err := r.Start(context.Background(), "i", interceptState{Value: "v"})
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	if cp.last.State.Value != "saved:v" {
		t.Fatalf("before save interceptor not applied: %+v", cp.last.State)
	}

	snap := cp.reads["i"]
	snap.State = interceptState{Value: "v2"}
	cp.reads["i"] = snap
	res, err := g.NewRunner(cp, testInterceptor{}).Resume(
		context.Background(),
		ResumeToken{ThreadID: snap.ThreadID, SnapshotRevision: snap.Revision},
	)
	if err != nil {
		t.Fatalf("resume: %v", err)
	}
	if res.State.Value != "loaded:v2" {
		t.Fatalf("after load interceptor not applied: %+v", res.State)
	}
}

func TestCompletedUsesGraphEdges(t *testing.T) {
	t.Parallel()
	type state struct{ Steps int }
	b := NewGraph[state, NoEffect](func(_ state, u state) state { return u })
	b.AddNode("a", func(_ context.Context, s state) (state, Directive, error) {
		s.Steps++
		return s, Completed(), nil
	})
	b.AddNode("b", func(_ context.Context, s state) (state, Directive, error) {
		s.Steps++
		return s, End(), nil
	})
	b.AddEdge("a", "b")
	b.AllowNoOutgoingRoute("b")
	b.SetEntryPoint("a")
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	res, err := g.NewRunner(newMemoryCP[state, NoEffect]()).Start(context.Background(), "edge", state{})
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	if res.State.Steps != 2 {
		t.Fatalf("expected two steps, got %d", res.State.Steps)
	}
}

func TestConditionalEdgeRouting(t *testing.T) {
	t.Parallel()
	type state struct {
		Route string
		Seen  string
	}
	b := NewGraph[state, NoEffect](func(_ state, u state) state { return u })
	b.AddNode("router", func(_ context.Context, s state) (state, Directive, error) {
		return s, Completed(), nil
	})
	b.AddNode("x", func(_ context.Context, s state) (state, Directive, error) {
		s.Seen = "x"
		return s, End(), nil
	})
	b.AddConditionalEdge("router", func(_ context.Context, s state) (string, error) {
		if s.Route == "x" {
			return "x", nil
		}
		return EndNode, nil
	}, "x", EndNode)
	b.AllowNoOutgoingRoute("x")
	b.SetEntryPoint("router")
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	res, err := g.NewRunner(newMemoryCP[state, NoEffect]()).Start(context.Background(), "cond", state{Route: "x"})
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	if res.State.Seen != "x" {
		t.Fatalf("unexpected state: %+v", res.State)
	}
}

func TestSubgraphSuspendPropagates(t *testing.T) {
	t.Parallel()
	type childState struct {
		Pause bool
	}
	type parentState struct {
		Child childState
	}
	subBuilder := NewGraph[childState, NoEffect](func(_ childState, u childState) childState { return u })
	subBuilder.AddNode("check", func(_ context.Context, s childState) (childState, Directive, error) {
		if s.Pause {
			return s, Suspend("wait_child"), nil
		}
		return s, End(), nil
	})
	subBuilder.SetEntryPoint("check")
	subBuilder.AllowNoOutgoingRoute("check")
	sub, err := subBuilder.Compile()
	if err != nil {
		t.Fatalf("compile subgraph: %v", err)
	}

	parentBuilder := NewGraph[parentState, NoEffect](func(_ parentState, u parentState) parentState { return u })
	parentBuilder.AddNode("sub", SubgraphNode(
		sub,
		func(s parentState) childState { return s.Child },
		func(s parentState, c childState) parentState {
			s.Child = c
			return s
		},
	))
	parentBuilder.SetEntryPoint("sub")
	parentBuilder.AllowNoOutgoingRoute("sub")
	parentGraph, err := parentBuilder.Compile()
	if err != nil {
		t.Fatalf("compile parent graph: %v", err)
	}

	res, err := parentGraph.NewRunner(newMemoryCP[parentState, NoEffect]()).
		Start(context.Background(), "sub", parentState{
			Child: childState{Pause: true},
		})
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	if res.Status != RunStatusSuspended {
		t.Fatalf("expected suspended status, got %s", res.Status)
	}
	if res.Reason != "wait_child" {
		t.Fatalf("unexpected suspend reason %q", res.Reason)
	}
}

func TestBuilderUseAppliesOnionOrder(t *testing.T) {
	t.Parallel()
	type state struct{ Trace []string }
	reducer := func(current state, update state) state {
		current.Trace = append(current.Trace, update.Trace...)
		return current
	}

	record := func(label string) NodeMiddleware[state, NoEffect] {
		return func(next Node[state, NoEffect]) Node[state, NoEffect] {
			return func(ctx context.Context, s state) (state, Directive, error) {
				s.Trace = append(s.Trace, "before:"+label)
				out, directive, err := next(ctx, s)
				out.Trace = append(out.Trace, "after:"+label)
				return out, directive, err
			}
		}
	}

	b := NewGraph[state, NoEffect](reducer)
	b.Use(record("one"), record("two"))
	b.AddNode("n", func(_ context.Context, s state) (state, Directive, error) {
		s.Trace = append(s.Trace, "node")
		return s, End(), nil
	})
	b.SetEntryPoint("n")
	b.AllowNoOutgoingRoute("n")
	g, _ := b.Compile()
	res, err := g.NewRunner(newMemoryCP[state, NoEffect]()).Start(context.Background(), "mw-1", state{})
	if err != nil {
		t.Fatalf("start: %v", err)
	}

	got := fmt.Sprint(res.State.Trace)
	want := "[before:one before:two node after:two after:one]"
	if got != want {
		t.Fatalf("unexpected middleware order: got %s want %s", got, want)
	}
}

func TestRecoverMiddlewareConvertsPanicToError(t *testing.T) {
	t.Parallel()
	type state struct{}
	b := NewGraph[state, NoEffect](func(_ state, u state) state { return u })
	b.Use(RecoverMiddleware[state, NoEffect]())
	b.AddNode("panic", func(_ context.Context, _ state) (state, Directive, error) {
		panic("boom")
	})
	b.SetEntryPoint("panic")
	b.AllowNoOutgoingRoute("panic")
	g, _ := b.Compile()
	_, err := g.NewRunner(newMemoryCP[state, NoEffect]()).Start(context.Background(), "panic-1", state{})
	if err == nil {
		t.Fatal("expected error from recovered panic")
	}
}

func TestNodeMiddlewareContextReachesWrappedNode(t *testing.T) {
	t.Parallel()
	type state struct {
		Seen string
	}
	type ctxKey string
	const key ctxKey = "trace"

	b := NewGraph[state, NoEffect](func(_ state, u state) state { return u })
	b.Use(func(next Node[state, NoEffect]) Node[state, NoEffect] {
		return func(ctx context.Context, s state) (state, Directive, error) {
			ctx = context.WithValue(ctx, key, "trace-123")
			return next(ctx, s)
		}
	})
	b.AddNode("n", func(ctx context.Context, s state) (state, Directive, error) {
		if traceID, _ := ctx.Value(key).(string); traceID != "" {
			s.Seen = traceID
		}
		return s, End(), nil
	})
	b.SetEntryPoint("n")
	b.AllowNoOutgoingRoute("n")
	g, _ := b.Compile()

	res, err := g.NewRunner(newMemoryCP[state, NoEffect]()).Start(context.Background(), "ctx-propagation", state{})
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	if res.State.Seen != "trace-123" {
		t.Fatalf("expected trace-123, got %q", res.State.Seen)
	}
}

func TestNodeMiddlewareContextDoesNotLeakToNextGraphNode(t *testing.T) {
	t.Parallel()
	type state struct {
		Seen []string
	}
	type ctxKey string
	const key ctxKey = "chain"

	b := NewGraph[state, NoEffect](func(_ state, u state) state { return u })
	b.Use(func(next Node[state, NoEffect]) Node[state, NoEffect] {
		return func(ctx context.Context, s state) (state, Directive, error) {
			prefix, _ := ctx.Value(key).(string)
			nodeName := NodeNameFromContext(ctx)
			ctx = context.WithValue(ctx, key, prefix+"->"+nodeName)
			return next(ctx, s)
		}
	})
	b.AddNode("a", func(ctx context.Context, s state) (state, Directive, error) {
		s.Seen = append(s.Seen, ctx.Value(key).(string))
		return s, Completed(), nil
	})
	b.AddNode("b", func(ctx context.Context, s state) (state, Directive, error) {
		s.Seen = append(s.Seen, ctx.Value(key).(string))
		return s, End(), nil
	})
	b.AddEdge("a", "b")
	b.AllowNoOutgoingRoute("b")
	b.SetEntryPoint("a")
	g, _ := b.Compile()

	res, err := g.NewRunner(newMemoryCP[state, NoEffect]()).Start(context.Background(), "ctx-no-leak", state{})
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	if fmt.Sprint(res.State.Seen) != "[->a ->b]" {
		t.Fatalf("expected no context leakage between nodes, got %v", res.State.Seen)
	}
}

func TestNodeMiddlewareContextPropagationOnResume(t *testing.T) {
	t.Parallel()
	type state struct {
		Seen string
		Pass int
	}
	type ctxKey string
	const key ctxKey = "resume-trace"

	cp := newMemoryCP[state, NoEffect]()

	b := NewGraph[state, NoEffect](func(_ state, u state) state { return u })
	b.Use(func(next Node[state, NoEffect]) Node[state, NoEffect] {
		return func(ctx context.Context, s state) (state, Directive, error) {
			ctx = context.WithValue(ctx, key, "resume-ctx")
			return next(ctx, s)
		}
	})
	b.AddNode("resume_node", func(ctx context.Context, s state) (state, Directive, error) {
		s.Pass++
		s.Seen, _ = ctx.Value(key).(string)
		if s.Pass == 1 {
			return s, Suspend("hold"), nil
		}
		return s, End(), nil
	})
	b.SetEntryPoint("resume_node")
	b.AllowNoOutgoingRoute("resume_node")
	g, _ := b.Compile()

	runner := g.NewRunner(cp)
	_, err := runner.Start(context.Background(), "resume-mw", state{})
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	res, err := resumeLoaded(context.Background(), runner, cp, "resume-mw")
	if err != nil {
		t.Fatalf("resume: %v", err)
	}
	if res.State.Seen != "resume-ctx" {
		t.Fatalf("expected middleware context on resume, got %q", res.State.Seen)
	}
}

func TestResumeRestoresTelemetryContext(t *testing.T) {
	type state struct {
		Trace string
		Round int
	}
	type ctxKey string
	const key ctxKey = "trace"

	SetTelemetryBridge(testTelemetryBridge{
		extract: func(ctx context.Context) map[string]string {
			traceID, _ := ctx.Value(key).(string)
			if traceID == "" {
				return nil
			}
			return map[string]string{"trace_id": traceID}
		},
		inject: func(ctx context.Context, meta map[string]string) context.Context {
			traceID := meta["trace_id"]
			if traceID == "" {
				return ctx
			}
			return context.WithValue(ctx, key, traceID)
		},
	})
	defer SetTelemetryBridge(nil)

	cp := newMemoryCP[state, NoEffect]()
	g, _ := NewGraph[state, NoEffect](func(_ state, u state) state { return u }).
		AddNode("wait", func(ctx context.Context, s state) (state, Directive, error) {
			s.Round++
			s.Trace, _ = ctx.Value(key).(string)
			if s.Round == 1 {
				return s, Suspend("hold"), nil
			}
			return s, End(), nil
		}).
		AllowNoOutgoingRoute("wait").
		SetEntryPoint("wait").
		Compile()

	runner := g.NewRunner(cp)
	_, err := runner.Start(context.WithValue(context.Background(), key, "trace-42"), "trace-thread", state{})
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	if cp.last.RunMeta.TelemetryContext["trace_id"] != "trace-42" {
		t.Fatalf("expected telemetry saved in run meta, got %+v", cp.last.RunMeta.TelemetryContext)
	}

	res, err := resumeLoaded(context.Background(), runner, cp, "trace-thread")
	if err != nil {
		t.Fatalf("resume: %v", err)
	}
	if res.State.Trace != "trace-42" {
		t.Fatalf("expected restored trace context, got %q", res.State.Trace)
	}
}

type testTelemetryBridge struct {
	extract func(ctx context.Context) map[string]string
	inject  func(ctx context.Context, metadata map[string]string) context.Context
}

func (t testTelemetryBridge) Extract(ctx context.Context) map[string]string {
	if t.extract == nil {
		return nil
	}
	return t.extract(ctx)
}

func (t testTelemetryBridge) Inject(ctx context.Context, metadata map[string]string) context.Context {
	if t.inject == nil {
		return ctx
	}
	return t.inject(ctx, metadata)
}
