package flowy

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"
)

type memoryCP[T any] struct {
	last  Snapshot[T]
	reads map[string]Snapshot[T]
	hist  map[string][]Snapshot[T]
}

func newMemoryCP[T any]() *memoryCP[T] {
	return &memoryCP[T]{reads: map[string]Snapshot[T]{}, hist: map[string][]Snapshot[T]{}}
}

func (m *memoryCP[T]) Save(_ context.Context, snapshot Snapshot[T]) error {
	m.last = snapshot
	m.reads[snapshot.ThreadID] = snapshot
	m.hist[snapshot.ThreadID] = append(m.hist[snapshot.ThreadID], snapshot)
	return nil
}

func (m *memoryCP[T]) Load(_ context.Context, threadID string) (Snapshot[T], error) {
	s, ok := m.reads[threadID]
	if !ok {
		var zero Snapshot[T]
		return zero, ErrThreadNotFound
	}
	return s, nil
}

func (m *memoryCP[T]) GetHistory(_ context.Context, threadID string, limit int) ([]Snapshot[T], error) {
	items := m.hist[threadID]
	if limit <= 0 || limit > len(items) {
		limit = len(items)
	}
	out := make([]Snapshot[T], 0, limit)
	for i := len(items) - 1; i >= len(items)-limit; i-- {
		out = append(out, items[i])
	}
	return out, nil
}

func (m *memoryCP[T]) Prune(_ context.Context, threadID string, retainCount int) error {
	items := m.hist[threadID]
	if len(items) == 0 {
		return nil
	}
	if retainCount <= 0 {
		delete(m.hist, threadID)
		delete(m.reads, threadID)
		var zero Snapshot[T]
		m.last = zero
		return nil
	}
	if len(items) <= retainCount {
		return nil
	}
	trimmed := append([]Snapshot[T](nil), items[len(items)-retainCount:]...)
	m.hist[threadID] = trimmed
	m.reads[threadID] = trimmed[len(trimmed)-1]
	m.last = trimmed[len(trimmed)-1]
	return nil
}

func TestRunnerSuspendSavesAndResumeWithPatch(t *testing.T) {
	t.Parallel()
	type state struct {
		Value int
	}

	b := NewGraph(func(_ state, u state) state { return u })
	b.AddNode("start", func(_ context.Context, s state) (state, Directive, error) {
		s.Value++
		return s, Effect(Suspend("wait_input"), "notify-ui"), nil
	})
	b.SetEntryPoint("start")
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	cp := newMemoryCP[state]()
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

	cp.reads["th-1"] = Snapshot[state]{
		ThreadID: "th-1",
		NodeID:   "done",
		State:    cp.last.State,
		RunMeta:  cp.last.RunMeta,
		Effects:  cp.last.Effects,
	}
	b2 := NewGraph(func(_ state, u state) state { return u })
	b2.AddNode("done", func(_ context.Context, s state) (state, Directive, error) {
		return s, End(), nil
	})
	b2.SetEntryPoint("done")
	g2, err := b2.Compile()
	if err != nil {
		t.Fatalf("compile resumed graph: %v", err)
	}
	runner2 := g2.NewRunner(cp)
	resumed, err := runner2.Resume(context.Background(), "th-1", WithStatePatch(func(s *state) {
		s.Value += 10
	}))
	if err != nil {
		t.Fatalf("resume: %v", err)
	}
	if resumed.State.Value != 12 {
		t.Fatalf("unexpected resumed state: %+v", resumed.State)
	}
}

func TestRunnerContextCancelSavesSnapshot(t *testing.T) {
	t.Parallel()
	type state struct{ N int }
	b := NewGraph(func(_ state, u state) state { return u })
	b.AddNode("loop", func(_ context.Context, s state) (state, Directive, error) {
		s.N++
		return s, Next("loop"), nil
	})
	b.SetEntryPoint("loop")
	g, err := b.Compile(WithMaxSteps(100))
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	cp := newMemoryCP[state]()
	runner := g.NewRunner(cp)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = runner.Start(ctx, "ctx-1", state{})
	if err == nil {
		t.Fatal("expected context error")
	}
	if cp.last.ThreadID != "ctx-1" {
		t.Fatal("expected saved snapshot on context cancellation")
	}
}

func TestRunnerRetryWithFallback(t *testing.T) {
	t.Parallel()
	type state struct{ Attempts int }
	b := NewGraph(func(_ state, u state) state { return u })
	b.AddNode("work", func(_ context.Context, s state) (state, Directive, error) {
		s.Attempts++
		return s, Retry(1, "fallback"), nil
	})
	b.AddNode("fallback", func(_ context.Context, s state) (state, Directive, error) {
		return s, End(), nil
	})
	b.SetEntryPoint("work")
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	res, err := g.NewRunner(newMemoryCP[state]()).Start(context.Background(), "retry-1", state{})
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	if res.State.Attempts != 1 {
		t.Fatalf("expected 1 attempt, got %d", res.State.Attempts)
	}
}

func TestRunnerMaxStepsExceeded(t *testing.T) {
	t.Parallel()
	type state struct{}
	b := NewGraph(func(_ state, u state) state { return u })
	b.AddNode("loop", func(_ context.Context, s state) (state, Directive, error) {
		return s, Next("loop"), nil
	})
	b.SetEntryPoint("loop")
	g, err := b.Compile(WithMaxSteps(1))
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	_, err = g.NewRunner(newMemoryCP[state]()).Start(context.Background(), "loop-1", state{})
	if !errors.Is(err, ErrMaxStepsExceeded) {
		t.Fatalf("expected ErrMaxStepsExceeded, got %v", err)
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
	b := NewGraph(func(_ interceptState, u interceptState) interceptState { return u })
	b.AddNode("n", func(_ context.Context, s interceptState) (interceptState, Directive, error) {
		return s, Suspend("x"), nil
	})
	b.SetEntryPoint("n")
	g, _ := b.Compile()
	cp := newMemoryCP[interceptState]()
	r := g.NewRunner(cp, testInterceptor{})
	_, err := r.Start(context.Background(), "i", interceptState{Value: "v"})
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	if cp.last.State.Value != "saved:v" {
		t.Fatalf("before save interceptor not applied: %+v", cp.last.State)
	}

	cp.reads["i"] = Snapshot[interceptState]{
		ThreadID: "i",
		NodeID:   "end",
		State:    interceptState{Value: "v2"},
		RunMeta:  RunMetadata{SegmentStartTime: time.Now().UTC(), RetryCounts: map[string]int{}},
	}
	b2 := NewGraph(func(_ interceptState, u interceptState) interceptState { return u })
	b2.AddNode("end", func(_ context.Context, s interceptState) (interceptState, Directive, error) {
		return s, End(), nil
	})
	b2.SetEntryPoint("end")
	g2, _ := b2.Compile()
	res, err := g2.NewRunner(cp, testInterceptor{}).Resume(context.Background(), "i")
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
	b := NewGraph(func(_ state, u state) state { return u })
	b.AddNode("a", func(_ context.Context, s state) (state, Directive, error) {
		s.Steps++
		return s, Completed(), nil
	})
	b.AddNode("b", func(_ context.Context, s state) (state, Directive, error) {
		s.Steps++
		return s, End(), nil
	})
	b.AddEdge("a", "b")
	b.SetEntryPoint("a")
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	res, err := g.NewRunner(newMemoryCP[state]()).Start(context.Background(), "edge", state{})
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
	b := NewGraph(func(_ state, u state) state { return u })
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
	})
	b.SetEntryPoint("router")
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	res, err := g.NewRunner(newMemoryCP[state]()).Start(context.Background(), "cond", state{Route: "x"})
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
	subBuilder := NewGraph(func(_ childState, u childState) childState { return u })
	subBuilder.AddNode("check", func(_ context.Context, s childState) (childState, Directive, error) {
		if s.Pause {
			return s, Suspend("wait_child"), nil
		}
		return s, End(), nil
	})
	subBuilder.SetEntryPoint("check")
	sub, err := subBuilder.Compile()
	if err != nil {
		t.Fatalf("compile subgraph: %v", err)
	}

	parentBuilder := NewGraph(func(_ parentState, u parentState) parentState { return u })
	parentBuilder.AddNode("sub", SubgraphNode(
		sub,
		func(s parentState) childState { return s.Child },
		func(s parentState, c childState) parentState {
			s.Child = c
			return s
		},
	))
	parentBuilder.SetEntryPoint("sub")
	parentGraph, err := parentBuilder.Compile()
	if err != nil {
		t.Fatalf("compile parent graph: %v", err)
	}

	res, err := parentGraph.NewRunner(newMemoryCP[parentState]()).Start(context.Background(), "sub", parentState{
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

	record := func(label string) NodeMiddleware[state] {
		return func(next Node[state]) Node[state] {
			return func(ctx context.Context, s state) (state, Directive, error) {
				s.Trace = append(s.Trace, "before:"+label)
				out, directive, err := next(ctx, s)
				out.Trace = append(out.Trace, "after:"+label)
				return out, directive, err
			}
		}
	}

	b := NewGraph(reducer)
	b.Use(record("one"), record("two"))
	b.AddNode("n", func(_ context.Context, s state) (state, Directive, error) {
		s.Trace = append(s.Trace, "node")
		return s, End(), nil
	})
	b.SetEntryPoint("n")
	g, _ := b.Compile()
	res, err := g.NewRunner(newMemoryCP[state]()).Start(context.Background(), "mw-1", state{})
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
	b := NewGraph(func(_ state, u state) state { return u })
	b.Use(RecoverMiddleware[state]())
	b.AddNode("panic", func(_ context.Context, _ state) (state, Directive, error) {
		panic("boom")
	})
	b.SetEntryPoint("panic")
	g, _ := b.Compile()
	_, err := g.NewRunner(newMemoryCP[state]()).Start(context.Background(), "panic-1", state{})
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

	b := NewGraph(func(_ state, u state) state { return u })
	b.Use(func(next Node[state]) Node[state] {
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
	g, _ := b.Compile()

	res, err := g.NewRunner(newMemoryCP[state]()).Start(context.Background(), "ctx-propagation", state{})
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

	b := NewGraph(func(_ state, u state) state { return u })
	b.Use(func(next Node[state]) Node[state] {
		return func(ctx context.Context, s state) (state, Directive, error) {
			prefix, _ := ctx.Value(key).(string)
			nodeName := NodeNameFromContext(ctx)
			ctx = context.WithValue(ctx, key, prefix+"->"+nodeName)
			return next(ctx, s)
		}
	})
	b.AddNode("a", func(ctx context.Context, s state) (state, Directive, error) {
		s.Seen = append(s.Seen, ctx.Value(key).(string))
		return s, Next("b"), nil
	})
	b.AddNode("b", func(ctx context.Context, s state) (state, Directive, error) {
		s.Seen = append(s.Seen, ctx.Value(key).(string))
		return s, End(), nil
	})
	b.SetEntryPoint("a")
	g, _ := b.Compile()

	res, err := g.NewRunner(newMemoryCP[state]()).Start(context.Background(), "ctx-no-leak", state{})
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
	}
	type ctxKey string
	const key ctxKey = "resume-trace"

	cp := newMemoryCP[state]()
	cp.reads["resume-mw"] = Snapshot[state]{
		ThreadID: "resume-mw",
		NodeID:   "resume_node",
		Revision: 3,
		State:    state{},
		RunMeta: RunMetadata{
			SegmentStartTime: time.Now().UTC(),
			RetryCounts:      map[string]int{},
		},
	}

	b := NewGraph(func(_ state, u state) state { return u })
	b.Use(func(next Node[state]) Node[state] {
		return func(ctx context.Context, s state) (state, Directive, error) {
			ctx = context.WithValue(ctx, key, "resume-ctx")
			return next(ctx, s)
		}
	})
	b.AddNode("resume_node", func(ctx context.Context, s state) (state, Directive, error) {
		s.Seen, _ = ctx.Value(key).(string)
		return s, End(), nil
	})
	b.SetEntryPoint("resume_node")
	g, _ := b.Compile()

	res, err := g.NewRunner(cp).Resume(context.Background(), "resume-mw")
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

	cp := newMemoryCP[state]()
	gStart, _ := NewGraph(func(_ state, u state) state { return u }).
		AddNode("wait", func(_ context.Context, s state) (state, Directive, error) {
			return s, Suspend("hold"), nil
		}).
		SetEntryPoint("wait").
		Compile()

	_, err := gStart.NewRunner(cp).
		Start(context.WithValue(context.Background(), key, "trace-42"), "trace-thread", state{})
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	if cp.last.RunMeta.TelemetryContext["trace_id"] != "trace-42" {
		t.Fatalf("expected telemetry saved in run meta, got %+v", cp.last.RunMeta.TelemetryContext)
	}

	cp.reads["trace-thread"] = Snapshot[state]{
		ThreadID: "trace-thread",
		NodeID:   "end",
		Revision: cp.last.Revision,
		State:    state{},
		RunMeta:  cp.last.RunMeta,
	}

	gResume, _ := NewGraph(func(_ state, u state) state { return u }).
		AddNode("end", func(ctx context.Context, s state) (state, Directive, error) {
			s.Trace, _ = ctx.Value(key).(string)
			return s, End(), nil
		}).
		SetEntryPoint("end").
		Compile()

	res, err := gResume.NewRunner(cp).Resume(context.Background(), "trace-thread")
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
