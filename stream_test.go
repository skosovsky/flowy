package flowy

import (
	"context"
	"errors"
	"testing"
	"time"

	"go.uber.org/goleak"
)

func TestStreamEmitsSuspendBeforeClose(t *testing.T) {
	type state struct{ N int }
	b := NewGraph(func(_ state, u state) state { return u })
	b.AddNode("start", func(_ context.Context, s state) (state, Directive, error) {
		s.N++
		return s, Suspend("wait"), nil
	})
	b.SetEntryPoint("start")
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	handle, err := g.NewRunner(newMemoryCP[state]()).Stream(context.Background(), "suspend-1", state{})
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
	events := collectEvents(t, handle.Events(), 2*time.Second)
	if len(events) < 3 {
		t.Fatalf("expected at least 3 events, got %d", len(events))
	}
	if events[len(events)-1].Type != EventSuspended {
		t.Fatalf("expected final suspended event, got %s", events[len(events)-1].Type)
	}
	if err := handle.Done(); err != nil {
		t.Fatalf("unexpected done error: %v", err)
	}
}

func TestStreamEmitsCompletedBeforeClose(t *testing.T) {
	type state struct{ N int }
	b := NewGraph(func(_ state, u state) state { return u })
	b.AddNode("start", func(_ context.Context, s state) (state, Directive, error) {
		s.N++
		return s, End(), nil
	})
	b.SetEntryPoint("start")
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	handle, err := g.NewRunner(newMemoryCP[state]()).Stream(context.Background(), "completed-1", state{})
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
	events := collectEvents(t, handle.Events(), 2*time.Second)
	if len(events) == 0 || events[len(events)-1].Type != EventCompleted {
		t.Fatalf("expected final completed event, got %+v", events)
	}
	if err := handle.Done(); err != nil {
		t.Fatalf("unexpected done error: %v", err)
	}
}

func TestStreamEffectPayload(t *testing.T) {
	type state struct{}
	b := NewGraph(func(_ state, u state) state { return u })
	b.AddNode("n", func(_ context.Context, s state) (state, Directive, error) {
		return s, Effect(End(), map[string]any{"kind": "metric"}), nil
	})
	b.SetEntryPoint("n")
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	handle, err := g.NewRunner(newMemoryCP[state]()).Stream(context.Background(), "fx-1", state{})
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
	events := collectEvents(t, handle.Events(), 2*time.Second)
	if len(events) < 2 {
		t.Fatalf("expected stream events, got %d", len(events))
	}
	found := false
	for _, event := range events {
		if event.Type == EventNodeCompleted && event.Effect != nil {
			found = true
			break
		}
	}
	if !found {
		t.Fatal("expected node_completed event with effect payload")
	}
	if err := handle.Done(); err != nil {
		t.Fatalf("unexpected done error: %v", err)
	}
}

func TestNodeCompletedIncludesDuration(t *testing.T) {
	type state struct{}
	b := NewGraph(func(_ state, u state) state { return u })
	b.AddNode("n", func(_ context.Context, s state) (state, Directive, error) {
		time.Sleep(5 * time.Millisecond)
		return s, End(), nil
	})
	b.SetEntryPoint("n")
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	handle, err := g.NewRunner(newMemoryCP[state]()).Stream(context.Background(), "duration-1", state{})
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
	events := collectEvents(t, handle.Events(), 2*time.Second)
	var nodeCompleted *RunEvent[state]
	for i := range events {
		if events[i].Type == EventNodeCompleted {
			nodeCompleted = &events[i]
			break
		}
	}
	if nodeCompleted == nil {
		t.Fatal("expected node_completed event")
	}
	if nodeCompleted.Duration <= 0 {
		t.Fatalf("expected positive duration, got %s", nodeCompleted.Duration)
	}
	if err := handle.Done(); err != nil {
		t.Fatalf("unexpected done error: %v", err)
	}
}

func TestNodeCompletedIncludesMetrics(t *testing.T) {
	type state struct{}
	b := NewGraph(func(_ state, u state) state { return u })
	b.AddNode("n", func(_ context.Context, s state) (state, Directive, error) {
		return s, Effect(End(), map[string]any{"tokens": 42, "model": "mini"}), nil
	})
	b.SetEntryPoint("n")
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	handle, err := g.NewRunner(newMemoryCP[state]()).Stream(context.Background(), "metrics-1", state{})
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
	events := collectEvents(t, handle.Events(), 2*time.Second)
	found := false
	for _, event := range events {
		if event.Type == EventNodeCompleted && event.Metrics != nil {
			if got := event.Metrics["tokens"]; got != 42 {
				t.Fatalf("unexpected metrics payload: %+v", event.Metrics)
			}
			found = true
			break
		}
	}
	if !found {
		t.Fatal("expected node_completed event with metrics")
	}
	if err := handle.Done(); err != nil {
		t.Fatalf("unexpected done error: %v", err)
	}
}

func TestNodeCompletedIncludesStructMetrics(t *testing.T) {
	type usage struct {
		Tokens int    `json:"tokens"`
		Model  string `json:"model"`
	}
	type state struct{}

	b := NewGraph(func(_ state, u state) state { return u })
	b.AddNode("n", func(_ context.Context, s state) (state, Directive, error) {
		return s, Effect(End(), usage{Tokens: 128, Model: "gpt"}), nil
	})
	b.SetEntryPoint("n")
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	handle, err := g.NewRunner(newMemoryCP[state]()).Stream(context.Background(), "metrics-struct-1", state{})
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
	events := collectEvents(t, handle.Events(), 2*time.Second)
	found := false
	for _, event := range events {
		if event.Type == EventNodeCompleted && event.Metrics != nil {
			if got := event.Metrics["tokens"]; got != float64(128) {
				t.Fatalf("unexpected struct metrics payload: %+v", event.Metrics)
			}
			found = true
			break
		}
	}
	if !found {
		t.Fatal("expected node_completed event with struct metrics")
	}
	if err := handle.Done(); err != nil {
		t.Fatalf("unexpected done error: %v", err)
	}
}

func TestCompletedEventHasZeroOrCarryDurationPolicy(t *testing.T) {
	type state struct{}
	b := NewGraph(func(_ state, u state) state { return u })
	b.AddNode("n", func(_ context.Context, s state) (state, Directive, error) {
		time.Sleep(3 * time.Millisecond)
		return s, End(), nil
	})
	b.SetEntryPoint("n")
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	handle, err := g.NewRunner(newMemoryCP[state]()).Stream(context.Background(), "duration-policy", state{})
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
	events := collectEvents(t, handle.Events(), 2*time.Second)
	if len(events) == 0 {
		t.Fatal("expected stream events")
	}
	terminal := events[len(events)-1]
	if terminal.Type != EventCompleted {
		t.Fatalf("expected completed terminal event, got %s", terminal.Type)
	}
	// Policy: terminal events do not carry node duration, only node_completed does.
	if terminal.Duration != 0 {
		t.Fatalf("expected zero terminal duration, got %s", terminal.Duration)
	}
	if err := handle.Done(); err != nil {
		t.Fatalf("unexpected done error: %v", err)
	}
}

func TestStreamResume(t *testing.T) {
	type state struct{ Value int }
	cp := newMemoryCP[state]()
	b := NewGraph(func(_ state, u state) state { return u })
	b.AddNode("save", func(_ context.Context, s state) (state, Directive, error) {
		s.Value++
		return s, Suspend("hold"), nil
	})
	b.SetEntryPoint("save")
	g, _ := b.Compile()
	_, _ = g.NewRunner(cp).Start(context.Background(), "resume-1", state{})

	cp.reads["resume-1"] = Snapshot[state]{
		ThreadID: "resume-1",
		NodeID:   "done",
		Revision: cp.last.Revision,
		State:    cp.last.State,
		RunMeta:  cp.last.RunMeta,
	}

	b2 := NewGraph(func(_ state, u state) state { return u })
	b2.AddNode("done", func(_ context.Context, s state) (state, Directive, error) {
		s.Value++
		return s, End(), nil
	})
	b2.SetEntryPoint("done")
	g2, _ := b2.Compile()

	handle, err := g2.NewRunner(cp).StreamResume(context.Background(), "resume-1")
	if err != nil {
		t.Fatalf("stream resume: %v", err)
	}
	events := collectEvents(t, handle.Events(), 2*time.Second)
	if len(events) == 0 {
		t.Fatal("expected stream resume events")
	}
	if err := handle.Done(); err != nil {
		t.Fatalf("unexpected done error: %v", err)
	}
}

func TestStreamResumeMissingThread(t *testing.T) {
	type state struct{}
	g, _ := NewGraph(func(_ state, u state) state { return u }).
		AddNode("n", func(_ context.Context, s state) (state, Directive, error) {
			return s, End(), nil
		}).
		SetEntryPoint("n").
		Compile()
	_, err := g.NewRunner(newMemoryCP[state]()).StreamResume(context.Background(), "missing")
	if !errors.Is(err, ErrThreadNotFound) {
		t.Fatalf("expected ErrThreadNotFound, got %v", err)
	}
}

func TestStreamResumeRequiresCheckpointer(t *testing.T) {
	type state struct{}
	g, _ := NewGraph(func(_ state, u state) state { return u }).
		AddNode("n", func(_ context.Context, s state) (state, Directive, error) {
			return s, End(), nil
		}).
		SetEntryPoint("n").
		Compile()
	var cp Checkpointer[state]
	_, err := g.NewRunner(cp).StreamResume(context.Background(), "missing")
	if err == nil {
		t.Fatal("expected checkpointer required error")
	}
}

func TestStreamEmitsFailedEvent(t *testing.T) {
	type state struct{}
	b := NewGraph(func(_ state, u state) state { return u })
	b.AddNode("n", func(_ context.Context, s state) (state, Directive, error) {
		return s, Completed(), errors.New("boom")
	})
	b.SetEntryPoint("n")
	g, _ := b.Compile()
	handle, err := g.NewRunner(newMemoryCP[state]()).Stream(context.Background(), "fail-1", state{})
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
	events := collectEvents(t, handle.Events(), 2*time.Second)
	if len(events) == 0 || events[len(events)-1].Type != EventFailed {
		t.Fatalf("expected final failed event, got %+v", events)
	}
	if err := handle.Done(); err == nil {
		t.Fatal("expected failed done error")
	}
}

func TestStreamContextCancelSavesSnapshot(t *testing.T) {
	type state struct{ Value int }
	cp := &cancelAwareCP[state]{}
	b := NewGraph(func(_ state, u state) state { return u })
	b.AddNode("loop", func(_ context.Context, s state) (state, Directive, error) {
		s.Value++
		return s, Next("loop"), nil
	})
	b.SetEntryPoint("loop")
	g, _ := b.Compile(WithMaxSteps(100))

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	handle, err := g.NewRunner(cp).Stream(ctx, "cancel-1", state{})
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
	events := collectEvents(t, handle.Events(), 2*time.Second)
	if len(events) == 0 || events[len(events)-1].Type != EventSuspended {
		t.Fatalf("expected final suspended event, got %+v", events)
	}
	if !cp.saved {
		t.Fatal("expected snapshot to be saved on canceled context")
	}
	if err := handle.Done(); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context canceled, got %v", err)
	}
}

func TestStreamEarlyConsumerStopNoLeak(t *testing.T) {
	defer goleak.VerifyNone(t)

	type state struct{ N int }
	b := NewGraph(func(_ state, u state) state { return u })
	b.AddNode("loop", func(_ context.Context, s state) (state, Directive, error) {
		s.N++
		return s, Next("loop"), nil
	})
	b.SetEntryPoint("loop")
	g, _ := b.Compile(WithMaxSteps(10_000))

	handle, err := g.NewRunner(newMemoryCP[state]()).Stream(context.Background(), "leak-1", state{})
	if err != nil {
		t.Fatalf("stream: %v", err)
	}

	events := handle.Events()
	for range 3 {
		<-events
	}
	handle.Close()
	if err := handle.Done(); err != nil {
		t.Fatalf("unexpected done error: %v", err)
	}
	_ = collectEvents(t, events, 2*time.Second)
}

func TestStreamBufferFullThenContextCancelNoLeak(t *testing.T) {
	defer goleak.VerifyNone(t)

	type state struct{ N int }
	b := NewGraph(func(_ state, u state) state { return u })
	b.AddNode("loop", func(_ context.Context, s state) (state, Directive, error) {
		s.N++
		return s, Next("loop"), nil
	})
	b.SetEntryPoint("loop")
	g, _ := b.Compile(WithMaxSteps(100_000))

	ctx, cancel := context.WithCancel(context.Background())
	handle, err := g.NewRunner(newMemoryCP[state]()).Stream(ctx, "buffer-cancel", state{})
	if err != nil {
		t.Fatalf("stream: %v", err)
	}

	// Не читаем events вообще: producer упрется в буфер и должен корректно завершиться по ctx cancel.
	time.Sleep(25 * time.Millisecond)
	cancel()

	if err := handle.Done(); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context canceled, got %v", err)
	}
}

func TestStreamCloseWithoutDrainNoDeadlock(t *testing.T) {
	defer goleak.VerifyNone(t)

	type state struct{}
	b := NewGraph(func(_ state, u state) state { return u })
	b.AddNode("one", func(_ context.Context, s state) (state, Directive, error) {
		return s, End(), nil
	})
	b.SetEntryPoint("one")
	g, _ := b.Compile()

	handle, err := g.NewRunner(newMemoryCP[state]()).Stream(context.Background(), "close-no-drain", state{})
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
	handle.Close()
	if err := handle.Done(); err != nil {
		t.Fatalf("unexpected done error: %v", err)
	}
}

func TestStreamRecoverMiddlewareEmitsFailedEvent(t *testing.T) {
	type state struct{}
	b := NewGraph(func(_ state, u state) state { return u })
	b.Use(RecoverMiddleware[state]())
	b.AddNode("panic", func(_ context.Context, _ state) (state, Directive, error) {
		panic("stream panic")
	})
	b.SetEntryPoint("panic")
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	handle, err := g.NewRunner(newMemoryCP[state]()).Stream(context.Background(), "panic-stream", state{})
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
	events := collectEvents(t, handle.Events(), 2*time.Second)
	if len(events) == 0 || events[len(events)-1].Type != EventFailed {
		t.Fatalf("expected final failed event, got %+v", events)
	}
	if err := handle.Done(); err == nil {
		t.Fatal("expected stream failure error")
	}
}

func collectEvents[T any](t *testing.T, stream <-chan RunEvent[T], timeout time.Duration) []RunEvent[T] {
	t.Helper()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	out := make([]RunEvent[T], 0)
	for {
		select {
		case event, ok := <-stream:
			if !ok {
				return out
			}
			out = append(out, event)
		case <-timer.C:
			t.Fatalf("stream timeout after %s", timeout)
		}
	}
}

type cancelAwareCP[T any] struct {
	saved bool
}

func (c *cancelAwareCP[T]) Save(ctx context.Context, _ Snapshot[T]) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	c.saved = true
	return nil
}

func (c *cancelAwareCP[T]) Load(context.Context, string) (Snapshot[T], error) {
	return Snapshot[T]{}, ErrThreadNotFound
}

func (c *cancelAwareCP[T]) GetHistory(context.Context, string, int) ([]Snapshot[T], error) {
	return []Snapshot[T]{}, nil
}

func (c *cancelAwareCP[T]) Prune(context.Context, string, int) error {
	return nil
}
