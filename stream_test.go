package flowy

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestStreamEmitsSuspendBeforeRequestStop(t *testing.T) {
	t.Parallel()

	type state struct{ N int }
	b := NewGraph[state, NoEffect](func(_ state, u state) state { return u })
	b.AddNode("start", func(_ context.Context, s state) (state, Directive, error) {
		s.N++
		return s, Suspend("wait"), nil
	})
	b.SetEntryPoint("start")
	b.AllowNoOutgoingRoute("start")
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	handle, err := g.NewRunner(newMemoryCP[state, NoEffect]()).
		Stream(context.Background(), "suspend-1", state{})
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
	events, waitErr := CollectEventsAndWait(context.Background(), handle)
	if len(events) < 3 {
		t.Fatalf("expected at least 3 events, got %d", len(events))
	}
	if events[len(events)-1].Type != EventSuspended {
		t.Fatalf("expected final suspended event, got %s", events[len(events)-1].Type)
	}
	if waitErr != nil {
		t.Fatalf("unexpected wait error: %v", waitErr)
	}
}

func TestStreamEmitsCompletedBeforeRequestStop(t *testing.T) {
	t.Parallel()

	type state struct{ N int }
	b := NewGraph[state, NoEffect](func(_ state, u state) state { return u })
	b.AddNode("start", func(_ context.Context, s state) (state, Directive, error) {
		s.N++
		return s, End(), nil
	})
	b.SetEntryPoint("start")
	b.AllowNoOutgoingRoute("start")
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	handle, err := g.NewRunner(newMemoryCP[state, NoEffect]()).
		Stream(context.Background(), "completed-1", state{})
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
	events, waitErr := CollectEventsAndWait(context.Background(), handle)
	if len(events) == 0 || events[len(events)-1].Type != EventCompleted {
		t.Fatalf("expected final completed event, got %+v", events)
	}
	if waitErr != nil {
		t.Fatalf("unexpected wait error: %v", waitErr)
	}
}

func TestStreamEffectPayload(t *testing.T) {
	t.Parallel()

	type streamEffect struct {
		Kind string
	}
	type state struct{}
	b := NewGraph[state, streamEffect](func(_ state, u state) state { return u })
	b.AddNode("n", func(_ context.Context, s state) (state, Directive, error) {
		return s, Effect[streamEffect](End(), streamEffect{Kind: "metric"}), nil
	})
	b.SetEntryPoint("n")
	b.AllowNoOutgoingRoute("n")
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	handle, err := g.NewRunner(newMemoryCP[state, streamEffect]()).
		Stream(context.Background(), "fx-1", state{})
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
	events, waitErr := CollectEventsAndWait(context.Background(), handle)
	if len(events) < 2 {
		t.Fatalf("expected stream events, got %d", len(events))
	}
	found := false
	for _, event := range events {
		if event.Type == EventNodeCompleted && event.HasEffect {
			found = true
			break
		}
	}
	if !found {
		t.Fatal("expected node_completed event with effect payload")
	}
	if waitErr != nil {
		t.Fatalf("unexpected wait error: %v", waitErr)
	}
}

func TestNodeCompletedIncludesDuration(t *testing.T) {
	t.Parallel()

	type state struct{}
	b := NewGraph[state, NoEffect](func(_ state, u state) state { return u })
	b.AddNode("n", func(_ context.Context, s state) (state, Directive, error) {
		time.Sleep(5 * time.Millisecond)
		return s, End(), nil
	})
	b.SetEntryPoint("n")
	b.AllowNoOutgoingRoute("n")
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	handle, err := g.NewRunner(newMemoryCP[state, NoEffect]()).
		Stream(context.Background(), "duration-1", state{})
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
	events, waitErr := CollectEventsAndWait(context.Background(), handle)
	var nodeCompleted *RunEvent[state, NoEffect]
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
	if waitErr != nil {
		t.Fatalf("unexpected wait error: %v", waitErr)
	}
}

func TestNodeCompletedIncludesMetrics(t *testing.T) {
	t.Parallel()

	type usage struct {
		Tokens int
		Model  string
	}
	type state struct{}
	b := NewGraph[state, usage](func(_ state, u state) state { return u })
	b.AddNode("n", func(_ context.Context, s state) (state, Directive, error) {
		return s, Effect[usage](End(), usage{Tokens: 42, Model: "mini"}), nil
	})
	b.SetEntryPoint("n")
	b.AllowNoOutgoingRoute("n")
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	handle, err := g.NewRunner(newMemoryCP[state, usage]()).
		Stream(context.Background(), "metrics-1", state{})
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
	events, waitErr := CollectEventsAndWait(context.Background(), handle)
	found := false
	for _, event := range events {
		if event.Type == EventNodeCompleted && event.HasEffect {
			if got := event.Effect.Tokens; got != 42 {
				t.Fatalf("unexpected effect payload: %+v", event.Effect)
			}
			found = true
			break
		}
	}
	if !found {
		t.Fatal("expected node_completed event with effect payload")
	}
	if waitErr != nil {
		t.Fatalf("unexpected wait error: %v", waitErr)
	}
}

func TestNodeCompletedIncludesStructMetrics(t *testing.T) {
	t.Parallel()

	type usage struct {
		Tokens int    `json:"tokens"`
		Model  string `json:"model"`
	}
	type state struct{}

	b := NewGraph[state, usage](func(_ state, u state) state { return u })
	b.AddNode("n", func(_ context.Context, s state) (state, Directive, error) {
		return s, Effect[usage](End(), usage{Tokens: 128, Model: "gpt"}), nil
	})
	b.SetEntryPoint("n")
	b.AllowNoOutgoingRoute("n")
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	handle, err := g.NewRunner(newMemoryCP[state, usage]()).
		Stream(context.Background(), "metrics-struct-1", state{})
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
	events, waitErr := CollectEventsAndWait(context.Background(), handle)
	found := false
	for _, event := range events {
		if event.Type == EventNodeCompleted && event.HasEffect {
			if got := event.Effect.Tokens; got != 128 {
				t.Fatalf("unexpected struct effect payload: %+v", event.Effect)
			}
			found = true
			break
		}
	}
	if !found {
		t.Fatal("expected node_completed event with struct effect payload")
	}
	if waitErr != nil {
		t.Fatalf("unexpected wait error: %v", waitErr)
	}
}

func TestCompletedEventHasZeroOrCarryDurationPolicy(t *testing.T) {
	t.Parallel()

	type state struct{}
	b := NewGraph[state, NoEffect](func(_ state, u state) state { return u })
	b.AddNode("n", func(_ context.Context, s state) (state, Directive, error) {
		time.Sleep(3 * time.Millisecond)
		return s, End(), nil
	})
	b.SetEntryPoint("n")
	b.AllowNoOutgoingRoute("n")
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	handle, err := g.NewRunner(newMemoryCP[state, NoEffect]()).
		Stream(context.Background(), "duration-policy", state{})
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
	events, waitErr := CollectEventsAndWait(context.Background(), handle)
	if len(events) == 0 {
		t.Fatal("expected stream events")
	}
	terminal := events[len(events)-1]
	if terminal.Type != EventCompleted {
		t.Fatalf("expected completed terminal event, got %s", terminal.Type)
	}
	if terminal.Duration != 0 {
		t.Fatalf("expected zero terminal duration, got %s", terminal.Duration)
	}
	if waitErr != nil {
		t.Fatalf("unexpected wait error: %v", waitErr)
	}
}

func TestWithRunMetadataOnStream(t *testing.T) {
	t.Parallel()

	type state struct{ Tokens int }

	b := NewGraph[state, NoEffect](func(_ state, u state) state { return u })
	b.AddNode("n", func(ctx context.Context, s state) (state, Directive, error) {
		_ = UseBudget(ctx, "tokens", 3)
		s.Tokens = BudgetUsed(ctx, "tokens")
		return s, End(), nil
	})
	b.SetEntryPoint("n")
	b.AllowNoOutgoingRoute("n")
	g, err := b.Compile(WithNamedBudget("tokens", 10))
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	handle, err := g.NewRunner(newMemoryCP[state, NoEffect]()).Stream(
		context.Background(),
		"stream-meta-th",
		state{},
		WithRunMetadata[state, NoEffect](RunMetadataInput{
			BudgetCounts: map[string]int{"tokens": 2},
		}),
	)
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
	events, waitErr := CollectEventsAndWait(context.Background(), handle)
	if len(events) == 0 || events[len(events)-1].Type != EventCompleted {
		t.Fatalf("expected completed stream, got %+v", events)
	}
	if events[len(events)-1].State.Tokens != 5 {
		t.Fatalf("expected budget 5 (2 seed + 3 use), got %d", events[len(events)-1].State.Tokens)
	}
	if waitErr != nil {
		t.Fatalf("Wait: %v", waitErr)
	}
}

func TestResumeStream(t *testing.T) {
	t.Parallel()

	type state struct{ Value int }
	cp := newMemoryCP[state, NoEffect]()
	b := NewGraph[state, NoEffect](func(_ state, u state) state { return u })
	b.AddNode("save", func(_ context.Context, s state) (state, Directive, error) {
		s.Value++
		if s.Value < 2 {
			return s, Suspend("hold"), nil
		}
		return s, Completed(), nil
	})
	b.AddNode("done", func(_ context.Context, s state) (state, Directive, error) {
		return s, End(), nil
	})
	b.AddEdge("save", "done")
	b.AllowNoOutgoingRoute("done")
	b.SetEntryPoint("save")
	g, _ := b.Compile()
	runner := g.NewRunner(cp)
	startRes, err := runner.Start(context.Background(), "resume-1", state{})
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	requireResumeTokenMatchesSnapshot(t, startRes.ResumeToken, cp.last)

	handle, err := runner.ResumeStream(context.Background(), startRes.ResumeToken)
	if err != nil {
		t.Fatalf("stream resume: %v", err)
	}
	events, waitErr := CollectEventsAndWait(context.Background(), handle)
	if len(events) == 0 {
		t.Fatal("expected stream resume events")
	}
	requireTerminalEventReason(t, events, EventCompleted, "")
	term, _ := terminalEvent(events)
	if term.State.Value != 2 {
		t.Fatalf("expected final state value 2, got %+v", term.State)
	}
	if term.ExecutionPointer != "done" {
		t.Fatalf("expected pointer done, got %q", term.ExecutionPointer)
	}
	if waitErr != nil {
		t.Fatalf("unexpected wait error: %v", waitErr)
	}
}

func TestResumeStreamMissingThread(t *testing.T) {
	t.Parallel()

	type state struct{}
	g, _ := NewGraph[state, NoEffect](func(_ state, u state) state { return u }).
		AddNode("n", func(_ context.Context, s state) (state, Directive, error) {
			return s, End(), nil
		}).
		SetEntryPoint("n").
		AllowNoOutgoingRoute("n").
		Compile()
	_, err := g.NewRunner(newMemoryCP[state, NoEffect]()).ResumeStream(
		context.Background(),
		ResumeToken{ThreadID: "missing"},
	)
	if !errors.Is(err, ErrThreadNotFound) {
		t.Fatalf("expected ErrThreadNotFound, got %v", err)
	}
}

func TestResumeStreamRequiresCheckpointer(t *testing.T) {
	t.Parallel()

	type state struct{}
	g, _ := NewGraph[state, NoEffect](func(_ state, u state) state { return u }).
		AddNode("n", func(_ context.Context, s state) (state, Directive, error) {
			return s, End(), nil
		}).
		SetEntryPoint("n").
		AllowNoOutgoingRoute("n").
		Compile()
	var cp Checkpointer[state, NoEffect]
	_, err := g.NewRunner(cp).ResumeStream(context.Background(), ResumeToken{ThreadID: "missing"})
	if err == nil {
		t.Fatal("expected checkpointer required error")
	}
}

func TestStreamEmitsFailedEvent(t *testing.T) {
	t.Parallel()

	type state struct{}
	b := NewGraph[state, NoEffect](func(_ state, u state) state { return u })
	b.AddNode("n", func(_ context.Context, s state) (state, Directive, error) {
		return s, Completed(), errors.New("boom")
	})
	b.SetEntryPoint("n")
	b.AllowNoOutgoingRoute("n")
	g, _ := b.Compile()
	cp := newMemoryCP[state, NoEffect]()
	runner := g.NewRunner(cp)
	syncRes, syncErr := runner.Start(context.Background(), "fail-sync", state{})
	if syncErr == nil {
		t.Fatalf("expected sync error, got %+v", syncRes)
	}
	if syncRes.Reason == "" {
		t.Fatalf("expected non-empty sync reason, got %q", syncRes.Reason)
	}
	handle, err := runner.Stream(context.Background(), "fail-1", state{})
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
	events, waitErr := CollectEventsAndWait(context.Background(), handle)
	requireTerminalEventReason(t, events, EventFailed, syncRes.Reason)
	if term, ok := terminalEvent(events); !ok || term.Error == nil {
		t.Fatal("expected EventFailed error")
	}
	if waitErr == nil {
		t.Fatal("expected failed Wait error")
	}
}

func TestStreamContextCancelSavesSnapshot(t *testing.T) {
	t.Parallel()

	type state struct{ Value int }
	cp := &cancelAwareCP[state, NoEffect]{}
	b := NewGraph[state, NoEffect](func(_ state, u state) state { return u })
	b.AddNode("loop", func(_ context.Context, s state) (state, Directive, error) {
		s.Value++
		return s, Completed(), nil
	})
	b.AddEdge("loop", "loop")
	b.SetEntryPoint("loop")
	g, _ := b.Compile(WithMaxSteps(100))

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	handle, err := g.NewRunner(cp).Stream(ctx, "cancel-1", state{})
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
	events, waitErr := CollectEventsAndWait(context.Background(), handle)
	if len(events) == 0 || events[len(events)-1].Type != EventContextCanceled {
		t.Fatalf("expected final context_canceled event, got %+v", events)
	}
	if events[len(events)-1].Reason != "context_canceled" {
		t.Fatalf("expected context_canceled reason, got %q", events[len(events)-1].Reason)
	}
	if !cp.saved {
		t.Fatal("expected snapshot to be saved on canceled context")
	}
	if !errors.Is(waitErr, context.Canceled) {
		t.Fatalf("expected context canceled, got %v", waitErr)
	}
}

// TestStreamEarlyConsumerStopNoLeak exercises intentional partial-drain on the caller goroutine
// (partial-drain consumer contract): read N events, RequestStop, then Wait without background drain.
func TestStreamEarlyConsumerStopNoLeak(t *testing.T) {
	type state struct{ N int }
	b := NewGraph[state, NoEffect](func(_ state, u state) state { return u })
	b.AddNode("loop", func(_ context.Context, s state) (state, Directive, error) {
		s.N++
		return s, Completed(), nil
	})
	b.AddEdge("loop", "loop")
	b.SetEntryPoint("loop")
	g, _ := b.Compile(WithMaxSteps(10_000))

	handle, err := g.NewRunner(newMemoryCP[state, NoEffect]()).
		Stream(context.Background(), "leak-1", state{})
	if err != nil {
		t.Fatalf("stream: %v", err)
	}

	events := handle.Events()
	for range 3 {
		<-events
	}
	handle.RequestStop()
	if err := handle.Wait(); err != nil {
		t.Fatalf("unexpected wait error: %v", err)
	}
	_ = collectEvents(t, events, 2*time.Second)
}

// Intentional no-drain: goleak contract — cancel without consuming Events(); Wait() must not deadlock.
func TestStreamBufferFullThenContextCancelNoLeak(t *testing.T) {
	type state struct{ N int }
	b := NewGraph[state, NoEffect](func(_ state, u state) state { return u })
	b.AddNode("loop", func(_ context.Context, s state) (state, Directive, error) {
		s.N++
		return s, Completed(), nil
	})
	b.AddEdge("loop", "loop")
	b.SetEntryPoint("loop")
	g, _ := b.Compile(WithMaxSteps(100_000))

	ctx, cancel := context.WithCancel(context.Background())
	handle, err := g.NewRunner(newMemoryCP[state, NoEffect]()).Stream(ctx, "buffer-cancel", state{})
	if err != nil {
		t.Fatalf("stream: %v", err)
	}

	waitForStreamBufferBackpressure()
	cancel()

	if err := handle.Wait(); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context canceled, got %v", err)
	}
}

func TestStreamHandoffPersistVsEventDroppedTerminalEventWhenRequestStop(t *testing.T) {
	t.Parallel()

	type state struct{ N int }
	ready := make(chan struct{})
	b := NewGraph[state, NoEffect](func(_ state, u state) state { return u })
	b.AddNode("work", func(ctx context.Context, s state) (state, Directive, error) {
		s.N++
		close(ready)
		<-ctx.Done()
		return s, Completed(), nil
	})
	b.AllowNoOutgoingRoute("work")
	b.SetEntryPoint("work")
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	cp := newMemoryCP[state, NoEffect]()
	runner := g.NewRunner(cp)
	handle, err := runner.Stream(context.Background(), "stream-handoff-closed-th", state{})
	if err != nil {
		t.Fatalf("stream: %v", err)
	}

	outCh := BeginStreamCollect(handle)
	<-ready
	handle.RequestStop()
	_, waitErr := awaitStreamCollect(t, handle, outCh, 5*time.Second)
	if waitErr != nil {
		t.Fatalf("Wait after RequestStop: %v", waitErr)
	}
	snap := requireSnapshotPresent(t, cp, "stream-handoff-closed-th")
	if snap.ExecutionPointer != "work" {
		t.Fatalf("expected pointer work, got %q", snap.ExecutionPointer)
	}
	if snap.RunMeta.Segment.EndReason != SegmentEndContextCanceled {
		t.Fatalf("expected context_canceled segment, got %q", snap.RunMeta.Segment.EndReason)
	}
	handoffErr := runner.RequestLocalHandoff(context.Background(), "stream-handoff-closed-th")
	if !errors.Is(handoffErr, ErrNoActiveExecution) {
		t.Fatalf(
			"expected ErrNoActiveExecution after RequestStop terminated run, got %v",
			handoffErr,
		)
	}
}

func TestWithRunMetadataOnResumeStream(t *testing.T) {
	t.Parallel()

	type state struct{ Tokens int }

	b := NewGraph[state, NoEffect](func(_ state, u state) state { return u })
	b.AddNode("save", func(_ context.Context, s state) (state, Directive, error) {
		if s.Tokens == 0 {
			s.Tokens = 1
			return s, Suspend("hold"), nil
		}
		return s, Completed(), nil
	})
	b.AddNode("done", func(ctx context.Context, s state) (state, Directive, error) {
		_ = UseBudget(ctx, "tokens", 4)
		s.Tokens = BudgetUsed(ctx, "tokens")
		return s, End(), nil
	})
	b.AddEdge("save", "done")
	b.AllowNoOutgoingRoute("done")
	b.SetEntryPoint("save")
	g, err := b.Compile(WithNamedBudget("tokens", 10))
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	cp := newMemoryCP[state, NoEffect]()
	runner := g.NewRunner(cp)
	startRes, err := runner.Start(context.Background(), "stream-resume-meta-th", state{})
	if err != nil {
		t.Fatalf("start: %v", err)
	}

	handle, err := runner.ResumeStream(
		context.Background(),
		startRes.ResumeToken,
		WithRunMetadata[state, NoEffect](RunMetadataInput{
			BudgetCounts: map[string]int{"tokens": 3},
		}),
	)
	if err != nil {
		t.Fatalf("stream resume: %v", err)
	}
	events, waitErr := CollectEventsAndWait(context.Background(), handle)
	if len(events) == 0 || events[len(events)-1].Type != EventCompleted {
		t.Fatalf("expected completed stream, got %+v", events)
	}
	if events[len(events)-1].State.Tokens != 7 {
		t.Fatalf("expected budget 7 (3 seed + 4 use), got %d", events[len(events)-1].State.Tokens)
	}
	if waitErr != nil {
		t.Fatalf("Wait: %v", waitErr)
	}
}

func TestStreamRequestLocalHandoffEmitsHandoffEvent(t *testing.T) {
	t.Parallel()

	type state struct{ N int }
	ready := make(chan struct{})
	b := NewGraph[state, NoEffect](func(_ state, u state) state { return u })
	b.AddNode("work", func(ctx context.Context, s state) (state, Directive, error) {
		s.N++
		close(ready)
		<-ctx.Done()
		return s, Completed(), nil
	})
	b.AllowNoOutgoingRoute("work")
	b.SetEntryPoint("work")
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	cp := newMemoryCP[state, NoEffect]()
	runner := g.NewRunner(cp)
	handle, err := runner.Stream(context.Background(), "stream-handoff-th", state{})
	if err != nil {
		t.Fatalf("stream: %v", err)
	}

	outCh := BeginStreamCollect(handle)
	<-ready
	if err := runner.RequestLocalHandoff(context.Background(), "stream-handoff-th"); err != nil {
		t.Fatalf("handoff: %v", err)
	}
	events, waitErr := awaitStreamCollect(t, handle, outCh, 5*time.Second)
	if waitErr != nil {
		t.Fatalf("Wait: %v", waitErr)
	}
	if len(events) == 0 {
		t.Fatal("expected events")
	}
	last := events[len(events)-1]
	if last.Type != EventHandoff {
		t.Fatalf("expected handoff event, got %s", last.Type)
	}
	snap, _, loadErr := cp.Load(context.Background(), "stream-handoff-th")
	if loadErr != nil {
		t.Fatalf("expected snapshot after stream handoff, got %v", loadErr)
	}
	if snap.Revision <= 0 {
		t.Fatalf("expected positive snapshot revision, got %d", snap.Revision)
	}
	if last.Reason != "background_handoff" {
		t.Fatalf("expected background_handoff reason on event, got %q", last.Reason)
	}
	if snap.RunMeta.Segment.EndReason != SegmentEndHandoff {
		t.Fatalf("expected handoff segment, got %q", snap.RunMeta.Segment.EndReason)
	}
}

func TestStreamRequestLocalHandoffAfterWaitReturnsNoActiveExecution(t *testing.T) {
	t.Parallel()

	type state struct{}

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

	runner := g.NewRunner(newMemoryCP[state, NoEffect]())
	handle, err := runner.Stream(context.Background(), "stream-htb-after-done-th", state{})
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
	events, waitErr := CollectEventsAndWait(context.Background(), handle)
	if waitErr != nil {
		t.Fatalf("Wait: %v", waitErr)
	}
	if len(events) == 0 || events[len(events)-1].Type != EventCompleted {
		t.Fatalf("expected completed stream, got %+v", events)
	}
	if err := runner.RequestLocalHandoff(
		context.Background(),
		"stream-htb-after-done-th",
	); !errors.Is(
		err,
		ErrNoActiveExecution,
	) {
		t.Fatalf("expected ErrNoActiveExecution after Wait, got %v", err)
	}
}

func TestStreamRequestLocalHandoffRetentionFailAfterPersist(t *testing.T) {
	t.Parallel()

	type state struct{ N int }
	ready := make(chan struct{})
	cp := &failingMemoryCP[state, NoEffect]{
		memoryCP:  newMemoryCP[state, NoEffect](),
		failPrune: true,
	}
	b := NewGraph[state, NoEffect](func(_ state, u state) state { return u })
	b.AddNode("work", func(ctx context.Context, s state) (state, Directive, error) {
		s.N++
		close(ready)
		<-ctx.Done()
		return s, Completed(), nil
	})
	b.AllowNoOutgoingRoute("work")
	b.SetEntryPoint("work")
	g, err := b.Compile(WithRetentionLimit(2))
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	runner := g.NewRunner(cp)
	handle, err := runner.Stream(context.Background(), "stream-htb-retention-th", state{})
	if err != nil {
		t.Fatalf("stream: %v", err)
	}

	out := BeginStreamCollect(handle)
	<-ready
	handoffErr := runner.RequestLocalHandoff(context.Background(), "stream-htb-retention-th")
	if handoffErr == nil {
		t.Fatal("expected retention error on stream handoff")
	}
	if !strings.Contains(handoffErr.Error(), "retention") {
		t.Fatalf("expected retention in error, got %v", handoffErr)
	}
	events, waitErr := awaitStreamCollect(t, handle, out, 5*time.Second)
	if waitErr == nil {
		t.Fatal("expected retention error from Wait")
	}
	if _, _, loadErr := cp.Load(context.Background(), "stream-htb-retention-th"); loadErr != nil {
		t.Fatalf("snapshot must exist despite retention failure: %v", loadErr)
	}

	syncReady := make(chan struct{})
	syncB := NewGraph[state, NoEffect](func(_ state, u state) state { return u })
	syncB.AddNode("work", func(ctx context.Context, s state) (state, Directive, error) {
		close(syncReady)
		<-ctx.Done()
		return s, Completed(), nil
	})
	syncB.AllowNoOutgoingRoute("work")
	syncB.SetEntryPoint("work")
	syncG, syncCompileErr := syncB.Compile(WithRetentionLimit(2))
	if syncCompileErr != nil {
		t.Fatalf("compile sync graph: %v", syncCompileErr)
	}
	syncCP := &failingMemoryCP[state, NoEffect]{
		memoryCP:  newMemoryCP[state, NoEffect](),
		failPrune: true,
	}
	syncRunner := syncG.NewRunner(syncCP)
	syncDone := make(chan *RunResult[state, NoEffect], 1)
	go func() {
		res, runErr := syncRunner.Start(context.Background(), "stream-htb-retention-sync-th", state{})
		if runErr == nil {
			t.Errorf("expected sync retention error")
		}
		syncDone <- res
	}()
	<-syncReady
	syncHTBErr := syncRunner.RequestLocalHandoff(context.Background(), "stream-htb-retention-sync-th")
	if syncHTBErr == nil || !strings.Contains(syncHTBErr.Error(), "retention") {
		t.Fatalf("expected sync retention error, got %v", syncHTBErr)
	}
	syncRes := <-syncDone
	wantReason := retentionFailedReason("background_handoff")
	foundHandoff := false
	for _, ev := range events {
		if ev.Type == EventHandoff {
			foundHandoff = true
			if ev.Reason != wantReason {
				t.Fatalf("expected handoff reason %q, got %q", wantReason, ev.Reason)
			}
		}
	}
	if !foundHandoff {
		t.Fatalf("expected EventHandoff on retention fail, events=%+v", events)
	}
	if syncRes != nil && syncRes.Reason != wantReason {
		t.Fatalf("expected sync reason %q, got %q", wantReason, syncRes.Reason)
	}
	assertTerminalEventReasonMatchesSync(t, events, EventHandoff, wantReason)
	assertRunMetaHandoffStatusMatchesSnapshot(t, syncRes, syncCP, "stream-htb-retention-sync-th")
}

func TestStreamRequestLocalHandoffSaveHardFail(t *testing.T) {
	t.Parallel()

	type state struct{ N int }
	ready := make(chan struct{})
	b := NewGraph[state, NoEffect](func(_ state, u state) state { return u })
	b.AddNode("work", func(ctx context.Context, s state) (state, Directive, error) {
		s.N++
		close(ready)
		<-ctx.Done()
		return s, Completed(), nil
	})
	b.AllowNoOutgoingRoute("work")
	b.SetEntryPoint("work")
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	cp := &failingMemoryCP[state, NoEffect]{failSave: true}
	runner := g.NewRunner(cp)
	handle, err := runner.Stream(context.Background(), "stream-htb-savefail-th", state{})
	if err != nil {
		t.Fatalf("stream: %v", err)
	}

	out := BeginStreamCollect(handle)
	<-ready
	handoffErr := runner.RequestLocalHandoff(context.Background(), "stream-htb-savefail-th")
	if handoffErr == nil {
		t.Fatal("expected handoff save error")
	}
	if errors.Is(handoffErr, ErrCheckpointSkipped) {
		t.Fatalf("hard fail must not return ErrCheckpointSkipped, got %v", handoffErr)
	}
	if !strings.Contains(handoffErr.Error(), "save failed") {
		t.Fatalf("expected save failed in error, got %v", handoffErr)
	}
	events, doneErr := awaitStreamCollect(t, handle, out, 5*time.Second)
	if doneErr == nil {
		t.Fatal("expected save error from Wait")
	}
	var failed *RunEvent[state, NoEffect]
	for i := range events {
		if events[i].Type == EventFailed {
			failed = &events[i]
			break
		}
	}
	if failed == nil {
		t.Fatalf("expected EventFailed on stream HTB save hard fail, got %+v", events)
	}
	if failed.Reason != ReasonHandoffSaveFailed {
		t.Fatalf("EventFailed reason: want %q, got %q", ReasonHandoffSaveFailed, failed.Reason)
	}

	syncReady := make(chan struct{})
	syncB := NewGraph[state, NoEffect](func(_ state, u state) state { return u })
	syncB.AddNode("work", func(ctx context.Context, s state) (state, Directive, error) {
		s.N++
		close(syncReady)
		<-ctx.Done()
		return s, Completed(), nil
	})
	syncB.AllowNoOutgoingRoute("work")
	syncB.SetEntryPoint("work")
	syncG, err := syncB.Compile()
	if err != nil {
		t.Fatalf("sync compile: %v", err)
	}
	syncRunner := syncG.NewRunner(cp)
	syncDone := make(chan struct {
		res *RunResult[state, NoEffect]
		err error
	}, 1)
	go func() {
		res, runErr := syncRunner.Start(
			context.Background(),
			"stream-htb-savefail-sync-th",
			state{},
		)
		syncDone <- struct {
			res *RunResult[state, NoEffect]
			err error
		}{res, runErr}
	}()
	<-syncReady
	syncHandoffErr := syncRunner.RequestLocalHandoff(
		context.Background(),
		"stream-htb-savefail-sync-th",
	)
	if syncHandoffErr == nil {
		t.Fatal("expected sync handoff save error")
	}
	if !strings.Contains(syncHandoffErr.Error(), "save failed") {
		t.Fatalf("expected save failed in sync handoff error, got %v", syncHandoffErr)
	}
	syncOutcome := <-syncDone
	if syncOutcome.err == nil {
		t.Fatalf("expected sync execute save failure, got %+v", syncOutcome.res)
	}
	if syncOutcome.res == nil || syncOutcome.res.Reason != ReasonHandoffSaveFailed {
		t.Fatalf("sync reason: want %q, got res=%+v", ReasonHandoffSaveFailed, syncOutcome.res)
	}
	if failed.Reason != syncOutcome.res.Reason {
		t.Fatalf(
			"stream/sync reason mismatch: stream %q sync %q",
			failed.Reason,
			syncOutcome.res.Reason,
		)
	}
}

func assertStreamHandoffDuringLeaseLossRace[T, E any](
	t *testing.T,
	handoffErr, waitErr error,
	events []RunEvent[T, E],
) {
	t.Helper()
	if handoffErr == nil {
		if waitErr != nil {
			t.Fatalf("expected nil Wait after successful handoff, got %v", waitErr)
		}
		return
	}
	if errors.Is(handoffErr, ErrNoActiveExecution) {
		t.Fatalf(
			"handoff must not return ErrNoActiveExecution while session may still be active, got %v",
			handoffErr,
		)
	}
	if !errors.Is(handoffErr, ErrLeaseLost) {
		t.Fatalf("expected nil or ErrLeaseLost on handoff, got %v", handoffErr)
	}
	if !errors.Is(waitErr, ErrLeaseLost) {
		t.Fatalf("stream Wait: want ErrLeaseLost, got %v", waitErr)
	}
	requireTerminalEventReason(t, events, EventFailed, ErrLeaseLost.Error())
}

func TestStreamRequestLocalHandoffLeaseLost(t *testing.T) {
	t.Parallel()

	type state struct{ N int }

	leaseOpts := []RunOption[state, NoEffect]{
		WithRunLease[state, NoEffect]("worker-a", 50*time.Millisecond),
	}

	t.Run("lease_lost_before_handoff", func(t *testing.T) {
		t.Parallel()
		// RequestLocalHandoff before forceLeaseTakeover is intentional: deterministic ordering vs concurrent race.

		lease := NewMemoryLeaseManager()
		g, ready := blockingHandoffWorkGraph[state, NoEffect](t)
		runner := g.NewRunnerWithOptions(
			newMemoryCP[state, NoEffect](),
			[]RunnerOption[state, NoEffect]{
				WithLeaseManager[state, NoEffect](lease),
			},
		)

		handle, err := runner.Stream(
			context.Background(),
			"stream-htb-lease-before-th",
			state{},
			leaseOpts...)
		if err != nil {
			t.Fatalf("stream: %v", err)
		}
		out := BeginStreamCollect(handle)
		<-ready
		handoffErr := runner.RequestLocalHandoff(context.Background(), "stream-htb-lease-before-th")
		if errors.Is(handoffErr, ErrNoActiveExecution) {
			t.Fatalf(
				"handoff must not return ErrNoActiveExecution while session is active, got %v",
				handoffErr,
			)
		}
		forceLeaseTakeover(t, lease, "stream-htb-lease-before-th")
		waitForLeaseTTLExpiry()

		events, waitErr := awaitStreamCollect(t, handle, out, 5*time.Second)
		assertStreamHandoffDuringLeaseLossRace(t, handoffErr, waitErr, events)
	})

	t.Run("session_closed_after_lease_lost", func(t *testing.T) {
		t.Parallel()
		// Stream Wait may be context.Canceled when RequestStop/consumer teardown races lease loss.

		lease := NewMemoryLeaseManager()
		g, ready := blockingHandoffWorkGraph[state, NoEffect](t)
		runner := g.NewRunnerWithOptions(
			newMemoryCP[state, NoEffect](),
			[]RunnerOption[state, NoEffect]{
				WithLeaseManager[state, NoEffect](lease),
			},
		)

		handle, err := runner.Stream(
			context.Background(),
			"stream-htb-lease-after-th",
			state{},
			leaseOpts...)
		if err != nil {
			t.Fatalf("stream: %v", err)
		}
		out := BeginStreamCollect(handle)
		<-ready
		forceLeaseTakeover(t, lease, "stream-htb-lease-after-th")
		waitForLeaseTTLExpiry()

		_, waitErr := awaitStreamCollect(t, handle, out, 5*time.Second)
		if waitErr != nil && !errors.Is(waitErr, ErrLeaseLost) &&
			!errors.Is(waitErr, context.Canceled) {
			t.Fatalf(
				"stream Wait: want ErrLeaseLost or context.Canceled after lease loss, got %v",
				waitErr,
			)
		}

		handoffErr := runner.RequestLocalHandoff(context.Background(), "stream-htb-lease-after-th")
		if !errors.Is(handoffErr, ErrNoActiveExecution) {
			t.Fatalf("expected ErrNoActiveExecution after session closed, got %v", handoffErr)
		}
	})
}

func TestStreamRequestLocalHandoffEnqueueAndRetentionJoin(t *testing.T) {
	t.Parallel()

	type state struct{ N int }
	ready := make(chan struct{})
	outbox := &stubHandoffOutbox{err: errors.New("broker down")}
	cp := &failingMemoryCP[state, NoEffect]{
		memoryCP:  newMemoryCP[state, NoEffect](),
		failPrune: true,
	}
	b := NewGraph[state, NoEffect](func(_ state, u state) state { return u })
	b.AddNode("work", func(ctx context.Context, s state) (state, Directive, error) {
		s.N++
		close(ready)
		<-ctx.Done()
		return s, Completed(), nil
	})
	b.AllowNoOutgoingRoute("work")
	b.SetEntryPoint("work")
	g, err := b.Compile(WithRetentionLimit(2))
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	runner := g.NewRunner(cp)
	handle, err := runner.Stream(context.Background(), "stream-htb-enqueue-retention-th", state{},
		WithHandoffOutbox[state, NoEffect](outbox),
	)
	if err != nil {
		t.Fatalf("stream: %v", err)
	}

	out := BeginStreamCollect(handle)
	<-ready
	handoffErr := runner.RequestLocalHandoff(context.Background(), "stream-htb-enqueue-retention-th")
	if !errors.Is(handoffErr, ErrHandoffEnqueueFailed) {
		t.Fatalf("expected enqueue failure, got %v", handoffErr)
	}
	if !strings.Contains(handoffErr.Error(), "retention") {
		t.Fatalf("expected retention error in join chain, got %v", handoffErr)
	}
	events, waitErr := awaitStreamCollect(t, handle, out, 5*time.Second)
	if !errors.Is(waitErr, ErrHandoffEnqueueFailed) {
		t.Fatalf("expected enqueue failure from Wait, got %v", waitErr)
	}
	wantReason := retentionFailedReason(ReasonHandoffOrphaned)
	foundHandoff := false
	for _, ev := range events {
		if ev.Type == EventHandoff {
			foundHandoff = true
			if ev.Reason != wantReason {
				t.Fatalf("expected handoff reason %q, got %q", wantReason, ev.Reason)
			}
		}
	}
	if !foundHandoff {
		t.Fatalf("expected EventHandoff, events=%+v", events)
	}

	syncReady := make(chan struct{})
	syncB := NewGraph[state, NoEffect](func(_ state, u state) state { return u })
	syncB.AddNode("work", func(ctx context.Context, s state) (state, Directive, error) {
		close(syncReady)
		<-ctx.Done()
		return s, Completed(), nil
	})
	syncB.AllowNoOutgoingRoute("work")
	syncB.SetEntryPoint("work")
	syncG, syncCompileErr := syncB.Compile(WithRetentionLimit(2))
	if syncCompileErr != nil {
		t.Fatalf("compile sync graph: %v", syncCompileErr)
	}
	syncCP := &failingMemoryCP[state, NoEffect]{
		memoryCP:  newMemoryCP[state, NoEffect](),
		failPrune: true,
	}
	syncRunner := syncG.NewRunner(syncCP)
	syncDone := make(chan *RunResult[state, NoEffect], 1)
	go func() {
		res, _ := syncRunner.Start(context.Background(), "stream-htb-enqueue-retention-sync-th", state{},
			WithHandoffOutbox[state, NoEffect](outbox),
		)
		syncDone <- res
	}()
	<-syncReady
	syncHTBErr := syncRunner.RequestLocalHandoff(context.Background(), "stream-htb-enqueue-retention-sync-th")
	if !errors.Is(syncHTBErr, ErrHandoffEnqueueFailed) {
		t.Fatalf("expected sync enqueue failure, got %v", syncHTBErr)
	}
	syncRes := <-syncDone
	if syncRes == nil || syncRes.Reason != wantReason {
		t.Fatalf("expected sync reason %q, got %+v", wantReason, syncRes)
	}
	assertTerminalEventReasonMatchesSync(t, events, EventHandoff, syncRes.Reason)
	assertOrphanedHandoffSnapshot(t, cp, "stream-htb-enqueue-retention-th", nil, "")
	assertRunMetaHandoffStatusMatchesSnapshot(t, syncRes, syncCP, "stream-htb-enqueue-retention-sync-th")
}

func TestStreamRequestStopPersistsSnapshot(t *testing.T) {
	t.Parallel()

	type state struct{ N int }
	cp := newMemoryCP[state, NoEffect]()
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

	handle, err := g.NewRunner(cp).Stream(context.Background(), "close-persist-th", state{})
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
	var pre []RunEvent[state, NoEffect]
	var count int
	waitErr := ConsumeEventsAndWait(
		context.Background(),
		handle,
		func(ev RunEvent[state, NoEffect]) bool {
			pre = append(pre, ev)
			count++
			return count < 2
		},
	)
	if waitErr != nil {
		t.Fatalf("unexpected wait error after RequestStop persist: %v", waitErr)
	}
	snap, _, loadErr := cp.Load(context.Background(), "close-persist-th")
	if loadErr != nil {
		t.Fatalf("expected snapshot after RequestStop, got %v", loadErr)
	}
	for i := range pre {
		if pre[i].Type != EventContextCanceled {
			continue
		}
		if pre[i].ExecutionPointer != snap.ExecutionPointer {
			t.Fatalf(
				"event pointer %q != snapshot pointer %q",
				pre[i].ExecutionPointer,
				snap.ExecutionPointer,
			)
		}
	}
}

func TestStreamRequestStopEmitsContextCanceled(t *testing.T) {
	t.Parallel()

	type state struct{ N int }
	cp := newMemoryCP[state, NoEffect]()
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

	handle, err := g.NewRunner(cp).Stream(context.Background(), "close-event-th", state{})
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
	var pre []RunEvent[state, NoEffect]
	var count int
	waitErr := ConsumeEventsAndWait(
		context.Background(),
		handle,
		func(ev RunEvent[state, NoEffect]) bool {
			pre = append(pre, ev)
			count++
			return count < 2
		},
	)
	if waitErr != nil {
		t.Fatalf("unexpected wait error after RequestStop: %v", waitErr)
	}
	if _, _, loadErr := cp.Load(context.Background(), "close-event-th"); loadErr != nil {
		t.Fatalf("expected persisted snapshot on RequestStop, got %v", loadErr)
	}
	for i := range pre {
		if pre[i].Type != EventContextCanceled {
			continue
		}
		if pre[i].Reason != "context_canceled" {
			t.Fatalf("expected context_canceled reason, got %q", pre[i].Reason)
		}
	}
}

func TestStreamBufferFullCancelPersistsSnapshot(t *testing.T) {
	type state struct{ N int }
	cp := newMemoryCP[state, NoEffect]()
	b := NewGraph[state, NoEffect](func(_ state, u state) state { return u })
	b.AddNode("loop", func(_ context.Context, s state) (state, Directive, error) {
		s.N++
		return s, Completed(), nil
	})
	b.AddEdge("loop", "loop")
	b.SetEntryPoint("loop")
	g, _ := b.Compile(WithMaxSteps(100_000))

	ctx, cancel := context.WithCancel(context.Background())
	handle, err := g.NewRunner(cp).Stream(ctx, "buffer-persist-th", state{})
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
	out := BeginStreamCollect(handle)
	waitForStreamBufferBackpressure()
	cancel()
	// Stream ctx is already canceled; use Background for await to avoid select race on done ctx.
	events, waitErr := awaitStreamCollect(t, handle, out, 5*time.Second)
	if !errors.Is(waitErr, context.Canceled) {
		t.Fatalf("expected context canceled, got %v", waitErr)
	}
	if _, _, loadErr := cp.Load(context.Background(), "buffer-persist-th"); loadErr != nil {
		t.Fatalf("expected snapshot on buffer-full cancel, got %v", loadErr)
	}
	var terminal *RunEvent[state, NoEffect]
	for i := range events {
		if events[i].Type == EventContextCanceled {
			terminal = &events[i]
		}
	}
	if terminal == nil {
		t.Fatalf("expected EventContextCanceled, got %+v", events)
	}
	if terminal.Reason != "context_canceled" {
		t.Fatalf("expected context_canceled reason, got %q", terminal.Reason)
	}
}

func TestStreamClosePersistVsEventDroppedTerminalEventSkipOnSaveError(t *testing.T) {
	t.Parallel()

	type state struct{ N int }
	cp := &failingMemoryCP[state, NoEffect]{failSave: true}
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

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	handle, err := g.NewRunner(cp).Stream(ctx, "close-skip-on-save-th", state{},
		WithCheckpointErrorPolicy[state, NoEffect](CheckpointPolicySkipOnSaveError),
	)
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
	var pre []RunEvent[state, NoEffect]
	waitErr := ConsumeEventsAndWait(
		context.Background(),
		handle,
		func(ev RunEvent[state, NoEffect]) bool {
			pre = append(pre, ev)
			return len(pre) < 2
		},
	)
	if waitErr != nil && !errors.Is(waitErr, ErrCheckpointSkipped) {
		t.Fatalf("Wait: got %v want nil or ErrCheckpointSkipped", waitErr)
	}
	requireSnapshotMissing(t, cp, "close-skip-on-save-th")
	for _, ev := range pre {
		if ev.Type == EventContextCanceled {
			if ev.Reason != ReasonContextCanceledCheckpointSkipped {
				t.Fatalf(
					"expected reason %q on close soft warn, got %q",
					ReasonContextCanceledCheckpointSkipped, ev.Reason,
				)
			}
			return
		}
	}
	if hasStreamEventType(pre, EventCheckpointFailed) {
		return
	}
	t.Log(
		"persist-vs-event: terminal stream event dropped; Wait and snapshot absence are authoritative",
	)
}

func TestResumeStreamClosePersistVsEventDroppedTerminalEventSkipOnSaveError(t *testing.T) {
	t.Parallel()

	type state struct{ N int }
	cp := newMemoryCP[state, NoEffect]()
	b := NewGraph[state, NoEffect](func(_ state, u state) state { return u })
	b.AddNode("hold", func(_ context.Context, s state) (state, Directive, error) {
		if s.N == 0 {
			s.N = 1
			return s, Suspend("wait"), nil
		}
		return s, Completed(), nil
	})
	b.AddNode("loop", func(_ context.Context, s state) (state, Directive, error) {
		s.N++
		return s, Completed(), nil
	})
	b.AddEdge("hold", "loop")
	b.AddEdge("loop", "loop")
	b.SetEntryPoint("hold")
	g, err := b.Compile(WithMaxSteps(100))
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	runner := g.NewRunner(cp)
	startRes, err := runner.Start(context.Background(), "resume-close-skip-on-save-th", state{})
	if err != nil {
		t.Fatalf("start: %v", err)
	}

	failCP := &saveFailOverlayCP[state, NoEffect]{inner: cp}
	streamCtx, streamCancel := context.WithCancel(t.Context())
	defer streamCancel()
	handle, err := g.NewRunner(failCP).ResumeStream(streamCtx, startRes.ResumeToken,
		WithCheckpointErrorPolicy[state, NoEffect](CheckpointPolicySkipOnSaveError),
	)
	if err != nil {
		t.Fatalf("stream resume: %v", err)
	}
	var pre []RunEvent[state, NoEffect]
	waitErr := ConsumeEventsAndWait(
		context.Background(),
		handle,
		func(ev RunEvent[state, NoEffect]) bool {
			pre = append(pre, ev)
			return len(pre) < 2
		},
	)
	if waitErr != nil && !errors.Is(waitErr, ErrCheckpointSkipped) &&
		!errors.Is(waitErr, context.Canceled) {
		t.Fatalf("Wait: got %v want nil, ErrCheckpointSkipped, or context.Canceled", waitErr)
	}
	for _, ev := range pre {
		if ev.Type == EventContextCanceled {
			if ev.Reason != ReasonContextCanceledCheckpointSkipped {
				t.Fatalf(
					"expected reason %q on resume close soft warn, got %q",
					ReasonContextCanceledCheckpointSkipped, ev.Reason,
				)
			}
			return
		}
	}
	if hasStreamEventType(pre, EventCheckpointFailed) {
		return
	}
	t.Log("persist-vs-event: terminal stream event dropped; Wait is authoritative")
}

func TestStreamClosePersistVsEventDroppedTerminalEventRetentionFailed(t *testing.T) {
	t.Parallel()

	type state struct{ N int }
	cp := &failingMemoryCP[state, NoEffect]{
		memoryCP:  newMemoryCP[state, NoEffect](),
		failPrune: true,
	}
	b := NewGraph[state, NoEffect](func(_ state, u state) state { return u })
	b.AddNode("loop", func(_ context.Context, s state) (state, Directive, error) {
		s.N++
		return s, Completed(), nil
	})
	b.AddEdge("loop", "loop")
	b.SetEntryPoint("loop")
	g, err := b.Compile(WithMaxSteps(100), WithRetentionLimit(2))
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	handle, err := g.NewRunner(cp).Stream(ctx, "close-retention-th", state{})
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
	var pre []RunEvent[state, NoEffect]
	waitErr := ConsumeEventsAndWait(
		context.Background(),
		handle,
		func(ev RunEvent[state, NoEffect]) bool {
			pre = append(pre, ev)
			return len(pre) < 2
		},
	)
	if waitErr == nil {
		t.Fatal("Wait: got nil want error containing \"retention\"")
	}
	if !strings.Contains(waitErr.Error(), "retention") {
		t.Fatalf("Wait: got %v want error containing \"retention\"", waitErr)
	}
	requireSnapshotPresent(t, cp, "close-retention-th")
	for _, ev := range pre {
		if ev.Type == EventContextCanceled {
			if !strings.Contains(ev.Reason, "retention_failed") {
				t.Fatalf("expected retention_failed suffix in event reason, got %q", ev.Reason)
			}
			return
		}
	}
	t.Log("persist-vs-event: terminal stream event dropped; Wait and snapshot are authoritative")
}

func TestStreamClosePersistVsEventDroppedTerminalEventHardFailSave(t *testing.T) {
	t.Parallel()

	type state struct{ N int }
	cp := &failingMemoryCP[state, NoEffect]{failSave: true}
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

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	handle, err := g.NewRunner(cp).Stream(ctx, "close-savefail-th", state{})
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
	var pre []RunEvent[state, NoEffect]
	waitErr := ConsumeEventsAndWait(
		context.Background(),
		handle,
		func(ev RunEvent[state, NoEffect]) bool {
			pre = append(pre, ev)
			return len(pre) < 2
		},
	)
	if waitErr == nil {
		t.Fatal("Wait: got nil want error containing \"save failed\"")
	}
	if !strings.Contains(waitErr.Error(), "save failed") {
		t.Fatalf("Wait: got %v want error containing \"save failed\"", waitErr)
	}
	for _, ev := range pre {
		if ev.Type == EventContextCanceled {
			if !strings.Contains(ev.Reason, "save_failed") {
				t.Fatalf("expected save_failed suffix in event reason, got %q", ev.Reason)
			}
			return
		}
	}
	t.Log("persist-vs-event: terminal stream event dropped; Wait is authoritative")
}

func TestStreamRequestStopBeforeHandoffTerminalPersists(t *testing.T) {
	t.Parallel()

	type state struct{ N int }
	ready := make(chan struct{})
	b := NewGraph[state, NoEffect](func(_ state, u state) state { return u })
	b.AddNode("work", func(ctx context.Context, s state) (state, Directive, error) {
		s.N++
		close(ready)
		<-ctx.Done()
		return s, Completed(), nil
	})
	b.AllowNoOutgoingRoute("work")
	b.SetEntryPoint("work")
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	cp := newMemoryCP[state, NoEffect]()
	runner := g.NewRunner(cp)
	handle, err := runner.Stream(context.Background(), "close-handoff-bd-th", state{})
	if err != nil {
		t.Fatalf("stream: %v", err)
	}

	outCh := BeginStreamCollect(handle)
	<-ready
	handle.RequestStop()
	_, waitErr := awaitStreamCollect(t, handle, outCh, 5*time.Second)
	if waitErr != nil {
		t.Fatalf("Wait after RequestStop: %v", waitErr)
	}
	snap := requireSnapshotPresent(t, cp, "close-handoff-bd-th")
	if snap.State.N != 1 {
		t.Fatalf("expected N=1 in snapshot, got %+v", snap.State)
	}
	if snap.RunMeta.Segment.EndReason != SegmentEndContextCanceled {
		t.Fatalf("expected context_canceled segment, got %q", snap.RunMeta.Segment.EndReason)
	}
	handoffErr := runner.RequestLocalHandoff(context.Background(), "close-handoff-bd-th")
	if !errors.Is(handoffErr, ErrNoActiveExecution) {
		t.Fatalf("expected ErrNoActiveExecution after RequestStop, got %v", handoffErr)
	}
}

// Intentional no-drain: persist-vs-event — terminal event may be dropped after RequestStop without drain.
func TestStreamRequestStopBeforeCompletedTerminal(t *testing.T) {
	t.Parallel()

	type state struct{ N int }
	b := NewGraph[state, NoEffect](func(_ state, u state) state { return u })
	b.AddNode("once", func(_ context.Context, s state) (state, Directive, error) {
		s.N = 1
		return s, End(), nil
	})
	b.AllowNoOutgoingRoute("once")
	b.SetEntryPoint("once")
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	handle, err := g.NewRunner(newMemoryCP[state, NoEffect]()).Stream(
		context.Background(), "close-completed-bd-th", state{},
	)
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
	handle.RequestStop()
	if err := handle.Wait(); err != nil {
		t.Fatalf("completed run must succeed when consumer close blocks event delivery: %v", err)
	}
}

func TestStreamRequestLocalHandoffSkipOnSaveErrorSkip(t *testing.T) {
	t.Parallel()

	type state struct{ N int }
	ready := make(chan struct{})
	b := NewGraph[state, NoEffect](func(_ state, u state) state { return u })
	b.AddNode("work", func(ctx context.Context, s state) (state, Directive, error) {
		s.N++
		close(ready)
		<-ctx.Done()
		return s, Completed(), nil
	})
	b.AllowNoOutgoingRoute("work")
	b.SetEntryPoint("work")
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	cp := &failingMemoryCP[state, NoEffect]{failSave: true}
	runner := g.NewRunner(cp)
	handle, err := runner.Stream(context.Background(), "stream-htb-skip-on-save-th", state{},
		WithCheckpointErrorPolicy[state, NoEffect](CheckpointPolicySkipOnSaveError),
	)
	if err != nil {
		t.Fatalf("stream: %v", err)
	}

	out := BeginStreamCollect(handle)
	<-ready
	handoffErr := runner.RequestLocalHandoff(context.Background(), "stream-htb-skip-on-save-th")
	if !errors.Is(handoffErr, ErrCheckpointSkipped) {
		t.Fatalf("expected ErrCheckpointSkipped from RequestLocalHandoff, got %v", handoffErr)
	}
	_, waitErr := awaitStreamCollect(t, handle, out, 5*time.Second)
	if !errors.Is(waitErr, ErrCheckpointSkipped) {
		t.Fatalf("expected ErrCheckpointSkipped from Wait, got %v", waitErr)
	}
	if _, _, loadErr := cp.Load(context.Background(), "stream-htb-skip-on-save-th"); loadErr == nil {
		t.Fatal("expected no snapshot on stream handoff soft warn skip")
	}
}

func TestStreamRequestLocalHandoffEnqueueFailAfterPersist(t *testing.T) {
	t.Parallel()

	type state struct{ N int }
	ready := make(chan struct{})
	outbox := &stubHandoffOutbox{err: errors.New("broker down")}
	b := NewGraph[state, NoEffect](func(_ state, u state) state { return u })
	b.AddNode("work", func(ctx context.Context, s state) (state, Directive, error) {
		s.N++
		close(ready)
		<-ctx.Done()
		return s, Completed(), nil
	})
	b.AllowNoOutgoingRoute("work")
	b.SetEntryPoint("work")
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	cp := newMemoryCP[state, NoEffect]()
	runner := g.NewRunner(cp)
	handle, err := runner.Stream(context.Background(), "stream-htb-enqueue-th", state{},
		WithHandoffOutbox[state, NoEffect](outbox),
	)
	if err != nil {
		t.Fatalf("stream: %v", err)
	}

	outCh := BeginStreamCollect(handle)
	<-ready
	handoffErr := runner.RequestLocalHandoff(context.Background(), "stream-htb-enqueue-th")
	if !errors.Is(handoffErr, ErrHandoffEnqueueFailed) {
		t.Fatalf("expected ErrHandoffEnqueueFailed, got %v", handoffErr)
	}
	events, waitErr := awaitStreamCollect(t, handle, outCh, 5*time.Second)
	if !errors.Is(waitErr, ErrHandoffEnqueueFailed) {
		t.Fatalf("expected ErrHandoffEnqueueFailed from Wait, got %v", waitErr)
	}
	snapAfter, _, loadErr := cp.Load(context.Background(), "stream-htb-enqueue-th")
	if loadErr != nil {
		t.Fatalf("load: %v", loadErr)
	}
	if snapAfter.RunMeta.HandoffStatus != HandoffStatusOrphaned {
		t.Fatalf("expected orphaned snapshot, got %q", snapAfter.RunMeta.HandoffStatus)
	}
	foundHandoff := false
	for _, ev := range events {
		if ev.Type == EventHandoff {
			foundHandoff = true
			if ev.Reason != ReasonHandoffOrphaned {
				t.Fatalf("expected handoff reason %q, got %q", ReasonHandoffOrphaned, ev.Reason)
			}
		}
	}
	if !foundHandoff {
		t.Fatalf("expected EventHandoff on enqueue failure, got %+v", events)
	}

	syncG, syncReady := blockingHandoffWorkGraph[state, NoEffect](t)
	syncCP := newMemoryCP[state, NoEffect]()
	syncRunner := syncG.NewRunner(syncCP)
	startDone := make(chan *RunResult[state, NoEffect], 1)
	go func() {
		res, _ := syncRunner.Start(context.Background(), "stream-htb-enqueue-sync-th", state{},
			WithHandoffOutbox[state, NoEffect](outbox),
		)
		startDone <- res
	}()
	<-syncReady
	htbErr := syncRunner.RequestLocalHandoff(context.Background(), "stream-htb-enqueue-sync-th")
	if !errors.Is(htbErr, ErrHandoffEnqueueFailed) {
		t.Fatalf("expected sync ErrHandoffEnqueueFailed, got %v", htbErr)
	}
	syncRes := <-startDone
	assertTerminalEventReasonMatchesSync(t, events, EventHandoff, syncRes.Reason)
	assertHandoffReasonMatchesStatus(t, syncRes, syncCP, "stream-htb-enqueue-sync-th", "background_handoff")
	assertOrphanedHandoffSnapshot(t, cp, "stream-htb-enqueue-th", syncRes, "background_handoff")
	assertRunMetaHandoffStatusMatchesSnapshot(t, syncRes, syncCP, "stream-htb-enqueue-sync-th")
}

func TestStreamHandoffEnqueueOkPatchEnqueuedFails(t *testing.T) {
	t.Parallel()

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

	handle, err := g.NewRunner(cp).Stream(context.Background(), "stream-patch-enqueued-fail-th", state{},
		WithHandoffOutbox[state, NoEffect](outbox),
	)
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
	events, waitErr := CollectEventsAndWait(context.Background(), handle)
	if waitErr == nil {
		t.Fatal("expected patch failure from Wait")
	}
	if !errors.Is(waitErr, ErrHandoffPatchFailed) {
		t.Fatalf("expected ErrHandoffPatchFailed from Wait, got %v", waitErr)
	}
	if len(outbox.calls) != 0 {
		t.Fatalf("outbox must not receive intent before enqueued patch succeeds, got %d calls", len(outbox.calls))
	}
	assertPendingHandoffSnapshot(t, cp, "stream-patch-enqueued-fail-th")
	syncRes, syncErr := g.NewRunner(cp).Start(context.Background(), "stream-patch-enqueued-sync-th", state{},
		WithHandoffOutbox[state, NoEffect](outbox),
	)
	if syncErr == nil {
		t.Fatalf("expected sync patch failure, got %+v", syncRes)
	}
	assertPendingHandoffSnapshot(t, cp, "stream-patch-enqueued-fail-th")
	assertRunMetaHandoffStatusMatchesSnapshot(t, syncRes, cp, "stream-patch-enqueued-sync-th")
	assertHandoffReasonMatchesStatus(t, syncRes, cp, "stream-patch-enqueued-sync-th", "bg")
	assertTerminalEventReasonMatchesSync(t, events, EventHandoff, syncRes.Reason)
}

func TestStreamHandoffEnqueueFailPatchOrphanFails(t *testing.T) {
	t.Parallel()

	type state struct{}

	outbox := &stubHandoffOutbox{err: errors.New("broker down")}
	cp := &handoffPatchFailCP[state, NoEffect]{failOnStatus: HandoffStatusOrphaned}
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

	handle, err := g.NewRunner(cp).Stream(context.Background(), "stream-patch-orphan-fail-th", state{},
		WithHandoffOutbox[state, NoEffect](outbox),
	)
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
	events, waitErr := CollectEventsAndWait(context.Background(), handle)
	if !errors.Is(waitErr, ErrHandoffEnqueueFailed) {
		t.Fatalf("expected ErrHandoffEnqueueFailed, got %v", waitErr)
	}
	snap, _, loadErr := cp.Load(context.Background(), "stream-patch-orphan-fail-th")
	if loadErr != nil {
		t.Fatalf("load: %v", loadErr)
	}
	if snap.RunMeta.HandoffStatus != HandoffStatusEnqueued {
		t.Fatalf("expected enqueued when orphan patch fails, got %q", snap.RunMeta.HandoffStatus)
	}
	syncRes, syncErr := g.NewRunner(cp).Start(context.Background(), "stream-patch-orphan-sync-th", state{},
		WithHandoffOutbox[state, NoEffect](outbox),
	)
	if !errors.Is(syncErr, ErrHandoffEnqueueFailed) {
		t.Fatalf("expected sync ErrHandoffEnqueueFailed, got %v", syncErr)
	}
	assertRunMetaHandoffStatusMatchesSnapshot(t, syncRes, cp, "stream-patch-orphan-sync-th")
	assertHandoffReasonMatchesStatus(t, syncRes, cp, "stream-patch-orphan-sync-th", "bg")
	assertTerminalEventReasonMatchesSync(t, events, EventHandoff, syncRes.Reason)
}

func TestStreamTransactionalHandoffSuccess(t *testing.T) {
	t.Parallel()

	type state struct{}

	outbox := &stubHandoffOutbox{}
	cp := &transactionalMemoryCP[state, NoEffect]{memoryCP: newMemoryCP[state, NoEffect]()}
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

	handle, err := g.NewRunner(cp).Stream(context.Background(), "stream-tx-handoff-ok-th", state{},
		WithHandoffOutbox[state, NoEffect](outbox),
	)
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
	_, waitErr := CollectEventsAndWait(context.Background(), handle)
	if waitErr != nil {
		t.Fatalf("wait: %v", waitErr)
	}
	snap, rev, loadErr := cp.Load(context.Background(), "stream-tx-handoff-ok-th")
	if loadErr != nil {
		t.Fatalf("load: %v", loadErr)
	}
	if snap.RunMeta.HandoffStatus != HandoffStatusEnqueued {
		t.Fatalf("expected enqueued, got %q", snap.RunMeta.HandoffStatus)
	}
	if len(outbox.calls) != 1 || outbox.calls[0].SnapshotRevision != rev {
		t.Fatalf("unexpected outbox calls: %+v rev=%d", outbox.calls, rev)
	}
}

func TestStreamHandoffEnqueueOkBothPatchesFail(t *testing.T) {
	t.Parallel()

	type state struct{}

	outbox := &stubHandoffOutbox{}
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

	handle, err := g.NewRunner(cp).Stream(context.Background(), "stream-both-patch-fail-th", state{},
		WithHandoffOutbox[state, NoEffect](outbox),
	)
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
	events, waitErr := CollectEventsAndWait(context.Background(), handle)
	if waitErr == nil {
		t.Fatal("expected patch failure from Wait")
	}
	if !errors.Is(waitErr, ErrHandoffPatchFailed) {
		t.Fatalf("expected ErrHandoffPatchFailed from Wait, got %v", waitErr)
	}
	if len(outbox.calls) != 0 {
		t.Fatalf("outbox must not receive intent before enqueued patch succeeds, got %d calls", len(outbox.calls))
	}
	snap, _, loadErr := cp.Load(context.Background(), "stream-both-patch-fail-th")
	if loadErr != nil {
		t.Fatalf("load: %v", loadErr)
	}
	if snap.RunMeta.HandoffStatus != HandoffStatusPending {
		t.Fatalf("expected pending when both patches fail, got %q", snap.RunMeta.HandoffStatus)
	}
	syncRes, syncErr := g.NewRunner(cp).Start(context.Background(), "stream-both-patch-sync-th", state{},
		WithHandoffOutbox[state, NoEffect](outbox),
	)
	if syncErr == nil {
		t.Fatalf("expected sync patch failure, got %+v", syncRes)
	}
	assertRunMetaHandoffStatusMatchesSnapshot(t, syncRes, cp, "stream-both-patch-sync-th")
	assertHandoffReasonMatchesStatus(t, syncRes, cp, "stream-both-patch-sync-th", "bg")
	assertTerminalEventReasonMatchesSync(t, events, EventHandoff, syncRes.Reason)
}

type saveFailOverlayCP[T, E any] struct {
	inner *memoryCP[T, E]
}

func (s *saveFailOverlayCP[T, E]) Save(
	_ context.Context,
	_ uint64,
	_ Snapshot[T, E],
) (uint64, error) {
	return 0, errors.New("save failed")
}

func (s *saveFailOverlayCP[T, E]) Load(
	ctx context.Context,
	threadID string,
) (Snapshot[T, E], uint64, error) {
	return s.inner.Load(ctx, threadID)
}

func (s *saveFailOverlayCP[T, E]) GetHistory(
	ctx context.Context,
	threadID string,
	limit int,
) ([]Snapshot[T, E], error) {
	return s.inner.GetHistory(ctx, threadID, limit)
}

func (s *saveFailOverlayCP[T, E]) Prune(
	ctx context.Context,
	threadID string,
	retainCount int,
) error {
	return s.inner.Prune(ctx, threadID, retainCount)
}

func (s *saveFailOverlayCP[T, E]) Delete(ctx context.Context, threadID string) error {
	return s.inner.Delete(ctx, threadID)
}

func (s *saveFailOverlayCP[T, E]) DeleteIfIdle(ctx context.Context, threadID string) error {
	return s.inner.DeleteIfIdle(ctx, threadID)
}

// Intentional no-drain: RequestStop + Wait without Events() drain must not deadlock.
func TestStreamRequestStopWithoutDrainNoDeadlock(t *testing.T) {
	type state struct{}
	b := NewGraph[state, NoEffect](func(_ state, u state) state { return u })
	b.AddNode("one", func(_ context.Context, s state) (state, Directive, error) {
		return s, End(), nil
	})
	b.SetEntryPoint("one")
	b.AllowNoOutgoingRoute("one")
	g, _ := b.Compile()

	handle, err := g.NewRunner(newMemoryCP[state, NoEffect]()).
		Stream(context.Background(), "close-no-drain", state{})
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
	handle.RequestStop()
	if err := handle.Wait(); err != nil {
		t.Fatalf("unexpected wait error: %v", err)
	}
}

func TestStreamRequestStopUnblocksBlockingNode(t *testing.T) {
	type state struct{}
	ready := make(chan struct{})
	b := NewGraph[state, NoEffect](func(_ state, u state) state { return u })
	b.AddNode("work", func(ctx context.Context, s state) (state, Directive, error) {
		select {
		case <-ready:
		default:
			close(ready)
		}
		<-ctx.Done()
		return s, Completed(), nil
	})
	b.AllowNoOutgoingRoute("work")
	b.SetEntryPoint("work")
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	cp := newMemoryCP[state, NoEffect]()
	handle, err := g.NewRunner(cp).Stream(context.Background(), "stop-blocking-th", state{})
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
	outCh := BeginStreamCollect(handle)
	<-ready
	handle.RequestStop()
	events, waitErr := awaitStreamCollect(t, handle, outCh, 5*time.Second)
	if waitErr != nil {
		t.Fatalf("unexpected wait error after RequestStop on blocking node: %v", waitErr)
	}
	snap := requireSnapshotPresent(t, cp, "stop-blocking-th")
	if snap.RunMeta.Segment.EndReason != SegmentEndContextCanceled {
		t.Fatalf("expected context_canceled segment, got %q", snap.RunMeta.Segment.EndReason)
	}
	if term, ok := terminalEvent(events); ok {
		requireTerminalEventReason(t, events, EventContextCanceled, "context_canceled")
		if term.ExecutionPointer != "work" {
			t.Fatalf("expected pointer work, got %q", term.ExecutionPointer)
		}
	}
}

func TestStreamImmediateRequestStopCancelsBeforeSessionRegistration(t *testing.T) {
	type state struct{}
	b := NewGraph[state, NoEffect](func(_ state, u state) state { return u })
	b.AddNode("work", func(ctx context.Context, s state) (state, Directive, error) {
		<-ctx.Done()
		return s, Completed(), nil
	})
	b.AllowNoOutgoingRoute("work")
	b.SetEntryPoint("work")
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	handle, err := g.NewRunner(newMemoryCP[state, NoEffect]()).
		Stream(context.Background(), "immediate-stop-th", state{})
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
	handle.RequestStop()

	type waitResult struct {
		result *RunResult[state, NoEffect]
		err    error
	}
	done := make(chan waitResult, 1)
	go func() {
		result, waitErr := handle.WaitResult()
		done <- waitResult{result: result, err: waitErr}
	}()

	select {
	case got := <-done:
		if got.err != nil {
			t.Fatalf("unexpected wait error after immediate RequestStop: %v", got.err)
		}
		if got.result == nil || got.result.Status != RunStatusContextCanceled {
			t.Fatalf("expected context-canceled result, got %+v", got.result)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("WaitResult hung after immediate RequestStop")
	}
}

func TestStreamRecoverMiddlewareEmitsFailedEvent(t *testing.T) {
	t.Parallel()

	type state struct{}
	b := NewGraph[state, NoEffect](func(_ state, u state) state { return u })
	b.Use(RecoverMiddleware[state, NoEffect]())
	b.AddNode("panic", func(_ context.Context, _ state) (state, Directive, error) {
		panic("stream panic")
	})
	b.SetEntryPoint("panic")
	b.AllowNoOutgoingRoute("panic")
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	cp := newMemoryCP[state, NoEffect]()
	runner := g.NewRunner(cp)
	syncRes, syncErr := runner.Start(context.Background(), "panic-sync", state{})
	if syncErr == nil {
		t.Fatalf("expected sync error, got %+v", syncRes)
	}
	if syncRes.Reason == "" {
		t.Fatalf("expected sync reason, got empty")
	}
	handle, err := runner.Stream(context.Background(), "panic-stream", state{})
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
	events, waitErr := CollectEventsAndWait(context.Background(), handle)
	requireTerminalEventReason(t, events, EventFailed, syncRes.Reason)
	if waitErr == nil {
		t.Fatal("expected stream failure error")
	}
}

func hasStreamEventType[T, E any](events []RunEvent[T, E], eventType EventType) bool {
	for _, ev := range events {
		if ev.Type == eventType {
			return true
		}
	}
	return false
}

type cancelAwareCP[T, E any] struct {
	saved bool
}

func (c *cancelAwareCP[T, E]) Save(
	ctx context.Context,
	expectedRevision uint64,
	_ Snapshot[T, E],
) (uint64, error) {
	if ctx.Err() != nil {
		return 0, ctx.Err()
	}
	c.saved = true
	return expectedRevision + 1, nil
}

func (c *cancelAwareCP[T, E]) Load(context.Context, string) (Snapshot[T, E], uint64, error) {
	return Snapshot[T, E]{}, 0, ErrThreadNotFound
}

func (c *cancelAwareCP[T, E]) GetHistory(context.Context, string, int) ([]Snapshot[T, E], error) {
	return []Snapshot[T, E]{}, nil
}

func (c *cancelAwareCP[T, E]) Prune(context.Context, string, int) error {
	return nil
}

func (c *cancelAwareCP[T, E]) Delete(context.Context, string) error {
	return nil
}

// DeleteIfIdle is a stream test stub.
func (c *cancelAwareCP[T, E]) DeleteIfIdle(context.Context, string) error {
	return nil
}

func TestStreamClosePersistVsEventDroppedTerminalEventSkipOnSaveErrorParentCancel(t *testing.T) {
	t.Parallel()

	type state struct{ N int }
	ready := make(chan struct{})
	cp := &failingMemoryCP[state, NoEffect]{failSave: true}
	b := NewGraph[state, NoEffect](func(_ state, u state) state { return u })
	b.AddNode("work", func(ctx context.Context, s state) (state, Directive, error) {
		s.N++
		select {
		case <-ready:
		default:
			close(ready)
		}
		<-ctx.Done()
		return s, Completed(), nil
	})
	b.AllowNoOutgoingRoute("work")
	b.SetEntryPoint("work")
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	handle, err := g.NewRunner(cp).Stream(ctx, "close-cancel-race-th", state{},
		WithCheckpointErrorPolicy[state, NoEffect](CheckpointPolicySkipOnSaveError),
	)
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
	out := BeginStreamCollect(handle)
	<-ready
	cancel()
	handle.RequestStop()
	pre, waitErr := awaitStreamCollect(t, handle, out, 5*time.Second)
	if waitErr != nil && !errors.Is(waitErr, ErrCheckpointSkipped) &&
		!errors.Is(waitErr, context.Canceled) {
		t.Fatalf("Wait: got %v want nil, ErrCheckpointSkipped, or context.Canceled", waitErr)
	}
	requireSnapshotMissing(t, cp, "close-cancel-race-th")
	for _, ev := range pre {
		if ev.Type == EventContextCanceled {
			if ev.Reason != ReasonContextCanceledCheckpointSkipped {
				t.Fatalf(
					"expected reason %q on RequestStop+parent cancel, got %q",
					ReasonContextCanceledCheckpointSkipped, ev.Reason,
				)
			}
			return
		}
	}
	if hasStreamEventType(pre, EventCheckpointFailed) {
		return
	}
	t.Log("persist-vs-event: terminal stream event dropped; Wait is authoritative")
}

func TestStreamParentCancelAndRequestStopReturnsContextCanceled(t *testing.T) {
	t.Parallel()

	type state struct{}
	ready := make(chan struct{})
	b := NewGraph[state, NoEffect](func(_ state, u state) state { return u })
	b.AddNode("work", func(ctx context.Context, s state) (state, Directive, error) {
		select {
		case <-ready:
		default:
			close(ready)
		}
		<-ctx.Done()
		return s, Completed(), nil
	})
	b.AllowNoOutgoingRoute("work")
	b.SetEntryPoint("work")
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	ctx, cancel := context.WithCancel(t.Context())
	handle, err := g.NewRunner(newMemoryCP[state, NoEffect]()).Stream(ctx, "parent-cancel-stop-th", state{})
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
	out := BeginStreamCollect(handle)
	<-ready
	cancel()
	handle.RequestStop()

	result, waitErr := AwaitStreamCollect(context.Background(), handle, out)
	if !errors.Is(waitErr, context.Canceled) {
		t.Fatalf("expected context.Canceled when parent context cancels, got result=%+v err=%v", result, waitErr)
	}
	if result.Outcome == nil || result.Outcome.Status != RunStatusContextCanceled {
		t.Fatalf("expected context canceled outcome, got %+v", result.Outcome)
	}
}
