package flowy

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

func TestCollectEventsAndWaitCompleted(t *testing.T) {
	t.Parallel()

	type state struct{ N int }
	b := NewGraph[state, NoEffect](func(_ state, u state) state { return u })
	b.AddNode("done", func(_ context.Context, s state) (state, Directive, error) {
		s.N = 1
		return s, End(), nil
	})
	b.SetEntryPoint("done")
	b.AllowNoOutgoingRoute("done")
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	handle, err := g.NewRunner(newMemoryCP[state, NoEffect]()).Stream(context.Background(), "collect-th", state{})
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
	events, waitErr := CollectEventsAndWait(context.Background(), handle)
	if waitErr != nil {
		t.Fatalf("wait: %v", waitErr)
	}
	if len(events) < 2 {
		t.Fatalf("expected events, got %d", len(events))
	}
	if term, ok := terminalEvent(events); !ok || term.Type != EventCompleted {
		t.Fatalf("expected completed terminal, got %+v", events)
	}
}

func TestConsumeEventsAndWaitEarlyStop(t *testing.T) {
	t.Parallel()

	type state struct{ N int }
	ready := make(chan struct{})
	b := NewGraph[state, NoEffect](func(_ state, u state) state { return u })
	b.AddNode("loop", func(_ context.Context, s state) (state, Directive, error) {
		s.N++
		select {
		case <-ready:
		default:
			close(ready)
		}
		return s, Completed(), nil
	})
	b.AddEdge("loop", "loop")
	b.SetEntryPoint("loop")
	g, err := b.Compile(WithMaxSteps(100))
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	cp := newMemoryCP[state, NoEffect]()
	handle, err := g.NewRunner(cp).Stream(context.Background(), "early-stop-th", state{})
	if err != nil {
		t.Fatalf("stream: %v", err)
	}

	var callbackCalls atomic.Int32
	waitErr := ConsumeEventsAndWait(context.Background(), handle, func(ev RunEvent[state, NoEffect]) bool {
		callbackCalls.Add(1)
		return ev.Type != EventNodeCompleted
	})
	if waitErr != nil && !errors.Is(waitErr, context.Canceled) {
		t.Fatalf("unexpected wait: %v", waitErr)
	}
	if callbackCalls.Load() < 2 {
		t.Fatalf("expected at least 2 callback invocations before stop, got %d", callbackCalls.Load())
	}
	snap := requireSnapshotPresent(t, cp, "early-stop-th")
	if snap.ExecutionPointer != "loop" {
		t.Fatalf("expected pointer loop, got %q", snap.ExecutionPointer)
	}
}

func TestBeginStreamCollectRequestStop(t *testing.T) {
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
	handle, err := g.NewRunner(cp).Stream(context.Background(), "begin-collect-th", state{})
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
	out := BeginStreamCollect(handle)
	<-ready
	handle.RequestStop()
	result, waitErr := AwaitStreamCollect(context.Background(), handle, out)
	if waitErr != nil {
		t.Fatalf("wait: %v", waitErr)
	}
	if len(result.Events) == 0 {
		t.Fatal("expected some events")
	}
	snap := requireSnapshotPresent(t, cp, "begin-collect-th")
	if snap.RunMeta.Segment.EndReason != SegmentEndContextCanceled {
		t.Fatalf("segment: got %q want %q", snap.RunMeta.Segment.EndReason, SegmentEndContextCanceled)
	}
}

func TestConsumeEventsAndWaitSilentDrainAfterFalse(t *testing.T) {
	t.Parallel()

	type state struct{ N int }
	b := NewGraph[state, NoEffect](func(_ state, u state) state { return u })
	b.AddNode("loop", func(_ context.Context, s state) (state, Directive, error) {
		s.N++
		return s, Completed(), nil
	})
	b.AddEdge("loop", "loop")
	b.SetEntryPoint("loop")
	g, err := b.Compile(WithMaxSteps(20))
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	handle, err := g.NewRunner(newMemoryCP[state, NoEffect]()).Stream(context.Background(), "silent-drain-th", state{})
	if err != nil {
		t.Fatalf("stream: %v", err)
	}

	const stopAfter = 3
	var callbackCalls atomic.Int32
	waitErr := ConsumeEventsAndWait(context.Background(), handle, func(_ RunEvent[state, NoEffect]) bool {
		callbackCalls.Add(1)
		return int(callbackCalls.Load()) < stopAfter
	})
	if waitErr != nil && !errors.Is(waitErr, context.Canceled) {
		t.Fatalf("unexpected wait: %v", waitErr)
	}
	if callbackCalls.Load() != stopAfter {
		t.Fatalf("callback calls: got %d want %d (no invocations after false)", callbackCalls.Load(), stopAfter)
	}
}

func TestConsumeEventsAndWaitContextTimeout(t *testing.T) {
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

	handle, err := g.NewRunner(newMemoryCP[state, NoEffect]()).Stream(context.Background(), "ctx-timeout-th", state{})
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
	<-ready
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	waitErr := ConsumeEventsAndWait(ctx, handle, func(RunEvent[state, NoEffect]) bool { return true })
	if waitErr == nil {
		t.Fatal("expected context error")
	}
	if !errors.Is(waitErr, context.DeadlineExceeded) {
		t.Fatalf("expected deadline exceeded, got %v", waitErr)
	}
}

func TestAwaitStreamCollectContextTimeoutRequestStop(t *testing.T) {
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

	handle, err := g.NewRunner(newMemoryCP[state, NoEffect]()).
		Stream(context.Background(), "await-ctx-timeout-th", state{})
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
	out := BeginStreamCollect(handle)
	<-ready
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_, awaitErr := AwaitStreamCollect(ctx, handle, out)
	if awaitErr == nil {
		t.Fatal("expected context error from await")
	}
	if !errors.Is(awaitErr, context.DeadlineExceeded) {
		t.Fatalf("expected deadline exceeded, got %v", awaitErr)
	}
}

func TestAwaitStreamCollectWithSnapshotResumeToken(t *testing.T) {
	t.Parallel()

	type state struct{ N int }
	b := NewGraph[state, NoEffect](func(_ state, u state) state { return u })
	b.AddNode("wait", func(_ context.Context, s state) (state, Directive, error) {
		s.N = 1
		return s, Suspend("hold"), nil
	})
	b.AllowNoOutgoingRoute("wait")
	b.SetEntryPoint("wait")
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	cp := newMemoryCP[state, NoEffect]()
	threadID := "snapshot-collect-th"
	handle, err := g.NewRunner(cp).Stream(context.Background(), threadID, state{})
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
	out := BeginStreamCollect(handle)
	result, waitErr := AwaitStreamCollectWithSnapshot(context.Background(), handle, out, cp, threadID)
	if waitErr != nil {
		t.Fatalf("wait: %v", waitErr)
	}
	if result.Snapshot == nil {
		t.Fatal("expected snapshot")
	}
	if result.ResumeToken.ThreadID != threadID {
		t.Fatalf("token thread %q want %q", result.ResumeToken.ThreadID, threadID)
	}
	if result.ResumeToken.SnapshotRevision != result.Snapshot.Revision {
		t.Fatalf("token revision %d != snapshot revision %d",
			result.ResumeToken.SnapshotRevision, result.Snapshot.Revision)
	}
	if result.TerminalEvent == nil || result.TerminalEvent.Type != EventSuspended {
		t.Fatalf("expected suspended terminal, got %+v", result.TerminalEvent)
	}
}
