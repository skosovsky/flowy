package flowy

import (
	"context"
	"errors"
	"testing"
)

func TestDuplicateStartSameThreadIDReturnsThreadAlreadyRunning(t *testing.T) {
	t.Parallel()

	type state struct{}

	ready := make(chan struct{})
	b := NewGraph[state, NoEffect](func(_ state, u state) state { return u })
	b.AddNode("block", func(ctx context.Context, s state) (state, Directive, error) {
		close(ready)
		<-ctx.Done()
		return s, Completed(), nil
	})
	b.AllowNoOutgoingRoute("block")
	b.SetEntryPoint("block")
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	runner := g.NewRunner(newMemoryCP[state, NoEffect]())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		_, _ = runner.Start(ctx, "dup-start-th", state{})
	}()
	<-ready

	_, err = runner.Start(context.Background(), "dup-start-th", state{})
	if !errors.Is(err, ErrThreadAlreadyRunning) {
		t.Fatalf("expected ErrThreadAlreadyRunning, got %v", err)
	}
	cancel()
}

func TestDuplicateStreamSameThreadIDReturnsThreadAlreadyRunning(t *testing.T) {
	t.Parallel()

	type state struct{}

	ready := make(chan struct{})
	b := NewGraph[state, NoEffect](func(_ state, u state) state { return u })
	b.AddNode("block", func(ctx context.Context, s state) (state, Directive, error) {
		close(ready)
		<-ctx.Done()
		return s, Completed(), nil
	})
	b.AllowNoOutgoingRoute("block")
	b.SetEntryPoint("block")
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	runner := g.NewRunner(newMemoryCP[state, NoEffect]())
	streamCtx, streamCancel := context.WithCancel(context.Background())
	defer streamCancel()
	first, err := runner.Stream(streamCtx, "dup-stream-th", state{})
	if err != nil {
		t.Fatalf("first stream: %v", err)
	}
	<-first.Events()

	second, err := runner.Stream(context.Background(), "dup-stream-th", state{})
	if err != nil {
		t.Fatalf("second stream open: %v", err)
	}
	if secondDoneErr := second.Wait(); !errors.Is(secondDoneErr, ErrThreadAlreadyRunning) {
		t.Fatalf("expected ErrThreadAlreadyRunning on Wait, got %v", secondDoneErr)
	}
	third, err := runner.Stream(context.Background(), "dup-stream-th", state{})
	if err != nil {
		t.Fatalf("third stream open: %v", err)
	}
	if thirdDoneErr := third.Wait(); !errors.Is(thirdDoneErr, ErrThreadAlreadyRunning) {
		t.Fatalf("first session must stay active after duplicate Wait fail, got %v", thirdDoneErr)
	}
	first.RequestStop()
	streamCancel()
	_ = first.Wait()
}

func TestStartWhileStreamActiveSameThreadReturnsThreadAlreadyRunning(t *testing.T) {
	t.Parallel()

	type state struct{}

	ready := make(chan struct{})
	b := NewGraph[state, NoEffect](func(_ state, u state) state { return u })
	b.AddNode("block", func(ctx context.Context, s state) (state, Directive, error) {
		close(ready)
		<-ctx.Done()
		return s, Completed(), nil
	})
	b.AllowNoOutgoingRoute("block")
	b.SetEntryPoint("block")
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	runner := g.NewRunner(newMemoryCP[state, NoEffect]())
	streamCtx, streamCancel := context.WithCancel(context.Background())
	defer streamCancel()
	handle, err := runner.Stream(streamCtx, "mix-stream-start-th", state{})
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
	<-ready

	_, err = runner.Start(context.Background(), "mix-stream-start-th", state{})
	if !errors.Is(err, ErrThreadAlreadyRunning) {
		t.Fatalf("expected ErrThreadAlreadyRunning, got %v", err)
	}
	handle.RequestStop()
	streamCancel()
	_ = handle.Wait()
}

func TestStreamWhileStartActiveSameThreadReturnsThreadAlreadyRunningOnWait(t *testing.T) {
	t.Parallel()

	type state struct{}

	ready := make(chan struct{})
	b := NewGraph[state, NoEffect](func(_ state, u state) state { return u })
	b.AddNode("block", func(ctx context.Context, s state) (state, Directive, error) {
		close(ready)
		<-ctx.Done()
		return s, Completed(), nil
	})
	b.AllowNoOutgoingRoute("block")
	b.SetEntryPoint("block")
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	runner := g.NewRunner(newMemoryCP[state, NoEffect]())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		_, _ = runner.Start(ctx, "mix-start-stream-th", state{})
	}()
	<-ready

	second, err := runner.Stream(context.Background(), "mix-start-stream-th", state{})
	if err != nil {
		t.Fatalf("second stream open: %v", err)
	}
	if err := second.Wait(); !errors.Is(err, ErrThreadAlreadyRunning) {
		t.Fatalf("expected ErrThreadAlreadyRunning on Wait, got %v", err)
	}
	cancel()
}

func TestDuplicateResumeStreamSameThreadReturnsThreadAlreadyRunningOnWait(t *testing.T) {
	t.Parallel()

	type state struct{ Phase int }

	ready := make(chan struct{})
	b := NewGraph[state, NoEffect](func(_ state, u state) state { return u })
	b.AddNode("gate", func(ctx context.Context, s state) (state, Directive, error) {
		if s.Phase == 0 {
			s.Phase = 1
			return s, Suspend("pause"), nil
		}
		close(ready)
		<-ctx.Done()
		return s, Completed(), nil
	})
	b.AllowNoOutgoingRoute("gate")
	b.SetEntryPoint("gate")
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	cp := newMemoryCP[state, NoEffect]()
	runner := g.NewRunner(cp)
	suspended, err := runner.Start(context.Background(), "dup-streamresume-th", state{})
	if err != nil {
		t.Fatalf("start: %v", err)
	}

	streamCtx, streamCancel := context.WithCancel(context.Background())
	defer streamCancel()
	first, err := runner.ResumeStream(streamCtx, suspended.ResumeToken)
	if err != nil {
		t.Fatalf("first stream resume: %v", err)
	}
	<-ready

	second, err := runner.ResumeStream(context.Background(), suspended.ResumeToken)
	if err != nil {
		t.Fatalf("second stream resume open: %v", err)
	}
	if err := second.Wait(); !errors.Is(err, ErrThreadAlreadyRunning) {
		t.Fatalf("expected ErrThreadAlreadyRunning on Wait, got %v", err)
	}
	first.RequestStop()
	streamCancel()
	_ = first.Wait()
}

func TestResumeWhileStreamActiveSameThreadReturnsThreadAlreadyRunning(t *testing.T) {
	t.Parallel()

	type state struct{ Phase int }

	ready := make(chan struct{})
	b := NewGraph[state, NoEffect](func(_ state, u state) state { return u })
	b.AddNode("gate", func(ctx context.Context, s state) (state, Directive, error) {
		if s.Phase == 0 {
			s.Phase = 1
			return s, Suspend("pause"), nil
		}
		close(ready)
		<-ctx.Done()
		return s, Completed(), nil
	})
	b.AllowNoOutgoingRoute("gate")
	b.SetEntryPoint("gate")
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	cp := newMemoryCP[state, NoEffect]()
	runner := g.NewRunner(cp)
	suspended, err := runner.Start(context.Background(), "dup-mix-th", state{})
	if err != nil {
		t.Fatalf("start: %v", err)
	}

	streamCtx, streamCancel := context.WithCancel(context.Background())
	defer streamCancel()
	handle, err := runner.ResumeStream(streamCtx, suspended.ResumeToken)
	if err != nil {
		t.Fatalf("stream resume: %v", err)
	}
	<-ready

	_, err = runner.Resume(context.Background(), resumeTokenFromCP(t, cp, "dup-mix-th"))
	if !errors.Is(err, ErrThreadAlreadyRunning) {
		t.Fatalf("expected ErrThreadAlreadyRunning, got %v", err)
	}
	handle.RequestStop()
	streamCancel()
	_ = handle.Wait()
}
