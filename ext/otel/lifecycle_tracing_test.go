package otel

import (
	"context"
	"errors"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/skosovsky/flowy"
	"github.com/skosovsky/flowy/testutil"
)

func TestInstallLifecycleObserverWithTracing(t *testing.T) {
	sr := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sr))
	prev := otel.GetTracerProvider()
	otel.SetTracerProvider(tp)
	t.Cleanup(func() {
		otel.SetTracerProvider(prev)
		flowy.SetLifecycleObserver(nil)
	})

	if err := InstallLifecycleObserverWithTracing(); err != nil {
		t.Fatalf("install: %v", err)
	}

	type state struct{}
	b := flowy.NewGraph[state, flowy.NoEffect](func(_ state, u state) state { return u })
	b.AddNode("work", func(_ context.Context, s state) (state, flowy.Directive, error) {
		return s, flowy.Handoff("bg"), nil
	})
	b.AllowNoOutgoingRoute("work")
	b.SetEntryPoint("work")
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	_, _ = g.NewRunner(newCP[state, flowy.NoEffect]{}).Start(context.Background(), "trace-handoff-th", state{},
		flowy.WithHandoffOutbox[state, flowy.NoEffect](&testHandoffOutbox{err: errors.New("down")}),
	)

	spans := sr.Ended()
	if len(spans) != 1 {
		t.Fatalf("expected 1 lifecycle span, got %d", len(spans))
	}
	if spans[0].Name() != "flowy.lifecycle.handoff_enqueued" {
		t.Fatalf("unexpected span name: %s", spans[0].Name())
	}
	if !hasHandoffSpanStatus(spans[0].Attributes(), "enqueue_failed") {
		t.Fatalf("expected status=enqueue_failed, attrs=%+v", spans[0].Attributes())
	}
}

func TestInstallLifecycleObserverWithTracingHandoffSuccess(t *testing.T) {
	sr := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sr))
	prev := otel.GetTracerProvider()
	otel.SetTracerProvider(tp)
	t.Cleanup(func() {
		otel.SetTracerProvider(prev)
		flowy.SetLifecycleObserver(nil)
	})
	if err := InstallLifecycleObserverWithTracing(); err != nil {
		t.Fatalf("install: %v", err)
	}

	type state struct{}
	b := flowy.NewGraph[state, flowy.NoEffect](func(_ state, u state) state { return u })
	b.AddNode("work", func(_ context.Context, s state) (state, flowy.Directive, error) {
		return s, flowy.Handoff("bg"), nil
	})
	b.AllowNoOutgoingRoute("work").SetEntryPoint("work")
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	_, _ = g.NewRunner(newCP[state, flowy.NoEffect]{}).Start(context.Background(), "trace-handoff-ok-th", state{},
		flowy.WithHandoffOutbox[state, flowy.NoEffect](&testHandoffOutbox{}),
	)

	spans := sr.Ended()
	if len(spans) != 1 {
		t.Fatalf("expected 1 lifecycle span, got %d", len(spans))
	}
	if spans[0].Name() != "flowy.lifecycle.handoff_enqueued" {
		t.Fatalf("unexpected span name: %s", spans[0].Name())
	}
	if !hasHandoffSpanStatus(spans[0].Attributes(), "success") {
		t.Fatalf("expected status=success, attrs=%+v", spans[0].Attributes())
	}
}

func TestInstallLifecycleObserverWithTracingRecoverStaleHandoffFromOrphaned(t *testing.T) {
	sr := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sr))
	prev := otel.GetTracerProvider()
	otel.SetTracerProvider(tp)
	t.Cleanup(func() {
		otel.SetTracerProvider(prev)
		flowy.SetLifecycleObserver(nil)
	})
	if err := InstallLifecycleObserverWithTracing(); err != nil {
		t.Fatalf("install: %v", err)
	}

	type state struct{}
	cp := testutil.NewMemoryCheckpointer[state, flowy.NoEffect]()
	now := time.Now().UTC().Add(-time.Hour)
	if _, err := cp.Save(context.Background(), 0, flowy.Snapshot[state, flowy.NoEffect]{
		ThreadID: "trace-recover-orphan-th", ExecutionPointer: "work", State: state{},
		RunMeta: flowy.RunMetadata{
			HandoffStatus: flowy.HandoffStatusOrphaned, HandoffPendingAt: now,
		},
	}); err != nil {
		t.Fatal(err)
	}
	b := flowy.NewGraph[state, flowy.NoEffect](func(_ state, u state) state { return u })
	b.AddNode("work", func(_ context.Context, s state) (state, flowy.Directive, error) { return s, flowy.End(), nil })
	b.AllowNoOutgoingRoute("work").SetEntryPoint("work")
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	runner := g.NewRunnerWithOptions(cp, []flowy.RunnerOption[state, flowy.NoEffect]{
		flowy.WithRunnerHandoffOutbox[state, flowy.NoEffect](&testHandoffOutbox{}),
	})
	if _, recoverErr := runner.RecoverStaleHandoff(context.Background(), "trace-recover-orphan-th"); recoverErr != nil {
		t.Fatalf("recover: %v", recoverErr)
	}

	spans := sr.Ended()
	if len(spans) != 1 || spans[0].Name() != "flowy.lifecycle.handoff_enqueued" {
		t.Fatalf("expected handoff_enqueued span, got %+v", spans)
	}
	if !hasHandoffSpanStatus(spans[0].Attributes(), "success") {
		t.Fatalf("expected status=success, attrs=%+v", spans[0].Attributes())
	}
}

func TestInstallLifecycleObserverWithTracingRecoverStaleHandoffStalePending(t *testing.T) {
	sr := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sr))
	prev := otel.GetTracerProvider()
	otel.SetTracerProvider(tp)
	t.Cleanup(func() {
		otel.SetTracerProvider(prev)
		flowy.SetLifecycleObserver(nil)
	})
	if err := InstallLifecycleObserverWithTracing(); err != nil {
		t.Fatalf("install: %v", err)
	}

	type state struct{}
	cp := testutil.NewMemoryCheckpointer[state, flowy.NoEffect]()
	staleAt := time.Now().UTC().Add(-10 * time.Minute)
	if _, err := cp.Save(context.Background(), 0, flowy.Snapshot[state, flowy.NoEffect]{
		ThreadID: "trace-recover-stale-th", ExecutionPointer: "work", State: state{},
		RunMeta: flowy.RunMetadata{
			HandoffStatus: flowy.HandoffStatusPending, HandoffPendingAt: staleAt,
		},
	}); err != nil {
		t.Fatal(err)
	}
	b := flowy.NewGraph[state, flowy.NoEffect](func(_ state, u state) state { return u })
	b.AddNode("work", func(_ context.Context, s state) (state, flowy.Directive, error) { return s, flowy.End(), nil })
	b.AllowNoOutgoingRoute("work").SetEntryPoint("work")
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	runner := g.NewRunnerWithOptions(cp, []flowy.RunnerOption[state, flowy.NoEffect]{
		flowy.WithRunnerHandoffOutbox[state, flowy.NoEffect](&testHandoffOutbox{}),
		flowy.WithHandoffStaleAfter[state, flowy.NoEffect](time.Minute),
	})
	if _, recoverErr := runner.RecoverStaleHandoff(context.Background(), "trace-recover-stale-th"); recoverErr != nil {
		t.Fatalf("recover: %v", recoverErr)
	}

	spans := sr.Ended()
	if len(spans) != 1 || spans[0].Name() != "flowy.lifecycle.handoff_enqueued" {
		t.Fatalf("expected handoff_enqueued span, got %+v", spans)
	}
	if !hasHandoffSpanStatus(spans[0].Attributes(), "success") {
		t.Fatalf("expected status=success, attrs=%+v", spans[0].Attributes())
	}
}

func TestInstallLifecycleObserverWithTracingRecoverStaleHandoffFreshPending(t *testing.T) {
	sr := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sr))
	prev := otel.GetTracerProvider()
	otel.SetTracerProvider(tp)
	t.Cleanup(func() {
		otel.SetTracerProvider(prev)
		flowy.SetLifecycleObserver(nil)
	})
	if err := InstallLifecycleObserverWithTracing(); err != nil {
		t.Fatalf("install: %v", err)
	}

	type state struct{}
	cp := testutil.NewMemoryCheckpointer[state, flowy.NoEffect]()
	if _, err := cp.Save(context.Background(), 0, flowy.Snapshot[state, flowy.NoEffect]{
		ThreadID: "trace-recover-pending-th", ExecutionPointer: "work", State: state{},
		RunMeta: flowy.RunMetadata{
			HandoffStatus: flowy.HandoffStatusPending, HandoffPendingAt: time.Now().UTC(),
		},
	}); err != nil {
		t.Fatal(err)
	}
	b := flowy.NewGraph[state, flowy.NoEffect](func(_ state, u state) state { return u })
	b.AddNode("work", func(_ context.Context, s state) (state, flowy.Directive, error) { return s, flowy.End(), nil })
	b.AllowNoOutgoingRoute("work").SetEntryPoint("work")
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	runner := g.NewRunnerWithOptions(cp, []flowy.RunnerOption[state, flowy.NoEffect]{
		flowy.WithRunnerHandoffOutbox[state, flowy.NoEffect](&testHandoffOutbox{}),
		flowy.WithHandoffStaleAfter[state, flowy.NoEffect](5 * time.Minute),
	})
	_, _ = runner.RecoverStaleHandoff(context.Background(), "trace-recover-pending-th")

	spans := sr.Ended()
	if len(spans) != 1 || spans[0].Name() != "flowy.lifecycle.resume_rejected" {
		t.Fatalf("expected resume_rejected span, got %+v", spans)
	}
	if !hasResumeRejectedReason(spans[0].Attributes(), "handoff_pending") {
		t.Fatalf("expected reason=handoff_pending, attrs=%+v", spans[0].Attributes())
	}
}

func TestInstallLifecycleObserverWithTracingResumeRejected(t *testing.T) {
	sr := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sr))
	prev := otel.GetTracerProvider()
	otel.SetTracerProvider(tp)
	t.Cleanup(func() {
		otel.SetTracerProvider(prev)
		flowy.SetLifecycleObserver(nil)
	})
	if err := InstallLifecycleObserverWithTracing(); err != nil {
		t.Fatalf("install: %v", err)
	}

	type state struct{}
	cp := testutil.NewMemoryCheckpointer[state, flowy.NoEffect]()
	if _, err := cp.Save(context.Background(), 0, flowy.Snapshot[state, flowy.NoEffect]{
		ThreadID: "trace-resume-th", ExecutionPointer: "work", State: state{},
		RunMeta: flowy.RunMetadata{HandoffStatus: flowy.HandoffStatusPending, HandoffPendingAt: time.Now().UTC()},
	}); err != nil {
		t.Fatal(err)
	}
	b := flowy.NewGraph[state, flowy.NoEffect](func(_ state, u state) state { return u })
	b.AddNode("work", func(_ context.Context, s state) (state, flowy.Directive, error) { return s, flowy.End(), nil })
	b.AllowNoOutgoingRoute("work").SetEntryPoint("work")
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	_, rev, _ := cp.Load(context.Background(), "trace-resume-th")
	_, _ = g.NewRunner(cp).Resume(context.Background(), flowy.ResumeToken{
		ThreadID: "trace-resume-th", SnapshotRevision: rev,
	})

	spans := sr.Ended()
	if len(spans) != 1 || spans[0].Name() != "flowy.lifecycle.resume_rejected" {
		t.Fatalf("expected resume_rejected span, got %+v", spans)
	}
	if !hasResumeRejectedReason(spans[0].Attributes(), "handoff_pending") {
		t.Fatalf("expected reason=handoff_pending, attrs=%+v", spans[0].Attributes())
	}
}

func TestInstallLifecycleObserverWithTracingCheckpointSoftError(t *testing.T) {
	sr := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sr))
	prev := otel.GetTracerProvider()
	otel.SetTracerProvider(tp)
	t.Cleanup(func() {
		otel.SetTracerProvider(prev)
		flowy.SetLifecycleObserver(nil)
	})
	if err := InstallLifecycleObserverWithTracing(); err != nil {
		t.Fatalf("install: %v", err)
	}

	type state struct{}
	cp := &saveFailCP[state, flowy.NoEffect]{inner: testutil.NewMemoryCheckpointer[state, flowy.NoEffect]()}
	b := flowy.NewGraph[state, flowy.NoEffect](func(_ state, u state) state { return u })
	b.AddNode("work", func(_ context.Context, s state) (state, flowy.Directive, error) {
		return s, flowy.Suspend("hold"), nil
	})
	b.AllowNoOutgoingRoute("work").SetEntryPoint("work")
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	_, _ = g.NewRunner(cp).Start(context.Background(), "trace-soft-error-th", state{},
		flowy.WithCheckpointErrorPolicy[state, flowy.NoEffect](flowy.CheckpointPolicySkipOnSaveError),
	)

	spans := sr.Ended()
	if len(spans) != 1 || spans[0].Name() != "flowy.lifecycle.checkpoint_soft_error" {
		t.Fatalf("expected checkpoint_soft_error span, got %+v", spans)
	}
	if !hasSpanStringAttr(spans[0].Attributes(), "thread_id", "trace-soft-error-th") {
		t.Fatalf("expected thread_id attr, got %+v", spans[0].Attributes())
	}
	if !hasSpanStringAttr(spans[0].Attributes(), "node", "work") {
		t.Fatalf("expected node=work attr, got %+v", spans[0].Attributes())
	}
}

func TestInstallLifecycleObserverWithTracingSkipOnSaveErrorHandoff(t *testing.T) {
	sr := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sr))
	prev := otel.GetTracerProvider()
	otel.SetTracerProvider(tp)
	t.Cleanup(func() {
		otel.SetTracerProvider(prev)
		flowy.SetLifecycleObserver(nil)
	})
	if err := InstallLifecycleObserverWithTracing(); err != nil {
		t.Fatalf("install: %v", err)
	}

	type state struct{}
	cp := &saveFailCP[state, flowy.NoEffect]{inner: testutil.NewMemoryCheckpointer[state, flowy.NoEffect]()}
	b := flowy.NewGraph[state, flowy.NoEffect](func(_ state, u state) state { return u })
	b.AddNode("work", func(_ context.Context, s state) (state, flowy.Directive, error) {
		return s, flowy.Handoff("bg"), nil
	})
	b.AllowNoOutgoingRoute("work").SetEntryPoint("work")
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	_, _ = g.NewRunner(cp).Start(context.Background(), "trace-skip-handoff-th", state{},
		flowy.WithCheckpointErrorPolicy[state, flowy.NoEffect](flowy.CheckpointPolicySkipOnSaveError),
		flowy.WithHandoffOutbox[state, flowy.NoEffect](&testHandoffOutbox{}),
	)

	spans := sr.Ended()
	if len(spans) != 1 || spans[0].Name() != "flowy.lifecycle.checkpoint_soft_error" {
		t.Fatalf("expected checkpoint_soft_error span, got %+v", spans)
	}
	if !hasSpanStringAttr(spans[0].Attributes(), "thread_id", "trace-skip-handoff-th") {
		t.Fatalf("expected thread_id attr, got %+v", spans[0].Attributes())
	}
}

func TestInstallLifecycleObserverWithTracingHandoffSaveFailed(t *testing.T) {
	sr := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sr))
	prev := otel.GetTracerProvider()
	otel.SetTracerProvider(tp)
	t.Cleanup(func() {
		otel.SetTracerProvider(prev)
		flowy.SetLifecycleObserver(nil)
	})
	if err := InstallLifecycleObserverWithTracing(); err != nil {
		t.Fatalf("install: %v", err)
	}

	type state struct{}
	cp := &otelTxErrCP[state, flowy.NoEffect]{err: flowy.ErrConcurrencyConflict}
	b := flowy.NewGraph[state, flowy.NoEffect](func(_ state, u state) state { return u })
	b.AddNode("work", func(_ context.Context, s state) (state, flowy.Directive, error) {
		return s, flowy.Handoff("bg"), nil
	})
	b.AllowNoOutgoingRoute("work").SetEntryPoint("work")
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	_, _ = g.NewRunner(cp).Start(context.Background(), "trace-tx-save-fail-th", state{},
		flowy.WithHandoffOutbox[state, flowy.NoEffect](&testHandoffOutbox{}),
	)

	spans := sr.Ended()
	if len(spans) != 1 {
		t.Fatalf("expected 1 span, got %d", len(spans))
	}
	if spans[0].Name() != "flowy.lifecycle.handoff_enqueued" {
		t.Fatalf("unexpected span name: %s", spans[0].Name())
	}
	if !hasHandoffSpanStatus(spans[0].Attributes(), "save_failed") {
		t.Fatalf("expected status=save_failed, attrs=%+v", spans[0].Attributes())
	}
}

func TestInstallLifecycleObserverWithTracingHandoffCommitFailed(t *testing.T) {
	sr := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sr))
	prev := otel.GetTracerProvider()
	otel.SetTracerProvider(tp)
	t.Cleanup(func() {
		otel.SetTracerProvider(prev)
		flowy.SetLifecycleObserver(nil)
	})
	if err := InstallLifecycleObserverWithTracing(); err != nil {
		t.Fatalf("install: %v", err)
	}

	type state struct{}
	cp := &otelTxErrCP[state, flowy.NoEffect]{err: flowy.ErrTransactionalHandoffCommitFailed}
	b := flowy.NewGraph[state, flowy.NoEffect](func(_ state, u state) state { return u })
	b.AddNode("work", func(_ context.Context, s state) (state, flowy.Directive, error) {
		return s, flowy.Handoff("bg"), nil
	})
	b.AllowNoOutgoingRoute("work").SetEntryPoint("work")
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	_, _ = g.NewRunner(cp).Start(context.Background(), "trace-tx-commit-fail-th", state{},
		flowy.WithHandoffOutbox[state, flowy.NoEffect](&testHandoffOutbox{}),
	)

	spans := sr.Ended()
	if len(spans) != 1 {
		t.Fatalf("expected 1 span, got %d", len(spans))
	}
	if !hasHandoffSpanStatus(spans[0].Attributes(), "commit_failed") {
		t.Fatalf("expected status=commit_failed, attrs=%+v", spans[0].Attributes())
	}
}

func TestInstallLifecycleObserverWithTracingHandoffPatchEnqueuedFailed(t *testing.T) {
	sr := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sr))
	prev := otel.GetTracerProvider()
	otel.SetTracerProvider(tp)
	t.Cleanup(func() {
		otel.SetTracerProvider(prev)
		flowy.SetLifecycleObserver(nil)
	})
	if err := InstallLifecycleObserverWithTracing(); err != nil {
		t.Fatalf("install: %v", err)
	}

	type state struct{}
	cp := &otelHandoffPatchFailCP[state, flowy.NoEffect]{failOn: flowy.HandoffStatusEnqueued}
	b := flowy.NewGraph[state, flowy.NoEffect](func(_ state, u state) state { return u })
	b.AddNode("work", func(_ context.Context, s state) (state, flowy.Directive, error) {
		return s, flowy.Handoff("bg"), nil
	})
	b.AllowNoOutgoingRoute("work").SetEntryPoint("work")
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	_, _ = g.NewRunner(cp).Start(context.Background(), "trace-patch-enq-th", state{},
		flowy.WithHandoffOutbox[state, flowy.NoEffect](&testHandoffOutbox{}),
	)

	spans := sr.Ended()
	if len(spans) != 1 {
		t.Fatalf("expected 1 span, got %d", len(spans))
	}
	if !hasHandoffSpanStatus(spans[0].Attributes(), "patch_enqueued_failed") {
		t.Fatalf("expected status=patch_enqueued_failed, attrs=%+v", spans[0].Attributes())
	}
}

func TestInstallLifecycleObserverWithTracingHandoffPatchOrphanFailed(t *testing.T) {
	sr := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sr))
	prev := otel.GetTracerProvider()
	otel.SetTracerProvider(tp)
	t.Cleanup(func() {
		otel.SetTracerProvider(prev)
		flowy.SetLifecycleObserver(nil)
	})
	if err := InstallLifecycleObserverWithTracing(); err != nil {
		t.Fatalf("install: %v", err)
	}

	type state struct{}
	outbox := &testHandoffOutbox{err: errors.New("broker down")}
	cp := &otelHandoffPatchFailCP[state, flowy.NoEffect]{failOn: flowy.HandoffStatusOrphaned}
	b := flowy.NewGraph[state, flowy.NoEffect](func(_ state, u state) state { return u })
	b.AddNode("work", func(_ context.Context, s state) (state, flowy.Directive, error) {
		return s, flowy.Handoff("bg"), nil
	})
	b.AllowNoOutgoingRoute("work").SetEntryPoint("work")
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	_, _ = g.NewRunner(cp).Start(context.Background(), "trace-patch-orphan-th", state{},
		flowy.WithHandoffOutbox[state, flowy.NoEffect](outbox),
	)

	spans := sr.Ended()
	if len(spans) != 1 {
		t.Fatalf("expected 1 span, got %d", len(spans))
	}
	if !hasHandoffSpanStatus(spans[0].Attributes(), "patch_orphan_failed") {
		t.Fatalf("expected status=patch_orphan_failed, attrs=%+v", spans[0].Attributes())
	}
}

func hasHandoffSpanStatus(attrs []attribute.KeyValue, status string) bool {
	for _, attr := range attrs {
		if string(attr.Key) == "status" && attr.Value.AsString() == status {
			return true
		}
	}
	return false
}

func hasResumeRejectedReason(attrs []attribute.KeyValue, reason string) bool {
	for _, attr := range attrs {
		if string(attr.Key) == "reason" && attr.Value.AsString() == reason {
			return true
		}
	}
	return false
}
