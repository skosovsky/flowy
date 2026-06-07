package otel

import (
	"context"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/skosovsky/flowy"
	"github.com/skosovsky/flowy/testutil"
)

type tracingResumeRejectCase struct {
	name       string
	setup      func(t *testing.T) (flowy.Runner[tracingResumeRejectState, flowy.NoEffect], flowy.ResumeToken)
	wantReason string
}

type tracingResumeRejectState struct{}

func TestInstallLifecycleObserverWithTracingResumeRejectedMatrix(t *testing.T) {
	cases := []tracingResumeRejectCase{
		{name: "zero_revision", setup: tracingResumeRejectSetupZeroRevision, wantReason: "zero_revision"},
		{name: "handoff_pending", setup: tracingResumeRejectSetupHandoffPending, wantReason: "handoff_pending"},
		{name: "handoff_orphaned", setup: tracingResumeRejectSetupHandoffOrphaned, wantReason: "handoff_orphaned"},
		{name: "empty_token", setup: tracingResumeRejectSetupEmptyToken, wantReason: "empty_token"},
		{
			name: "invalid_handoff_status", setup: tracingResumeRejectSetupInvalidHandoffStatus,
			wantReason: "invalid_handoff_status",
		},
		{name: "invalid_snapshot", setup: tracingResumeRejectSetupInvalidSnapshot, wantReason: "invalid_snapshot"},
		{name: "invalid_pointer", setup: tracingResumeRejectSetupInvalidPointer, wantReason: "invalid_pointer"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assertTracingResumeRejected(t, tc.setup, tc.wantReason)
		})
	}
}

func assertTracingResumeRejected(
	t *testing.T,
	setup func(t *testing.T) (flowy.Runner[tracingResumeRejectState, flowy.NoEffect], flowy.ResumeToken),
	wantReason string,
) {
	t.Helper()
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

	runner, token := setup(t)
	_, _ = runner.Resume(context.Background(), token)

	spans := sr.Ended()
	if len(spans) != 1 || spans[0].Name() != "flowy.lifecycle.resume_rejected" {
		t.Fatalf("expected resume_rejected span, got %+v", spans)
	}
	if !hasResumeRejectedReason(spans[0].Attributes(), wantReason) {
		t.Fatalf("expected reason=%q, attrs=%+v", wantReason, spans[0].Attributes())
	}
}

func tracingResumeRejectWorkGraph(t *testing.T) *flowy.Graph[tracingResumeRejectState, flowy.NoEffect] {
	t.Helper()
	b := flowy.NewGraph[tracingResumeRejectState, flowy.NoEffect](
		func(_ tracingResumeRejectState, u tracingResumeRejectState) tracingResumeRejectState { return u },
	)
	b.AddNode("work", func(_ context.Context, s tracingResumeRejectState) (
		tracingResumeRejectState, flowy.Directive, error,
	) {
		return s, flowy.End(), nil
	})
	b.AllowNoOutgoingRoute("work")
	b.SetEntryPoint("work")
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	return g
}

func tracingResumeRejectSetupZeroRevision(
	t *testing.T,
) (flowy.Runner[tracingResumeRejectState, flowy.NoEffect], flowy.ResumeToken) {
	t.Helper()
	cp := testutil.NewMemoryCheckpointer[tracingResumeRejectState, flowy.NoEffect]()
	if _, err := cp.Save(context.Background(), 0, flowy.Snapshot[tracingResumeRejectState, flowy.NoEffect]{
		ThreadID: "trace-zero-rev-th", ExecutionPointer: "work", State: tracingResumeRejectState{},
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	return tracingResumeRejectWorkGraph(t).NewRunner(cp), flowy.ResumeToken{ThreadID: "trace-zero-rev-th"}
}

func tracingResumeRejectSetupHandoffPending(
	t *testing.T,
) (flowy.Runner[tracingResumeRejectState, flowy.NoEffect], flowy.ResumeToken) {
	t.Helper()
	cp := testutil.NewMemoryCheckpointer[tracingResumeRejectState, flowy.NoEffect]()
	if _, err := cp.Save(context.Background(), 0, flowy.Snapshot[tracingResumeRejectState, flowy.NoEffect]{
		ThreadID: "trace-pending-th", ExecutionPointer: "work", State: tracingResumeRejectState{},
		RunMeta: flowy.RunMetadata{
			HandoffStatus: flowy.HandoffStatusPending, HandoffPendingAt: time.Now().UTC(),
		},
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	_, rev, err := cp.Load(context.Background(), "trace-pending-th")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	return tracingResumeRejectWorkGraph(t).NewRunner(cp),
		flowy.ResumeToken{ThreadID: "trace-pending-th", SnapshotRevision: rev}
}

func tracingResumeRejectSetupHandoffOrphaned(
	t *testing.T,
) (flowy.Runner[tracingResumeRejectState, flowy.NoEffect], flowy.ResumeToken) {
	t.Helper()
	cp := testutil.NewMemoryCheckpointer[tracingResumeRejectState, flowy.NoEffect]()
	if _, err := cp.Save(context.Background(), 0, flowy.Snapshot[tracingResumeRejectState, flowy.NoEffect]{
		ThreadID: "trace-orphan-th", ExecutionPointer: "work", State: tracingResumeRejectState{},
		RunMeta: flowy.RunMetadata{HandoffStatus: flowy.HandoffStatusOrphaned},
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	_, rev, err := cp.Load(context.Background(), "trace-orphan-th")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	return tracingResumeRejectWorkGraph(t).NewRunner(cp),
		flowy.ResumeToken{ThreadID: "trace-orphan-th", SnapshotRevision: rev}
}

func tracingResumeRejectSetupEmptyToken(
	t *testing.T,
) (flowy.Runner[tracingResumeRejectState, flowy.NoEffect], flowy.ResumeToken) {
	t.Helper()
	return tracingResumeRejectWorkGraph(t).NewRunner(
		testutil.NewMemoryCheckpointer[tracingResumeRejectState, flowy.NoEffect](),
	), flowy.ResumeToken{}
}

func tracingResumeRejectSetupInvalidHandoffStatus(
	t *testing.T,
) (flowy.Runner[tracingResumeRejectState, flowy.NoEffect], flowy.ResumeToken) {
	t.Helper()
	cp := testutil.NewMemoryCheckpointer[tracingResumeRejectState, flowy.NoEffect]()
	if _, err := cp.Save(context.Background(), 0, flowy.Snapshot[tracingResumeRejectState, flowy.NoEffect]{
		ThreadID: "trace-invalid-hs-th", ExecutionPointer: "work", State: tracingResumeRejectState{},
		RunMeta: flowy.RunMetadata{HandoffStatus: flowy.HandoffStatus("corrupt")},
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	_, rev, err := cp.Load(context.Background(), "trace-invalid-hs-th")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	return tracingResumeRejectWorkGraph(t).NewRunner(cp),
		flowy.ResumeToken{ThreadID: "trace-invalid-hs-th", SnapshotRevision: rev}
}

func tracingResumeRejectSetupInvalidSnapshot(
	t *testing.T,
) (flowy.Runner[tracingResumeRejectState, flowy.NoEffect], flowy.ResumeToken) {
	t.Helper()
	cp := testutil.NewMemoryCheckpointer[tracingResumeRejectState, flowy.NoEffect]()
	if _, err := cp.Save(context.Background(), 0, flowy.Snapshot[tracingResumeRejectState, flowy.NoEffect]{
		ThreadID: "trace-invalid-snap-th", ExecutionPointer: "", State: tracingResumeRejectState{},
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	_, rev, err := cp.Load(context.Background(), "trace-invalid-snap-th")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	return tracingResumeRejectWorkGraph(t).NewRunner(cp),
		flowy.ResumeToken{ThreadID: "trace-invalid-snap-th", SnapshotRevision: rev}
}

func tracingResumeRejectSetupInvalidPointer(
	t *testing.T,
) (flowy.Runner[tracingResumeRejectState, flowy.NoEffect], flowy.ResumeToken) {
	t.Helper()
	cp := testutil.NewMemoryCheckpointer[tracingResumeRejectState, flowy.NoEffect]()
	if _, err := cp.Save(context.Background(), 0, flowy.Snapshot[tracingResumeRejectState, flowy.NoEffect]{
		ThreadID: "trace-bad-ptr-th", ExecutionPointer: "missing", State: tracingResumeRejectState{},
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	_, rev, err := cp.Load(context.Background(), "trace-bad-ptr-th")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	return tracingResumeRejectWorkGraph(t).NewRunner(cp),
		flowy.ResumeToken{ThreadID: "trace-bad-ptr-th", SnapshotRevision: rev}
}

func hasSpanStringAttr(attrs []attribute.KeyValue, key, want string) bool {
	for _, attr := range attrs {
		if string(attr.Key) == key && attr.Value.AsString() == want {
			return true
		}
	}
	return false
}
