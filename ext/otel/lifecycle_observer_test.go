package otel

import (
	"context"
	"errors"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/skosovsky/flowy"
	"github.com/skosovsky/flowy/testutil"
)

func TestInstallLifecycleObserver(t *testing.T) {
	if err := InstallLifecycleObserver(); err != nil {
		t.Fatalf("install: %v", err)
	}
	t.Cleanup(func() { flowy.SetLifecycleObserver(nil) })
}

func TestInstallLifecycleObserverReturnsNilOnSuccess(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	prev := otel.GetMeterProvider()
	otel.SetMeterProvider(provider)
	t.Cleanup(func() {
		otel.SetMeterProvider(prev)
		flowy.SetLifecycleObserver(nil)
	})
	if err := InstallLifecycleObserver(); err != nil {
		t.Fatalf("expected nil error with valid meter provider, got %v", err)
	}
}

func mustInstallLifecycleObserver(t *testing.T) {
	t.Helper()
	if err := InstallLifecycleObserver(); err != nil {
		t.Fatalf("install: %v", err)
	}
}

type testHandoffOutbox struct {
	err error
}

func (o *testHandoffOutbox) EnqueueIntent(_ context.Context, _ flowy.ResumeToken) error {
	return o.err
}

func datapointMatchesAttr(dp metricdata.DataPoint[int64], key, want string) bool {
	for _, attr := range dp.Attributes.ToSlice() {
		if string(attr.Key) == key && attr.Value.AsString() == want {
			return true
		}
	}
	return false
}

func counterAttributeValue(
	t *testing.T,
	reader *sdkmetric.ManualReader,
	name, key, want string,
) int64 {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("collect metrics: %v", err)
	}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != name {
				continue
			}
			sum, ok := m.Data.(metricdata.Sum[int64])
			if !ok {
				t.Fatalf("metric %q: expected Sum[int64], got %T", name, m.Data)
			}
			var total int64
			for _, dp := range sum.DataPoints {
				if datapointMatchesAttr(dp, key, want) {
					total += dp.Value
				}
			}
			return total
		}
	}
	return 0
}

func counterValue(t *testing.T, reader *sdkmetric.ManualReader, name string) int64 {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("collect metrics: %v", err)
	}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != name {
				continue
			}
			sum, ok := m.Data.(metricdata.Sum[int64])
			if !ok {
				t.Fatalf("metric %q: expected Sum[int64], got %T", name, m.Data)
			}
			var total int64
			for _, dp := range sum.DataPoints {
				total += dp.Value
			}
			return total
		}
	}
	return 0
}

func TestHandoffOrphanObservabilityContract(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	prev := otel.GetMeterProvider()
	otel.SetMeterProvider(provider)
	t.Cleanup(func() {
		otel.SetMeterProvider(prev)
		flowy.SetLifecycleObserver(nil)
	})
	mustInstallLifecycleObserver(t)

	type state struct{}
	cp := testutil.NewMemoryCheckpointer[state, flowy.NoEffect]()
	outbox := &testHandoffOutbox{err: errors.New("broker down")}
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

	handle, err := g.NewRunner(cp).Stream(context.Background(), "otel-orphan-contract-th", state{},
		flowy.WithHandoffOutbox[state, flowy.NoEffect](outbox),
	)
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
	events, waitErr := flowy.CollectEventsAndWait(context.Background(), handle)
	if !errors.Is(waitErr, flowy.ErrHandoffEnqueueFailed) {
		t.Fatalf("expected ErrHandoffEnqueueFailed, got %v", waitErr)
	}

	snap, _, loadErr := cp.Load(context.Background(), "otel-orphan-contract-th")
	if loadErr != nil {
		t.Fatalf("load: %v", loadErr)
	}
	if snap.RunMeta.HandoffStatus != flowy.HandoffStatusOrphaned {
		t.Fatalf("expected orphaned status, got %q", snap.RunMeta.HandoffStatus)
	}
	if got := counterAttributeValue(
		t, reader, "flowy.handoff_enqueued_total", "status", "enqueue_failed",
	); got != 1 {
		t.Fatalf("expected handoff_enqueued_total status=enqueue_failed=1, got %d", got)
	}

	foundHandoff := false
	for _, ev := range events {
		if ev.Type == flowy.EventHandoff {
			foundHandoff = true
			if ev.Reason != flowy.ReasonHandoffOrphaned {
				t.Fatalf("expected EventHandoff reason %q, got %q", flowy.ReasonHandoffOrphaned, ev.Reason)
			}
		}
	}
	if !foundHandoff {
		t.Fatalf("expected EventHandoff on orphan path, got %+v", events)
	}

	syncRes, syncErr := g.NewRunner(cp).Start(context.Background(), "otel-orphan-contract-sync-th", state{},
		flowy.WithHandoffOutbox[state, flowy.NoEffect](outbox),
	)
	if !errors.Is(syncErr, flowy.ErrHandoffEnqueueFailed) {
		t.Fatalf("expected sync ErrHandoffEnqueueFailed, got %v", syncErr)
	}
	if syncRes.Reason != flowy.ReasonHandoffOrphaned {
		t.Fatalf("expected sync reason %q, got %q", flowy.ReasonHandoffOrphaned, syncRes.Reason)
	}
}

func TestStreamHandoffOrphanLifecycleMetric(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	prev := otel.GetMeterProvider()
	otel.SetMeterProvider(provider)
	t.Cleanup(func() {
		otel.SetMeterProvider(prev)
		flowy.SetLifecycleObserver(nil)
	})
	mustInstallLifecycleObserver(t)

	type state struct{}
	cp := testutil.NewMemoryCheckpointer[state, flowy.NoEffect]()
	outbox := &testHandoffOutbox{err: errors.New("broker down")}
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

	handle, err := g.NewRunner(cp).Stream(context.Background(), "otel-stream-orphan-th", state{},
		flowy.WithHandoffOutbox[state, flowy.NoEffect](outbox),
	)
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
	events, waitErr := flowy.CollectEventsAndWait(context.Background(), handle)
	if !errors.Is(waitErr, flowy.ErrHandoffEnqueueFailed) {
		t.Fatalf("expected ErrHandoffEnqueueFailed, got %v", waitErr)
	}
	if got := counterAttributeValue(
		t, reader, "flowy.handoff_enqueued_total", "status", "enqueue_failed",
	); got != 1 {
		t.Fatalf("expected handoff_enqueued_total status=enqueue_failed=1, got %d", got)
	}

	snap, _, loadErr := cp.Load(context.Background(), "otel-stream-orphan-th")
	if loadErr != nil {
		t.Fatalf("load: %v", loadErr)
	}
	if snap.RunMeta.HandoffStatus != flowy.HandoffStatusOrphaned {
		t.Fatalf("expected orphaned status, got %q", snap.RunMeta.HandoffStatus)
	}
	foundHandoff := false
	for _, ev := range events {
		if ev.Type == flowy.EventHandoff {
			foundHandoff = true
			if ev.Reason != flowy.ReasonHandoffOrphaned {
				t.Fatalf("expected EventHandoff reason %q, got %q", flowy.ReasonHandoffOrphaned, ev.Reason)
			}
		}
	}
	if !foundHandoff {
		t.Fatalf("expected EventHandoff on orphan path, got %+v", events)
	}

	syncRes, syncErr := g.NewRunner(cp).Start(context.Background(), "otel-stream-orphan-sync-th", state{},
		flowy.WithHandoffOutbox[state, flowy.NoEffect](outbox),
	)
	if !errors.Is(syncErr, flowy.ErrHandoffEnqueueFailed) {
		t.Fatalf("expected sync ErrHandoffEnqueueFailed, got %v", syncErr)
	}
	if syncRes.Reason != flowy.ReasonHandoffOrphaned {
		t.Fatalf("expected sync reason %q, got %q", flowy.ReasonHandoffOrphaned, syncRes.Reason)
	}
}

func TestLifecycleObserverHandoffEnqueuedCounter(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	prev := otel.GetMeterProvider()
	otel.SetMeterProvider(provider)
	t.Cleanup(func() {
		otel.SetMeterProvider(prev)
		flowy.SetLifecycleObserver(nil)
	})

	mustInstallLifecycleObserver(t)

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

	_, _ = g.NewRunner(newCP[state, flowy.NoEffect]{}).Start(context.Background(), "otel-handoff-th", state{},
		flowy.WithHandoffOutbox[state, flowy.NoEffect](&testHandoffOutbox{err: errors.New("down")}),
	)

	if got := counterValue(t, reader, "flowy.handoff_enqueued_total"); got != 1 {
		t.Fatalf("expected handoff_enqueued_total=1, got %d", got)
	}
	if got := counterAttributeValue(t, reader, "flowy.handoff_enqueued_total", "status", "enqueue_failed"); got != 1 {
		t.Fatalf("expected handoff_enqueued_total status=enqueue_failed=1, got %d", got)
	}
}

func TestLifecycleObserverHandoffEnqueuedSuccess(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	prev := otel.GetMeterProvider()
	otel.SetMeterProvider(provider)
	t.Cleanup(func() {
		otel.SetMeterProvider(prev)
		flowy.SetLifecycleObserver(nil)
	})

	mustInstallLifecycleObserver(t)

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

	_, _ = g.NewRunner(newCP[state, flowy.NoEffect]{}).Start(context.Background(), "otel-handoff-ok-th", state{},
		flowy.WithHandoffOutbox[state, flowy.NoEffect](&testHandoffOutbox{}),
	)

	if got := counterAttributeValue(t, reader, "flowy.handoff_enqueued_total", "status", "success"); got != 1 {
		t.Fatalf("expected handoff_enqueued_total status=success=1, got %d", got)
	}
}

func TestLifecycleObserverResumeRejectedStaleToken(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	prev := otel.GetMeterProvider()
	otel.SetMeterProvider(provider)
	t.Cleanup(func() {
		otel.SetMeterProvider(prev)
		flowy.SetLifecycleObserver(nil)
	})

	mustInstallLifecycleObserver(t)

	type state struct{ N int }
	cp := testutil.NewMemoryCheckpointer[state, flowy.NoEffect]()
	b := flowy.NewGraph[state, flowy.NoEffect](func(_ state, u state) state { return u })
	b.AddNode("step", func(_ context.Context, s state) (state, flowy.Directive, error) {
		s.N++
		if s.N < 3 {
			return s, flowy.Suspend("more"), nil
		}
		return s, flowy.End(), nil
	})
	b.AllowNoOutgoingRoute("step")
	b.SetEntryPoint("step")
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	runner := g.NewRunner(cp)
	first, err := runner.Start(context.Background(), "otel-stale-th", state{})
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	staleToken := first.ResumeToken
	if _, err := runner.Resume(context.Background(), staleToken); err != nil {
		t.Fatalf("resume: %v", err)
	}
	_, _ = runner.Resume(context.Background(), staleToken)

	if got := counterAttributeValue(t, reader, "flowy.resume_rejected_total", "reason", "stale_token"); got != 1 {
		t.Fatalf("expected resume_rejected_total reason=stale_token=1, got %d", got)
	}
}

func TestLifecycleObserverResumeRejectedZeroRevision(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	prev := otel.GetMeterProvider()
	otel.SetMeterProvider(provider)
	t.Cleanup(func() {
		otel.SetMeterProvider(prev)
		flowy.SetLifecycleObserver(nil)
	})

	mustInstallLifecycleObserver(t)

	type state struct{}
	cp := testutil.NewMemoryCheckpointer[state, flowy.NoEffect]()
	if _, err := cp.Save(context.Background(), 0, flowy.Snapshot[state, flowy.NoEffect]{
		ThreadID:         "otel-zero-rev-th",
		ExecutionPointer: "work",
		State:            state{},
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	b := flowy.NewGraph[state, flowy.NoEffect](func(_ state, u state) state { return u })
	b.AddNode("work", func(_ context.Context, s state) (state, flowy.Directive, error) {
		return s, flowy.End(), nil
	})
	b.AllowNoOutgoingRoute("work")
	b.SetEntryPoint("work")
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	_, _ = g.NewRunner(cp).Resume(context.Background(), flowy.ResumeToken{
		ThreadID: "otel-zero-rev-th",
	})

	if got := counterAttributeValue(t, reader, "flowy.resume_rejected_total", "reason", "zero_revision"); got != 1 {
		t.Fatalf("expected resume_rejected_total reason=zero_revision=1, got %d", got)
	}
}

func TestLifecycleObserverResumeRejectedCounter(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	prev := otel.GetMeterProvider()
	otel.SetMeterProvider(provider)
	t.Cleanup(func() {
		otel.SetMeterProvider(prev)
		flowy.SetLifecycleObserver(nil)
	})

	mustInstallLifecycleObserver(t)

	type state struct{}
	cp := testutil.NewMemoryCheckpointer[state, flowy.NoEffect]()
	if _, err := cp.Save(context.Background(), 0, flowy.Snapshot[state, flowy.NoEffect]{
		ThreadID:         "otel-resume-th",
		ExecutionPointer: "work",
		State:            state{},
		RunMeta: flowy.RunMetadata{
			HandoffStatus:    flowy.HandoffStatusPending,
			HandoffPendingAt: time.Now().UTC(),
		},
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	b := flowy.NewGraph[state, flowy.NoEffect](func(_ state, u state) state { return u })
	b.AddNode("work", func(_ context.Context, s state) (state, flowy.Directive, error) {
		return s, flowy.End(), nil
	})
	b.AllowNoOutgoingRoute("work")
	b.SetEntryPoint("work")
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	_, rev, err := cp.Load(context.Background(), "otel-resume-th")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	_, _ = g.NewRunner(cp).Resume(context.Background(), flowy.ResumeToken{
		ThreadID:         "otel-resume-th",
		SnapshotRevision: rev,
	})

	if got := counterValue(t, reader, "flowy.resume_rejected_total"); got != 1 {
		t.Fatalf("expected resume_rejected_total=1, got %d", got)
	}
	if got := counterAttributeValue(t, reader, "flowy.resume_rejected_total", "reason", "handoff_pending"); got != 1 {
		t.Fatalf("expected resume_rejected_total reason=handoff_pending=1, got %d", got)
	}
}

func TestLifecycleObserverCheckpointSoftErrorCounter(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	prev := otel.GetMeterProvider()
	otel.SetMeterProvider(provider)
	t.Cleanup(func() {
		otel.SetMeterProvider(prev)
		flowy.SetLifecycleObserver(nil)
	})

	mustInstallLifecycleObserver(t)

	type state struct{}
	cp := &saveFailCP[state, flowy.NoEffect]{inner: testutil.NewMemoryCheckpointer[state, flowy.NoEffect]()}
	b := flowy.NewGraph[state, flowy.NoEffect](func(_ state, u state) state { return u })
	b.AddNode("work", func(_ context.Context, s state) (state, flowy.Directive, error) {
		return s, flowy.Suspend("hold"), nil
	})
	b.AllowNoOutgoingRoute("work")
	b.SetEntryPoint("work")
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	_, _ = g.NewRunner(cp).Start(context.Background(), "otel-soft-error-th", state{},
		flowy.WithCheckpointErrorPolicy[state, flowy.NoEffect](flowy.CheckpointPolicySkipOnSaveError),
	)

	if got := counterValue(t, reader, "flowy.checkpoint_soft_error_total"); got != 1 {
		t.Fatalf("expected checkpoint_soft_error_total=1, got %d", got)
	}
	if got := counterAttributeValue(
		t, reader, "flowy.checkpoint_soft_error_total", "thread_id", "otel-soft-error-th",
	); got != 1 {
		t.Fatalf("expected checkpoint_soft_error thread_id=otel-soft-error-th, got %d", got)
	}
	if got := counterAttributeValue(
		t, reader, "flowy.checkpoint_soft_error_total", "node", "work",
	); got != 1 {
		t.Fatalf("expected checkpoint_soft_error node=work, got %d", got)
	}
}

func TestLifecycleObserverSkipOnSaveErrorHandoff(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	prev := otel.GetMeterProvider()
	otel.SetMeterProvider(provider)
	t.Cleanup(func() {
		otel.SetMeterProvider(prev)
		flowy.SetLifecycleObserver(nil)
	})

	mustInstallLifecycleObserver(t)

	type state struct{}
	cp := &saveFailCP[state, flowy.NoEffect]{inner: testutil.NewMemoryCheckpointer[state, flowy.NoEffect]()}
	outbox := &testHandoffOutbox{}
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

	_, _ = g.NewRunner(cp).Start(context.Background(), "otel-skip-handoff-th", state{},
		flowy.WithCheckpointErrorPolicy[state, flowy.NoEffect](flowy.CheckpointPolicySkipOnSaveError),
		flowy.WithHandoffOutbox[state, flowy.NoEffect](outbox),
	)

	if got := counterValue(t, reader, "flowy.checkpoint_soft_error_total"); got != 1 {
		t.Fatalf("expected checkpoint_soft_error_total=1, got %d", got)
	}
	if got := counterAttributeValue(
		t,
		reader,
		"flowy.checkpoint_soft_error_total",
		"thread_id",
		"otel-skip-handoff-th",
	); got != 1 {
		t.Fatalf("expected checkpoint_soft_error thread_id, got %d", got)
	}
	if got := counterValue(t, reader, "flowy.handoff_enqueued_total"); got != 0 {
		t.Fatalf("expected handoff_enqueued_total=0 on skip-on-save, got %d", got)
	}
}

func TestLifecycleObserverSkipOnSaveErrorHTB(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	prev := otel.GetMeterProvider()
	otel.SetMeterProvider(provider)
	t.Cleanup(func() {
		otel.SetMeterProvider(prev)
		flowy.SetLifecycleObserver(nil)
	})

	mustInstallLifecycleObserver(t)

	type state struct{}
	cp := &saveFailCP[state, flowy.NoEffect]{inner: testutil.NewMemoryCheckpointer[state, flowy.NoEffect]()}
	outbox := &testHandoffOutbox{}
	ready := make(chan struct{})
	b := flowy.NewGraph[state, flowy.NoEffect](func(_ state, u state) state { return u })
	b.AddNode("work", func(ctx context.Context, s state) (state, flowy.Directive, error) {
		close(ready)
		<-ctx.Done()
		return s, flowy.Completed(), nil
	})
	b.AllowNoOutgoingRoute("work")
	b.SetEntryPoint("work")
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	runner := g.NewRunner(cp)
	startDone := make(chan error, 1)
	go func() {
		_, runErr := runner.Start(context.Background(), "otel-skip-htb-th", state{},
			flowy.WithCheckpointErrorPolicy[state, flowy.NoEffect](flowy.CheckpointPolicySkipOnSaveError),
			flowy.WithHandoffOutbox[state, flowy.NoEffect](outbox),
		)
		startDone <- runErr
	}()
	<-ready
	_ = runner.RequestLocalHandoff(context.Background(), "otel-skip-htb-th")
	<-startDone

	if got := counterValue(t, reader, "flowy.checkpoint_soft_error_total"); got != 1 {
		t.Fatalf("expected checkpoint_soft_error_total=1, got %d", got)
	}
	if got := counterAttributeValue(
		t,
		reader,
		"flowy.checkpoint_soft_error_total",
		"thread_id",
		"otel-skip-htb-th",
	); got != 1 {
		t.Fatalf("expected checkpoint_soft_error thread_id, got %d", got)
	}
	if got := counterValue(t, reader, "flowy.handoff_enqueued_total"); got != 0 {
		t.Fatalf("expected handoff_enqueued_total=0 on skip-on-save HTB, got %d", got)
	}
}

func TestLifecycleObserverResumeRejectedHandoffOrphaned(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	prev := otel.GetMeterProvider()
	otel.SetMeterProvider(provider)
	t.Cleanup(func() {
		otel.SetMeterProvider(prev)
		flowy.SetLifecycleObserver(nil)
	})

	mustInstallLifecycleObserver(t)

	type state struct{}
	cp := testutil.NewMemoryCheckpointer[state, flowy.NoEffect]()
	if _, err := cp.Save(context.Background(), 0, flowy.Snapshot[state, flowy.NoEffect]{
		ThreadID:         "otel-orphan-th",
		ExecutionPointer: "work",
		State:            state{},
		RunMeta: flowy.RunMetadata{
			HandoffStatus: flowy.HandoffStatusOrphaned,
		},
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	b := flowy.NewGraph[state, flowy.NoEffect](func(_ state, u state) state { return u })
	b.AddNode("work", func(_ context.Context, s state) (state, flowy.Directive, error) {
		return s, flowy.End(), nil
	})
	b.AllowNoOutgoingRoute("work")
	b.SetEntryPoint("work")
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	_, rev, err := cp.Load(context.Background(), "otel-orphan-th")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	_, _ = g.NewRunner(cp).Resume(context.Background(), flowy.ResumeToken{
		ThreadID:         "otel-orphan-th",
		SnapshotRevision: rev,
	})

	if got := counterAttributeValue(t, reader, "flowy.resume_rejected_total", "reason", "handoff_orphaned"); got != 1 {
		t.Fatalf("expected resume_rejected_total reason=handoff_orphaned=1, got %d", got)
	}
}

type saveFailCP[T, E any] struct {
	inner flowy.Checkpointer[T, E]
}

func (s *saveFailCP[T, E]) Save(context.Context, uint64, flowy.Snapshot[T, E]) (uint64, error) {
	return 0, errors.New("save failed")
}

func (s *saveFailCP[T, E]) Load(ctx context.Context, threadID string) (flowy.Snapshot[T, E], uint64, error) {
	return s.inner.Load(ctx, threadID)
}

func (s *saveFailCP[T, E]) GetHistory(ctx context.Context, threadID string, limit int) ([]flowy.Snapshot[T, E], error) {
	return s.inner.GetHistory(ctx, threadID, limit)
}

func (s *saveFailCP[T, E]) Prune(ctx context.Context, threadID string, retainCount int) error {
	return s.inner.Prune(ctx, threadID, retainCount)
}

func (s *saveFailCP[T, E]) Delete(ctx context.Context, threadID string) error {
	return s.inner.Delete(ctx, threadID)
}

func (s *saveFailCP[T, E]) DeleteIfIdle(ctx context.Context, threadID string) error {
	return s.inner.DeleteIfIdle(ctx, threadID)
}

func TestLifecycleObserverHandoffPatchOrphanFailed(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	prev := otel.GetMeterProvider()
	otel.SetMeterProvider(provider)
	t.Cleanup(func() {
		otel.SetMeterProvider(prev)
		flowy.SetLifecycleObserver(nil)
	})
	mustInstallLifecycleObserver(t)

	type state struct{}
	cp := &otelHandoffPatchFailCP[state, flowy.NoEffect]{
		failOnStatuses: map[flowy.HandoffStatus]struct{}{ //nolint:exhaustive // patch statuses under test
			flowy.HandoffStatusEnqueued: {},
			flowy.HandoffStatusOrphaned: {},
		},
	}
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
	_, _ = g.NewRunner(cp).Start(context.Background(), "otel-patch-orphan-th", state{},
		flowy.WithHandoffOutbox[state, flowy.NoEffect](&testHandoffOutbox{}),
	)
	if got := counterAttributeValue(
		t, reader, "flowy.handoff_enqueued_total", "status", "patch_orphan_failed",
	); got != 1 {
		t.Fatalf("expected patch_orphan_failed=1, got %d", got)
	}
}

func TestLifecycleObserverHandoffPatchEnqueuedFailed(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	prev := otel.GetMeterProvider()
	otel.SetMeterProvider(provider)
	t.Cleanup(func() {
		otel.SetMeterProvider(prev)
		flowy.SetLifecycleObserver(nil)
	})
	mustInstallLifecycleObserver(t)

	type state struct{}
	cp := &otelHandoffPatchFailCP[state, flowy.NoEffect]{failOn: flowy.HandoffStatusEnqueued}
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
	_, _ = g.NewRunner(cp).Start(context.Background(), "otel-patch-enq-th", state{},
		flowy.WithHandoffOutbox[state, flowy.NoEffect](&testHandoffOutbox{}),
	)
	if got := counterAttributeValue(
		t, reader, "flowy.handoff_enqueued_total", "status", "patch_enqueued_failed",
	); got != 1 {
		t.Fatalf("expected patch_enqueued_failed=1, got %d", got)
	}
}

func TestLifecycleObserverResumeRejectedEmptyToken(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	prev := otel.GetMeterProvider()
	otel.SetMeterProvider(provider)
	t.Cleanup(func() {
		otel.SetMeterProvider(prev)
		flowy.SetLifecycleObserver(nil)
	})
	mustInstallLifecycleObserver(t)

	type state struct{}
	g, err := flowy.NewGraph[state, flowy.NoEffect](func(_ state, u state) state { return u }).
		AddNode("n", func(_ context.Context, s state) (state, flowy.Directive, error) { return s, flowy.End(), nil }).
		SetEntryPoint("n").AllowNoOutgoingRoute("n").Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	cp := testutil.NewMemoryCheckpointer[state, flowy.NoEffect]()
	_, _ = g.NewRunner(cp).Resume(context.Background(), flowy.ResumeToken{})
	if got := counterAttributeValue(
		t, reader, "flowy.resume_rejected_total", "reason", "empty_token",
	); got != 1 {
		t.Fatalf("expected empty_token=1, got %d", got)
	}
}

func TestLifecycleObserverResumeRejectedInvalidHandoffStatus(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	prev := otel.GetMeterProvider()
	otel.SetMeterProvider(provider)
	t.Cleanup(func() {
		otel.SetMeterProvider(prev)
		flowy.SetLifecycleObserver(nil)
	})
	mustInstallLifecycleObserver(t)

	type state struct{}
	cp := testutil.NewMemoryCheckpointer[state, flowy.NoEffect]()
	if _, err := cp.Save(context.Background(), 0, flowy.Snapshot[state, flowy.NoEffect]{
		ThreadID: "otel-invalid-hs-th", ExecutionPointer: "work", State: state{},
		RunMeta: flowy.RunMetadata{HandoffStatus: flowy.HandoffStatus("corrupt")},
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	b := flowy.NewGraph[state, flowy.NoEffect](func(_ state, u state) state { return u })
	b.AddNode("work", func(_ context.Context, s state) (state, flowy.Directive, error) { return s, flowy.End(), nil })
	b.AllowNoOutgoingRoute("work").SetEntryPoint("work")
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	_, rev, _ := cp.Load(context.Background(), "otel-invalid-hs-th")
	_, _ = g.NewRunner(cp).Resume(context.Background(), flowy.ResumeToken{
		ThreadID: "otel-invalid-hs-th", SnapshotRevision: rev,
	})
	if got := counterAttributeValue(
		t, reader, "flowy.resume_rejected_total", "reason", "invalid_handoff_status",
	); got != 1 {
		t.Fatalf("expected invalid_handoff_status=1, got %d", got)
	}
}

func TestLifecycleObserverResumeRejectedInvalidSnapshot(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	prev := otel.GetMeterProvider()
	otel.SetMeterProvider(provider)
	t.Cleanup(func() {
		otel.SetMeterProvider(prev)
		flowy.SetLifecycleObserver(nil)
	})
	mustInstallLifecycleObserver(t)

	type state struct{}
	cp := testutil.NewMemoryCheckpointer[state, flowy.NoEffect]()
	if _, err := cp.Save(context.Background(), 0, flowy.Snapshot[state, flowy.NoEffect]{
		ThreadID: "otel-invalid-snap-th", ExecutionPointer: "", State: state{},
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	b := flowy.NewGraph[state, flowy.NoEffect](func(_ state, u state) state { return u })
	b.AddNode("work", func(_ context.Context, s state) (state, flowy.Directive, error) { return s, flowy.End(), nil })
	b.AllowNoOutgoingRoute("work").SetEntryPoint("work")
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	_, rev, _ := cp.Load(context.Background(), "otel-invalid-snap-th")
	_, _ = g.NewRunner(cp).Resume(context.Background(), flowy.ResumeToken{
		ThreadID: "otel-invalid-snap-th", SnapshotRevision: rev,
	})
	if got := counterAttributeValue(
		t, reader, "flowy.resume_rejected_total", "reason", "invalid_snapshot",
	); got != 1 {
		t.Fatalf("expected invalid_snapshot=1, got %d", got)
	}
}

func TestLifecycleObserverResumeRejectedInvalidPointer(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	prev := otel.GetMeterProvider()
	otel.SetMeterProvider(provider)
	t.Cleanup(func() {
		otel.SetMeterProvider(prev)
		flowy.SetLifecycleObserver(nil)
	})
	mustInstallLifecycleObserver(t)

	type state struct{}
	cp := testutil.NewMemoryCheckpointer[state, flowy.NoEffect]()
	if _, err := cp.Save(context.Background(), 0, flowy.Snapshot[state, flowy.NoEffect]{
		ThreadID: "otel-bad-ptr-th", ExecutionPointer: "missing", State: state{},
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	b := flowy.NewGraph[state, flowy.NoEffect](func(_ state, u state) state { return u })
	b.AddNode("work", func(_ context.Context, s state) (state, flowy.Directive, error) { return s, flowy.End(), nil })
	b.AllowNoOutgoingRoute("work").SetEntryPoint("work")
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	_, rev, _ := cp.Load(context.Background(), "otel-bad-ptr-th")
	_, _ = g.NewRunner(cp).Resume(context.Background(), flowy.ResumeToken{
		ThreadID: "otel-bad-ptr-th", SnapshotRevision: rev,
	})
	if got := counterAttributeValue(
		t, reader, "flowy.resume_rejected_total", "reason", "invalid_pointer",
	); got != 1 {
		t.Fatalf("expected invalid_pointer=1, got %d", got)
	}
}

func TestLifecycleObserverHandoffSaveFailed(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	prev := otel.GetMeterProvider()
	otel.SetMeterProvider(provider)
	t.Cleanup(func() {
		otel.SetMeterProvider(prev)
		flowy.SetLifecycleObserver(nil)
	})
	mustInstallLifecycleObserver(t)

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
	_, _ = g.NewRunner(cp).Start(context.Background(), "otel-tx-save-fail-th", state{},
		flowy.WithHandoffOutbox[state, flowy.NoEffect](&testHandoffOutbox{}),
	)
	if got := counterAttributeValue(
		t, reader, "flowy.handoff_enqueued_total", "status", "save_failed",
	); got != 1 {
		t.Fatalf("expected save_failed=1, got %d", got)
	}
}

func TestLifecycleObserverHandoffCommitFailed(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	prev := otel.GetMeterProvider()
	otel.SetMeterProvider(provider)
	t.Cleanup(func() {
		otel.SetMeterProvider(prev)
		flowy.SetLifecycleObserver(nil)
	})
	mustInstallLifecycleObserver(t)

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
	_, _ = g.NewRunner(cp).Start(context.Background(), "otel-tx-commit-fail-th", state{},
		flowy.WithHandoffOutbox[state, flowy.NoEffect](&testHandoffOutbox{}),
	)
	if got := counterAttributeValue(
		t, reader, "flowy.handoff_enqueued_total", "status", "commit_failed",
	); got != 1 {
		t.Fatalf("expected commit_failed=1, got %d", got)
	}
}

func TestLifecycleObserverRecoverStaleHandoffFromOrphaned(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	prev := otel.GetMeterProvider()
	otel.SetMeterProvider(provider)
	t.Cleanup(func() {
		otel.SetMeterProvider(prev)
		flowy.SetLifecycleObserver(nil)
	})
	mustInstallLifecycleObserver(t)

	type state struct{}
	cp := testutil.NewMemoryCheckpointer[state, flowy.NoEffect]()
	now := time.Now().UTC().Add(-time.Hour)
	if _, err := cp.Save(context.Background(), 0, flowy.Snapshot[state, flowy.NoEffect]{
		ThreadID: "otel-recover-orphan-th", ExecutionPointer: "work", State: state{},
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
	if recoverErr := runner.RecoverStaleHandoff(context.Background(), "otel-recover-orphan-th"); recoverErr != nil {
		t.Fatalf("recover: %v", recoverErr)
	}
	if got := counterAttributeValue(
		t, reader, "flowy.handoff_enqueued_total", "status", "success",
	); got != 1 {
		t.Fatalf("expected handoff_enqueued_total status=success=1, got %d", got)
	}
}

func TestLifecycleObserverRecoverStaleHandoffStalePending(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	prev := otel.GetMeterProvider()
	otel.SetMeterProvider(provider)
	t.Cleanup(func() {
		otel.SetMeterProvider(prev)
		flowy.SetLifecycleObserver(nil)
	})
	mustInstallLifecycleObserver(t)

	type state struct{}
	cp := testutil.NewMemoryCheckpointer[state, flowy.NoEffect]()
	staleAt := time.Now().UTC().Add(-10 * time.Minute)
	if _, err := cp.Save(context.Background(), 0, flowy.Snapshot[state, flowy.NoEffect]{
		ThreadID: "otel-recover-stale-th", ExecutionPointer: "work", State: state{},
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
	if recoverErr := runner.RecoverStaleHandoff(context.Background(), "otel-recover-stale-th"); recoverErr != nil {
		t.Fatalf("recover: %v", recoverErr)
	}
	if got := counterAttributeValue(
		t, reader, "flowy.handoff_enqueued_total", "status", "success",
	); got != 1 {
		t.Fatalf("expected handoff_enqueued_total status=success=1, got %d", got)
	}
}

func TestLifecycleObserverRecoverStaleHandoffFreshPending(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	prev := otel.GetMeterProvider()
	otel.SetMeterProvider(provider)
	t.Cleanup(func() {
		otel.SetMeterProvider(prev)
		flowy.SetLifecycleObserver(nil)
	})
	mustInstallLifecycleObserver(t)

	type state struct{}
	cp := testutil.NewMemoryCheckpointer[state, flowy.NoEffect]()
	if _, err := cp.Save(context.Background(), 0, flowy.Snapshot[state, flowy.NoEffect]{
		ThreadID: "otel-recover-pending-th", ExecutionPointer: "work", State: state{},
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
	_ = runner.RecoverStaleHandoff(context.Background(), "otel-recover-pending-th")
	if got := counterAttributeValue(
		t, reader, "flowy.resume_rejected_total", "reason", "handoff_pending",
	); got != 1 {
		t.Fatalf("expected handoff_pending from recovery=1, got %d", got)
	}
}

type otelTxErrCP[T, E any] struct {
	inner flowy.Checkpointer[T, E]
	err   error
}

func (c *otelTxErrCP[T, E]) ensureInner() {
	if c.inner == nil {
		c.inner = testutil.NewMemoryCheckpointer[T, E]()
	}
}

func (c *otelTxErrCP[T, E]) Save(
	ctx context.Context, expectedRevision uint64, snapshot flowy.Snapshot[T, E],
) (uint64, error) {
	c.ensureInner()
	return c.inner.Save(ctx, expectedRevision, snapshot)
}

func (c *otelTxErrCP[T, E]) Load(
	ctx context.Context, threadID string,
) (flowy.Snapshot[T, E], uint64, error) {
	c.ensureInner()
	return c.inner.Load(ctx, threadID)
}

func (c *otelTxErrCP[T, E]) GetHistory(
	ctx context.Context, threadID string, limit int,
) ([]flowy.Snapshot[T, E], error) {
	c.ensureInner()
	return c.inner.GetHistory(ctx, threadID, limit)
}

func (c *otelTxErrCP[T, E]) Prune(ctx context.Context, threadID string, retainCount int) error {
	c.ensureInner()
	return c.inner.Prune(ctx, threadID, retainCount)
}

func (c *otelTxErrCP[T, E]) Delete(ctx context.Context, threadID string) error {
	c.ensureInner()
	return c.inner.Delete(ctx, threadID)
}

func (c *otelTxErrCP[T, E]) DeleteIfIdle(ctx context.Context, threadID string) error {
	c.ensureInner()
	return c.inner.DeleteIfIdle(ctx, threadID)
}

func (c *otelTxErrCP[T, E]) SaveWithOutbox(
	_ context.Context, _ uint64, _ flowy.Snapshot[T, E],
	_ func(context.Context) error,
) (uint64, error) {
	return 0, c.err
}

type otelHandoffPatchFailCP[T, E any] struct {
	inner          flowy.Checkpointer[T, E]
	failOn         flowy.HandoffStatus
	failOnStatuses map[flowy.HandoffStatus]struct{}
}

func (c *otelHandoffPatchFailCP[T, E]) ensureInner() {
	if c.inner == nil {
		c.inner = testutil.NewMemoryCheckpointer[T, E]()
	}
}

func (c *otelHandoffPatchFailCP[T, E]) Save(
	_ context.Context, expectedRevision uint64, snapshot flowy.Snapshot[T, E],
) (uint64, error) {
	if c.failOn != "" && snapshot.RunMeta.HandoffStatus == c.failOn {
		return 0, errors.New("patch save failed")
	}
	if len(c.failOnStatuses) > 0 {
		if _, ok := c.failOnStatuses[snapshot.RunMeta.HandoffStatus]; ok {
			return 0, errors.New("patch save failed")
		}
	}
	c.ensureInner()
	return c.inner.Save(context.Background(), expectedRevision, snapshot)
}

func (c *otelHandoffPatchFailCP[T, E]) Load(
	ctx context.Context, threadID string,
) (flowy.Snapshot[T, E], uint64, error) {
	c.ensureInner()
	return c.inner.Load(ctx, threadID)
}

func (c *otelHandoffPatchFailCP[T, E]) GetHistory(
	ctx context.Context, threadID string, limit int,
) ([]flowy.Snapshot[T, E], error) {
	c.ensureInner()
	return c.inner.GetHistory(ctx, threadID, limit)
}

func (c *otelHandoffPatchFailCP[T, E]) Prune(ctx context.Context, threadID string, retainCount int) error {
	c.ensureInner()
	return c.inner.Prune(ctx, threadID, retainCount)
}

func (c *otelHandoffPatchFailCP[T, E]) Delete(ctx context.Context, threadID string) error {
	c.ensureInner()
	return c.inner.Delete(ctx, threadID)
}

func (c *otelHandoffPatchFailCP[T, E]) DeleteIfIdle(ctx context.Context, threadID string) error {
	c.ensureInner()
	return c.inner.DeleteIfIdle(ctx, threadID)
}
