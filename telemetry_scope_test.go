package flowy

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
)

type scopedObserver struct{ calls atomic.Int32 }

func (o *scopedObserver) ObserveLifecycle(context.Context, LifecycleObservation) { o.calls.Add(1) }

type scopedBridge struct{ carrier map[string]string }
type restoredScopeKey struct{}

func (b *scopedBridge) Capture(context.Context) map[string]string { return b.carrier }
func (*scopedBridge) Restore(ctx context.Context, carrier map[string]string) context.Context {
	value := carrier["scope"]
	carrier["scope"] = "callback-owned"
	return context.WithValue(ctx, restoredScopeKey{}, value)
}

func TestContextObservationIsolationAndExplicitDisable(t *testing.T) {
	// Arrange: scoped instances take priority over an explicit global default.
	global, first, second := &scopedObserver{}, &scopedObserver{}, &scopedObserver{}
	SetLifecycleObserver(global)
	t.Cleanup(func() { SetLifecycleObserver(nil) })
	ctxA := WithLifecycleObserver(context.Background(), first)
	ctxB := WithLifecycleObserver(context.Background(), second)
	var disabled *scopedObserver
	ctxDisabled := WithLifecycleObserver(context.Background(), disabled)
	var workers sync.WaitGroup
	// Act: immutable scopes route concurrent observations to their own instance.
	for range 20 {
		workers.Go(func() { observeLifecycle(ctxA, LifecycleObservation{}) })
		workers.Go(func() { observeLifecycle(ctxB, LifecycleObservation{}) })
		workers.Go(func() { observeLifecycle(ctxDisabled, LifecycleObservation{}) })
	}
	workers.Wait()
	observeLifecycle(context.Background(), LifecycleObservation{})
	// Assert: disabling a scope never falls back to the process default.
	if first.calls.Load() != 20 || second.calls.Load() != 20 || global.calls.Load() != 1 {
		t.Fatalf("first=%d second=%d global=%d", first.calls.Load(), second.calls.Load(), global.calls.Load())
	}
}

func TestContextBridgeIsolationDetachedCarriersAndDisable(t *testing.T) {
	// Arrange: global and scoped bridges have distinct host-owned carrier maps.
	global := &scopedBridge{carrier: map[string]string{"scope": "global"}}
	first := &scopedBridge{carrier: map[string]string{"scope": "first"}}
	second := &scopedBridge{carrier: map[string]string{"scope": "second"}}
	SetTelemetryBridge(global)
	t.Cleanup(func() { SetTelemetryBridge(nil) })
	ctxA := WithTelemetryBridge(context.Background(), first)
	ctxB := WithTelemetryBridge(context.Background(), second)
	var disabled *scopedBridge
	ctxDisabled := WithTelemetryBridge(context.Background(), disabled)
	// Act: capture/restore callbacks cannot mutate host carrier ownership.
	carrier := extractTelemetryContext(ctxA)
	restored := injectTelemetryContext(ctxB, carrier)
	carrier["scope"] = "caller-owned"
	// Assert.
	if restored.Value(restoredScopeKey{}) != "first" || first.carrier["scope"] != "first" ||
		second.carrier["scope"] != "second" || extractTelemetryContext(context.Background())["scope"] != "global" ||
		extractTelemetryContext(
			ctxDisabled,
		) != nil || injectTelemetryContext(ctxDisabled, first.carrier) != ctxDisabled {
		t.Fatalf("restored=%v first=%v second=%v", restored.Value(restoredScopeKey{}), first.carrier, second.carrier)
	}
}
