package otel

import (
	"context"
	"fmt"
	"testing"

	"go.opentelemetry.io/otel"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	"github.com/skosovsky/flowy"
)

func TestLifecycleDefaultMetricsHaveBoundedDimensions(t *testing.T) {
	// Arrange: one bounded operation/stage and a thousand unrelated runtime addresses.
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	previous := otel.GetMeterProvider()
	otel.SetMeterProvider(provider)
	t.Cleanup(func() { otel.SetMeterProvider(previous); _ = provider.Shutdown(context.Background()) })
	observer, err := newLifecycleMetricsObserver(otel.GetMeterProvider())
	if err != nil {
		t.Fatal(err)
	}
	// Act: arbitrary IDs, node names and diagnostic codes must not create metric series.
	for i := range 1000 {
		var event flowy.LifecycleObservation
		event.Operation, event.Stage = flowy.LifecycleActivity, flowy.LifecycleCommitted
		event.ExecutionID, event.ChildID, event.DecisionID = fmt.Sprintf(
			"execution-%d",
			i,
		), fmt.Sprintf(
			"child-%d",
			i,
		), fmt.Sprintf(
			"decision-%d",
			i,
		)
		event.Node, event.Code = flowy.ExecutionPointer(fmt.Sprintf("node-%d", i)), fmt.Sprintf("private reason %d", i)
		observer.ObserveLifecycle(context.Background(), event)
	}
	var metrics metricdata.ResourceMetrics
	if err = reader.Collect(context.Background(), &metrics); err != nil {
		t.Fatal(err)
	}
	// Assert: exactly one point with only the closed dimensions.
	found := false
	for _, scope := range metrics.ScopeMetrics {
		for _, metric := range scope.Metrics {
			if metric.Name != "flowy.lifecycle_total" {
				continue
			}
			points, ok := metric.Data.(metricdata.Sum[int64])
			if !ok || len(points.DataPoints) != 1 {
				t.Fatalf("unbounded series: %+v", metric)
			}
			point := points.DataPoints[0]
			if point.Value != 1000 || point.Attributes.Len() != 2 ||
				!datapointMatchesAttr(point, "operation", "activity") ||
				!datapointMatchesAttr(point, "stage", "committed") {
				t.Fatalf("unexpected dimensions: %+v", point)
			}
			found = true
		}
	}
	if !found {
		t.Fatal("runtime counter missing")
	}
}

func TestLifecycleReplaySpanUsesRestoredRemoteParentAndRedactsCode(t *testing.T) {
	// Arrange: W3C carrier crossing a worker-context boundary.
	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })
	original, parent := provider.Tracer("source").Start(context.Background(), "source")
	carrier := (bridge{}).Capture(original)
	parent.End()
	restored := (bridge{}).Restore(context.Background(), carrier)
	observer := lifecycleTracingDecorator{inner: nil, tracer: provider.Tracer("worker")}
	var event flowy.LifecycleObservation
	event.Operation, event.Stage, event.ExecutionID, event.Code = flowy.LifecycleActivity, flowy.LifecycleReplayed, "execution", "private host error payload"
	// Act: replay is a new observation span, not another external attempt.
	observer.ObserveLifecycle(restored, event)
	spans := recorder.Ended()
	// Assert: trace correlation survives, identifiers are fresh, raw reason is absent.
	if len(spans) != 2 {
		t.Fatalf("spans=%d", len(spans))
	}
	replay := spans[1]
	if replay.Name() != "flowy.lifecycle.activity.replayed" ||
		replay.SpanContext().TraceID() != parent.SpanContext().TraceID() ||
		replay.Parent().SpanID() != parent.SpanContext().SpanID() ||
		!replay.Parent().IsRemote() ||
		replay.SpanContext().SpanID() == parent.SpanContext().SpanID() {
		t.Fatalf("replay parent contract: parent=%+v span=%+v", replay.Parent(), replay.SpanContext())
	}
	for _, attribute := range replay.Attributes() {
		if attribute.Value.AsString() == event.Code {
			t.Fatal("arbitrary host reason exported")
		}
	}
	if trace.SpanContextFromContext(restored).TraceID() != parent.SpanContext().TraceID() {
		t.Fatal("carrier was execution authority instead of correlation")
	}
}
