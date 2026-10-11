package otel

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	"github.com/skosovsky/flowy"
)

type failingObservationProvider struct {
	metric.MeterProvider

	cause error
}

type failingObservationMeter struct {
	metric.Meter

	cause error
}

func (p failingObservationProvider) Meter(string, ...metric.MeterOption) metric.Meter {
	return failingObservationMeter{Meter: p.MeterProvider.Meter("test"), cause: p.cause}
}

func (m failingObservationMeter) Int64Counter(string, ...metric.Int64CounterOption) (metric.Int64Counter, error) {
	return nil, m.cause
}

func TestInstallationReturnsProviderErrorWithoutLoggingIt(t *testing.T) {
	// Arrange: provider errors belong to the caller; the library must not duplicate logging.
	cause := errors.New("provider rejected counter")
	previousProvider, previousLogger := otel.GetMeterProvider(), slog.Default()
	var output bytes.Buffer
	otel.SetMeterProvider(failingObservationProvider{MeterProvider: noop.NewMeterProvider(), cause: cause})
	slog.SetDefault(slog.New(slog.NewTextHandler(&output, nil)))
	t.Cleanup(func() { otel.SetMeterProvider(previousProvider); slog.SetDefault(previousLogger) })
	// Act.
	metricsErr := InstallLifecycleObserver()
	tracingErr := InstallLifecycleObserverWithTracing()
	// Assert.
	if !errors.Is(metricsErr, cause) || !errors.Is(tracingErr, cause) || output.Len() != 0 {
		t.Fatalf("metrics=%v tracing=%v logs=%q", metricsErr, tracingErr, output.String())
	}
}

func TestObservationConstructorsRejectNilProviders(t *testing.T) {
	t.Parallel()
	var typedMeter *sdkmetric.MeterProvider
	var typedTracer *sdktrace.TracerProvider
	for _, provider := range []metric.MeterProvider{nil, typedMeter} {
		// Arrange/Act: reject before any metric callback or global installation.
		observer, err := NewLifecycleObserver(provider)
		// Assert.
		if observer != nil || !errors.Is(err, ErrConfiguration) {
			t.Fatalf("observer=%v error=%v", observer, err)
		}
	}
	meter := sdkmetric.NewMeterProvider()
	t.Cleanup(func() { _ = meter.Shutdown(context.Background()) })
	for _, provider := range []trace.TracerProvider{nil, typedTracer} {
		observer, err := NewLifecycleObserverWithTracing(meter, provider)
		if observer != nil || !errors.Is(err, ErrConfiguration) {
			t.Fatalf("observer=%v error=%v", observer, err)
		}
	}
}

func TestExplicitObservationProvidersRemainIsolatedAndPrivate(t *testing.T) {
	t.Parallel()
	// Arrange: independent providers do not depend on global OTel installation.
	meter := sdkmetric.NewMeterProvider()
	first, second := tracetest.NewSpanRecorder(), tracetest.NewSpanRecorder()
	providerA := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(first))
	providerB := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(second))
	t.Cleanup(func() {
		_ = meter.Shutdown(context.Background())
		_ = providerA.Shutdown(context.Background())
		_ = providerB.Shutdown(context.Background())
	})
	observerA, err := NewLifecycleObserverWithTracing(meter, providerA)
	if err != nil {
		t.Fatal(err)
	}
	observerB, err := NewLifecycleObserverWithTracing(meter, providerB)
	if err != nil {
		t.Fatal(err)
	}
	// Act: arbitrary error/evidence-like text must collapse to a bounded code.
	observerA.ObserveLifecycle(context.Background(), flowy.LifecycleObservation{
		Operation: flowy.LifecycleCheckpoint, Stage: flowy.LifecycleCommitted,
		ExecutionID: "instance-a", Code: "private host evidence/error",
	})
	observerB.ObserveLifecycle(context.Background(), flowy.LifecycleObservation{
		Operation: flowy.LifecycleCheckpoint, Stage: flowy.LifecycleCommitted, ExecutionID: "instance-b",
	})
	// Assert: provider A never receives B and raw diagnostic text never escapes.
	for index, recorder := range []*tracetest.SpanRecorder{first, second} {
		spans := recorder.Ended()
		if len(spans) != 1 {
			t.Fatalf("provider=%d spans=%d", index, len(spans))
		}
		wantID := []string{"instance-a", "instance-b"}[index]
		foundID := false
		for _, attribute := range spans[0].Attributes() {
			if string(attribute.Key) == "thread_id" && attribute.Value.AsString() == wantID {
				foundID = true
			}
			if strings.Contains(attribute.Value.AsString(), "private host evidence/error") {
				t.Fatal("raw diagnostic text exported")
			}
		}
		if !foundID {
			t.Fatalf("provider=%d wrong identities=%v", index, spans[0].Attributes())
		}
	}
}
