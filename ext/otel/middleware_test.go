package otel

import (
	"context"
	"errors"
	"testing"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/baggage"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	"github.com/skosovsky/flowy"
)

func TestTracingMiddlewareCreatesSpanPerNode(t *testing.T) {
	rec := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec))
	defer func() { _ = tp.Shutdown(context.Background()) }()

	type state struct{}
	b := flowy.NewGraph(func(_ state, u state) state { return u })
	b.Use(TracingMiddleware[state](tp.Tracer("test")))
	b.AddNode("n", func(_ context.Context, s state) (state, flowy.Directive, error) {
		return s, flowy.End(), nil
	})
	b.SetEntryPoint("n")
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	_, err = g.NewRunner(newCP[state]{}).Start(context.Background(), "otel-span", state{})
	if err != nil {
		t.Fatalf("start: %v", err)
	}

	spans := rec.Ended()
	if len(spans) != 1 {
		t.Fatalf("expected 1 ended span, got %d", len(spans))
	}
	span := spans[0]
	if span.Name() != "flowy.node.n" {
		t.Fatalf("unexpected span name %q", span.Name())
	}
	if !hasAttr(span.Attributes(), "flowy.directive", "end") {
		t.Fatalf("expected flowy.directive=end attribute, got %+v", span.Attributes())
	}
}

func TestTracingMiddlewareRecordsNodeError(t *testing.T) {
	rec := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec))
	defer func() { _ = tp.Shutdown(context.Background()) }()

	type state struct{}
	b := flowy.NewGraph(func(_ state, u state) state { return u })
	b.Use(TracingMiddleware[state](tp.Tracer("test")))
	b.AddNode("n", func(_ context.Context, s state) (state, flowy.Directive, error) {
		return s, flowy.Completed(), errors.New("boom")
	})
	b.SetEntryPoint("n")
	g, _ := b.Compile()
	_, _ = g.NewRunner(newCP[state]{}).Start(context.Background(), "otel-error", state{})

	spans := rec.Ended()
	if len(spans) != 1 {
		t.Fatalf("expected 1 ended span, got %d", len(spans))
	}
	if spans[0].Status().Code != codes.Error {
		t.Fatalf("expected error span status, got %+v", spans[0].Status())
	}
}

func TestTelemetryBridgeRoundTrip(t *testing.T) {
	member, err := baggage.NewMember("tenant", "acme")
	if err != nil {
		t.Fatalf("new baggage member: %v", err)
	}
	bag, err := baggage.New(member)
	if err != nil {
		t.Fatalf("baggage: %v", err)
	}
	traceID, _ := trace.TraceIDFromHex("0123456789abcdef0123456789abcdef")
	spanID, _ := trace.SpanIDFromHex("0123456789abcdef")
	ctx := trace.ContextWithSpanContext(context.Background(), trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    traceID,
		SpanID:     spanID,
		TraceFlags: trace.FlagsSampled,
	}))
	ctx = baggage.ContextWithBaggage(ctx, bag)

	metadata := (bridge{}).Extract(ctx)
	if len(metadata) == 0 {
		t.Fatal("expected non-empty telemetry metadata")
	}
	restored := (bridge{}).Inject(context.Background(), metadata)
	restoredSC := trace.SpanContextFromContext(restored)
	if restoredSC.TraceID() != traceID {
		t.Fatalf("trace id mismatch: got %s want %s", restoredSC.TraceID(), traceID)
	}
	if baggage.FromContext(restored).Member("tenant").Value() != "acme" {
		t.Fatalf("expected baggage tenant=acme, got %+v", baggage.FromContext(restored))
	}
}

type newCP[T any] struct{}

func (newCP[T]) Save(context.Context, flowy.Snapshot[T]) error { return nil }
func (newCP[T]) Load(context.Context, string) (flowy.Snapshot[T], error) {
	return flowy.Snapshot[T]{}, flowy.ErrThreadNotFound
}
func (newCP[T]) GetHistory(context.Context, string, int) ([]flowy.Snapshot[T], error) {
	return nil, nil
}
func (newCP[T]) Prune(context.Context, string, int) error { return nil }

func hasAttr(attrs []attribute.KeyValue, key, value string) bool {
	for _, attr := range attrs {
		if string(attr.Key) == key && attr.Value.AsString() == value {
			return true
		}
	}
	return false
}
