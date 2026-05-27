package otel

import (
	"context"
	"maps"

	"go.opentelemetry.io/otel/propagation"

	"github.com/skosovsky/flowy"
)

// InstallTelemetryBridge registers OTel propagation bridge in flowy core.
func InstallTelemetryBridge() {
	flowy.SetTelemetryBridge(bridge{})
}

type bridge struct{}

func (bridge) Extract(ctx context.Context) map[string]string {
	carrier := mapCarrier{}
	propagation.TraceContext{}.Inject(ctx, carrier)
	propagation.Baggage{}.Inject(ctx, carrier)
	if len(carrier) == 0 {
		return nil
	}
	out := make(map[string]string, len(carrier))
	maps.Copy(out, carrier)
	return out
}

func (bridge) Inject(ctx context.Context, metadata map[string]string) context.Context {
	if len(metadata) == 0 {
		return ctx
	}
	carrier := mapCarrier{}
	maps.Copy(carrier, metadata)
	ctx = propagation.TraceContext{}.Extract(ctx, carrier)
	ctx = propagation.Baggage{}.Extract(ctx, carrier)
	return ctx
}

type mapCarrier map[string]string

func (m mapCarrier) Get(key string) string {
	return m[key]
}

func (m mapCarrier) Set(key, value string) {
	m[key] = value
}

func (m mapCarrier) Keys() []string {
	out := make([]string, 0, len(m))
	for key := range m {
		out = append(out, key)
	}
	return out
}
