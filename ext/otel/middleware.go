package otel

import (
	"context"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/skosovsky/flowy"
)

// TracingMiddleware creates span per flowy node.
func TracingMiddleware[T, E any](tracer trace.Tracer) flowy.NodeMiddleware[T, E] {
	return func(next flowy.Node[T, E]) flowy.Node[T, E] {
		return func(ctx context.Context, state T) (T, flowy.Directive, error) {
			nodeName := flowy.NodeNameFromContext(ctx)
			if nodeName == "" {
				nodeName = "unknown"
			}

			ctx, span := tracer.Start(ctx, "flowy.node."+nodeName)
			defer span.End()

			out, directive, err := next(ctx, state)
			base, _, unwrapErr := flowy.UnwrapDirective[E](directive)
			if unwrapErr != nil {
				span.RecordError(unwrapErr)
				span.SetStatus(codes.Error, unwrapErr.Error())
			} else {
				span.SetAttributes(attribute.String("flowy.directive", base.Type()))
			}
			if err != nil {
				span.RecordError(err)
				span.SetStatus(codes.Error, err.Error())
			}
			return out, directive, err
		}
	}
}
