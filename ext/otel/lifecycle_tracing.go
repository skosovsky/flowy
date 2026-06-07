package otel

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/skosovsky/flowy"
)

type lifecycleTracingDecorator struct {
	inner  flowy.LifecycleObserver
	tracer trace.Tracer
}

// InstallLifecycleObserverWithTracing registers OTel counters and lifecycle trace spans.
func InstallLifecycleObserverWithTracing() error {
	obs, err := newLifecycleMetricsObserver()
	if err != nil {
		return err
	}
	flowy.SetLifecycleObserver(&lifecycleTracingDecorator{
		inner:  obs,
		tracer: otel.Tracer("github.com/skosovsky/flowy/lifecycle"),
	})
	return nil
}

func (d *lifecycleTracingDecorator) HandoffEnqueued(
	ctx context.Context,
	threadID string,
	pointer flowy.ExecutionPointer,
	status string,
) {
	if d.inner != nil {
		d.inner.HandoffEnqueued(ctx, threadID, pointer, status)
	}
	if d.tracer == nil {
		return
	}
	_, span := d.tracer.Start(ctx, "flowy.lifecycle.handoff_enqueued")
	defer span.End()
	span.SetAttributes(
		attribute.String("thread_id", threadID),
		attribute.String("node", string(pointer)),
		attribute.String("status", status),
	)
}

func (d *lifecycleTracingDecorator) ResumeRejected(
	ctx context.Context,
	threadID string,
	pointer flowy.ExecutionPointer,
	reason string,
) {
	if d.inner != nil {
		d.inner.ResumeRejected(ctx, threadID, pointer, reason)
	}
	if d.tracer == nil {
		return
	}
	_, span := d.tracer.Start(ctx, "flowy.lifecycle.resume_rejected")
	defer span.End()
	span.SetAttributes(
		attribute.String("thread_id", threadID),
		attribute.String("node", string(pointer)),
		attribute.String("reason", reason),
	)
}

func (d *lifecycleTracingDecorator) CheckpointSoftError(
	ctx context.Context,
	threadID string,
	pointer flowy.ExecutionPointer,
) {
	if d.inner != nil {
		d.inner.CheckpointSoftError(ctx, threadID, pointer)
	}
	if d.tracer == nil {
		return
	}
	_, span := d.tracer.Start(ctx, "flowy.lifecycle.checkpoint_soft_error")
	defer span.End()
	span.SetAttributes(
		attribute.String("thread_id", threadID),
		attribute.String("node", string(pointer)),
	)
}
