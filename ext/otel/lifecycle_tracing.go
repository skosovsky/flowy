package otel

import (
	"context"
	"strconv"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"

	"github.com/skosovsky/flowy"
	"github.com/skosovsky/flowy/internal/nilvalue"
)

type lifecycleTracingDecorator struct {
	inner  flowy.LifecycleObserver
	tracer trace.Tracer
}

// InstallLifecycleObserverWithTracing registers OTel counters and lifecycle trace spans.
func InstallLifecycleObserverWithTracing() error {
	observer, err := NewLifecycleObserverWithTracing(otel.GetMeterProvider(), otel.GetTracerProvider())
	if err != nil {
		return err
	}
	flowy.SetLifecycleObserver(observer)
	return nil
}

// NewLifecycleObserverWithTracing binds counters/spans to explicit providers
// without installing a global core observer. Constructor errors belong to the host.
func NewLifecycleObserverWithTracing(
	meter metric.MeterProvider,
	tracer trace.TracerProvider,
) (flowy.LifecycleObserver, error) {
	if nilvalue.IsNil(tracer) {
		return nil, ErrConfiguration
	}
	obs, err := newLifecycleMetricsObserver(meter)
	if err != nil {
		return nil, err
	}
	return &lifecycleTracingDecorator{
		inner:  obs,
		tracer: tracer.Tracer("github.com/skosovsky/flowy/lifecycle"),
	}, nil
}

func (d *lifecycleTracingDecorator) ObserveLifecycle(ctx context.Context, event flowy.LifecycleObservation) {
	if d.inner != nil {
		d.inner.ObserveLifecycle(ctx, event)
	}
	if d.tracer == nil {
		return
	}
	name := "flowy.lifecycle." + boundedOperation(event.Operation) + "." + boundedStage(event.Stage)

	_, span := d.tracer.Start(ctx, name)
	defer span.End()
	if event.Stage == flowy.LifecycleFailed {
		span.SetStatus(codes.Error, "operation_failed")
	}
	span.SetAttributes(
		attribute.String("operation", boundedOperation(event.Operation)),
		attribute.String("stage", boundedStage(event.Stage)),
		attribute.String("thread_id", event.ExecutionID),
		attribute.String("node", string(event.Node)),
		attribute.String("segment_id", event.SegmentID),
		attribute.String("activity_id", event.ActivityID),
		attribute.Int("activity_attempt", event.Attempt),
		attribute.String("child_id", event.ChildID),
		attribute.String("child_execution_id", event.ChildExecutionID),
		attribute.String("work_id", event.WorkID),
		attribute.String(
			"decision_id",
			event.DecisionID,
		),
		attribute.String("parent_execution_id", event.ParentExecutionID),
		attribute.String("source_execution_id", event.SourceExecutionID),
		attribute.String("target_execution_id", event.TargetExecutionID),
		attribute.String("lease_incarnation", strconv.FormatUint(event.LeaseIncarnation, 10)),
		attribute.String("target_revision", strconv.FormatUint(event.TargetRevision, 10)),
		attribute.String("source_revision", strconv.FormatUint(event.SourceRevision, 10)),
		attribute.String("revision", strconv.FormatUint(event.Revision, 10)),
	)
	span.SetAttributes(attribute.String("runtime_code", boundedCode(event.Code)))
	if event.Operation == flowy.LifecycleHandoff {
		span.SetAttributes(attribute.String("status", boundedCode(event.Code)))
	}
	if event.Operation == flowy.LifecycleResume {
		span.SetAttributes(attribute.String("reason", boundedCode(event.Code)))
	}
}

func boundedCode(code string) string {
	switch code {
	case "invalid_classification",
		"success",
		"enqueue_failed",
		"patch_enqueued_failed",
		"patch_orphan_failed",
		"save_failed",
		"commit_failed",
		"empty_token",
		"invalid_snapshot",
		"zero_revision",
		"stale_token",
		"handoff_pending",
		"handoff_orphaned",
		"invalid_handoff_status",
		"invalid_pointer",
		"soft_error",
		"lease_lost",
		"outcome_completed",
		"outcome_failed",
		"retry_pending",
		"outcome_unknown",
		"outcome_running",
		"invalid_state",
		"child_planned",
		"child_queued",
		"wait_registration_failed",
		"child_group_prepared",
		"child_running",
		"child_completed",
		"child_failed",
		"child_waiting",
		"child_unknown",
		"child_canceled",
		"child_cancel_requested",
		"child_notify_failed",
		"wait_accepted",
		"wait_unmatched",
		"wait_lost",
		"wait_canceled":
		return code
	default:
		return otherDimension
	}
}
