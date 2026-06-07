package otel

import (
	"context"
	"log/slog"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/skosovsky/flowy"
)

func newLifecycleMetricsObserver() (*lifecycleObserver, error) {
	meter := otel.Meter("github.com/skosovsky/flowy")
	handoffEnqueued, err1 := meter.Int64Counter(
		"flowy.handoff_enqueued_total",
		metric.WithDescription("Handoff outbox enqueue attempts after checkpoint save"),
	)
	resumeRejected, err2 := meter.Int64Counter(
		"flowy.resume_rejected_total",
		metric.WithDescription("Resume attempts rejected by runner validation"),
	)
	checkpointSoftError, err3 := meter.Int64Counter(
		"flowy.checkpoint_soft_error_total",
		metric.WithDescription("Checkpoint save failures handled by skip-on-save-error policy"),
	)
	if err1 != nil {
		return nil, err1
	}
	if err2 != nil {
		return nil, err2
	}
	if err3 != nil {
		return nil, err3
	}
	return &lifecycleObserver{
		handoffEnqueued:     handoffEnqueued,
		resumeRejected:      resumeRejected,
		checkpointSoftError: checkpointSoftError,
	}, nil
}

// InstallLifecycleObserver registers OTel counters for flowy lifecycle events.
func InstallLifecycleObserver() error {
	obs, err := newLifecycleMetricsObserver()
	if err != nil {
		slog.Default().ErrorContext(context.Background(), "InstallLifecycleObserver", "err", err)
		return err
	}
	flowy.SetLifecycleObserver(obs)
	return nil
}

type lifecycleObserver struct {
	handoffEnqueued     metric.Int64Counter
	resumeRejected      metric.Int64Counter
	checkpointSoftError metric.Int64Counter
}

func (o *lifecycleObserver) HandoffEnqueued(
	ctx context.Context,
	threadID string,
	pointer flowy.ExecutionPointer,
	status string,
) {
	if o == nil || o.handoffEnqueued == nil {
		return
	}
	o.handoffEnqueued.Add(ctx, 1, metric.WithAttributes(
		attribute.String("thread_id", threadID),
		attribute.String("node", string(pointer)),
		attribute.String("status", status),
	))
}

func (o *lifecycleObserver) ResumeRejected(
	ctx context.Context,
	threadID string,
	pointer flowy.ExecutionPointer,
	reason string,
) {
	if o == nil || o.resumeRejected == nil {
		return
	}
	o.resumeRejected.Add(ctx, 1, metric.WithAttributes(
		attribute.String("thread_id", threadID),
		attribute.String("node", string(pointer)),
		attribute.String("reason", reason),
	))
}

func (o *lifecycleObserver) CheckpointSoftError(
	ctx context.Context,
	threadID string,
	pointer flowy.ExecutionPointer,
) {
	if o == nil || o.checkpointSoftError == nil {
		return
	}
	o.checkpointSoftError.Add(ctx, 1, metric.WithAttributes(
		attribute.String("thread_id", threadID),
		attribute.String("node", string(pointer)),
	))
}

var _ flowy.LifecycleObserver = (*lifecycleObserver)(nil)
