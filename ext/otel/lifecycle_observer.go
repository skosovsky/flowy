package otel

import (
	"context"
	"log/slog"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/skosovsky/flowy"
)

const otherDimension = "other"

func newLifecycleMetricsObserver() (*lifecycleObserver, error) {
	meter := otel.Meter("github.com/skosovsky/flowy")
	events, err := meter.Int64Counter(
		"flowy.lifecycle_total",
		metric.WithDescription("Runtime observations by bounded operation and stage"),
	)
	if err != nil {
		return nil, err
	}
	return &lifecycleObserver{events: events}, nil
}

// InstallLifecycleObserver registers the bounded runtime observation counter.
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
	events metric.Int64Counter
}

func (o *lifecycleObserver) ObserveLifecycle(ctx context.Context, event flowy.LifecycleObservation) {
	if o == nil || o.events == nil {
		return
	}
	attrs := metric.WithAttributes(
		attribute.String("operation", boundedOperation(event.Operation)),
		attribute.String("stage", boundedStage(event.Stage)),
	)
	o.events.Add(ctx, 1, attrs)
}

func boundedStage(stage flowy.LifecycleStage) string {
	switch stage {
	case flowy.LifecycleStarted, flowy.LifecycleFailed, flowy.LifecycleCommitted, flowy.LifecycleReplayed:
		return string(stage)
	default:
		return otherDimension
	}
}

func boundedOperation(operation flowy.LifecycleOperation) string {
	switch operation {
	case flowy.LifecycleExecution, flowy.LifecycleTerminal, flowy.LifecycleCheckpoint, flowy.LifecycleHandoff,
		flowy.LifecycleResume, flowy.LifecycleActivity, flowy.LifecycleReconcile, flowy.LifecycleRetry,
		flowy.LifecycleChildLaunch, flowy.LifecycleChildResolve, flowy.LifecycleChildJoin, flowy.LifecycleChildCancel,
		flowy.LifecycleWaitArm, flowy.LifecycleWaitWinner, flowy.LifecycleWaitCancel, flowy.LifecycleLease,
		flowy.LifecycleMigration, flowy.LifecycleImport, flowy.LifecycleFork, flowy.LifecycleRollover, flowy.LifecycleRetention:
		return string(operation)
	default:
		return otherDimension
	}
}

var _ flowy.LifecycleObserver = (*lifecycleObserver)(nil)
