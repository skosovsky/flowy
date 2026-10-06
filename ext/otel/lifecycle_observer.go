package otel

import (
	"context"
	"errors"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/skosovsky/flowy"
	"github.com/skosovsky/flowy/internal/nilvalue"
)

const otherDimension = "other"

// ErrConfiguration rejects missing observation providers before installation.
var ErrConfiguration = errors.New("flowy/otel: invalid configuration")

func newLifecycleMetricsObserver(provider metric.MeterProvider) (*lifecycleObserver, error) {
	if nilvalue.IsNil(provider) {
		return nil, ErrConfiguration
	}
	meter := provider.Meter("github.com/skosovsky/flowy")
	events, err := meter.Int64Counter(
		"flowy.lifecycle_total",
		metric.WithDescription("Runtime observations by bounded operation and stage"),
	)
	if err != nil {
		return nil, err
	}
	return &lifecycleObserver{events: events}, nil
}

// NewLifecycleObserver binds bounded metrics to an explicit provider without
// changing the process-wide observer. Use flowy.WithLifecycleObserver per run.
func NewLifecycleObserver(provider metric.MeterProvider) (flowy.LifecycleObserver, error) {
	observer, err := newLifecycleMetricsObserver(provider)
	if err != nil {
		return nil, err
	}
	return observer, nil
}

// InstallLifecycleObserver registers the bounded runtime observation counter.
func InstallLifecycleObserver() error {
	obs, err := NewLifecycleObserver(otel.GetMeterProvider())
	if err != nil {
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
		flowy.LifecycleWaitArm, flowy.LifecycleWaitDelivery, flowy.LifecycleWaitCancel, flowy.LifecycleLease,
		flowy.LifecycleMigration, flowy.LifecycleImport, flowy.LifecycleFork, flowy.LifecycleRollover, flowy.LifecycleRetention:
		return string(operation)
	default:
		return otherDimension
	}
}

var _ flowy.LifecycleObserver = (*lifecycleObserver)(nil)
