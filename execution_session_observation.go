package flowy

import (
	"context"
	"errors"
)

type executionSessionContextKey struct{}

func attachSessionObservation(ctx context.Context, envelope ExecutionEnvelope) {
	session, ok := ctx.Value(executionSessionContextKey{}).(*executionSession)
	if !ok || session.lease.ExecutionID != envelope.ExecutionID {
		return
	}
	event := executionObservation(envelope, LifecycleLease, LifecycleFailed)
	event.LeaseIncarnation = session.lease.Incarnation
	session.observationMu.Lock()
	session.observation, session.observationContext = event, ctx
	session.observationMu.Unlock()
}

func restoreExecutionTelemetry(ctx context.Context, envelope ExecutionEnvelope) context.Context {
	restored := injectTelemetryContext(ctx, envelope.RunMeta.TelemetryContext)
	attachSessionObservation(restored, envelope)
	return restored
}

func (s *executionSession) noteLeaseFailure(err error) {
	if !errors.Is(err, ErrLeaseLost) {
		return
	}
	s.observationMu.Lock()
	s.lossConfirmed = true
	s.observationMu.Unlock()
}

func (s *executionSession) observeLeaseLoss(ownedCtx context.Context) {
	s.observationMu.Lock()
	lost := s.lossConfirmed || errors.Is(context.Cause(ownedCtx), ErrLeaseLost)
	ctx, event := s.observationContext, s.observation
	s.observationMu.Unlock()
	if !lost {
		return
	}
	event.Code = "lease_lost"
	// Host-derived contexts may have been detached with WithoutCancel. Preserve
	// their trace values, but give this loss callback an explicit canceled cause.
	callbackCtx, cancel := context.WithCancelCause(context.WithoutCancel(ctx))
	cancel(ErrLeaseLost)
	observeLifecycle(callbackCtx, event)
}

func commitExecution(ctx context.Context, store ExecutionStore, revision uint64, lease ExecutionLease,
	target ExecutionEnvelope) (ExecutionEnvelope, error) {
	committed, err := store.CommitExecution(ctx, revision, lease, target)
	noteSessionLeaseFailure(ctx, err)
	if err == nil {
		attachSessionObservation(ctx, committed)
	}
	return committed, err
}

func noteSessionLeaseFailure(ctx context.Context, err error) {
	if session, ok := ctx.Value(executionSessionContextKey{}).(*executionSession); ok {
		session.noteLeaseFailure(err)
	}
}
