package flowy

import (
	"context"
	"fmt"
	"sync"
	"time"
)

type executionSession struct {
	ctx                context.Context
	lease              ExecutionLease
	finish             func() error
	observationMu      sync.Mutex
	observationContext context.Context
	observation        LifecycleObservation
	lossConfirmed      bool
}

func (r *DurableRunner[T, E]) acquireSession(ctx context.Context, id string) (*executionSession, error) {
	if !validRuntimeText(id) {
		return nil, ErrExecutionCapability
	}
	if ctx.Err() != nil {
		return nil, context.Cause(ctx)
	}
	lease, err := r.store.AcquireExecution(ctx, id, r.options.Owner, r.options.LeaseTTL)
	if err != nil {
		return nil, err
	}
	ownedCtx, cancel := context.WithCancelCause(ctx)
	session := &executionSession{ctx: ownedCtx, lease: lease, finish: nil,
		observationMu: sync.Mutex{}, observationContext: ownedCtx,
		observation: lifecycleObservation(LifecycleLease, LifecycleFailed, id, ""), lossConfirmed: false}
	session.observation.LeaseIncarnation = lease.Incarnation
	ownedCtx = context.WithValue(ownedCtx, executionSessionContextKey{}, session)
	session.ctx = ownedCtx
	stop := r.heartbeat(ownedCtx, cancel, lease)
	var once sync.Once
	var cleanupErr error
	session.finish = func() error {
		once.Do(func() {
			cancel(context.Canceled)
			stop()
			releaseErr := r.release(ctx, lease)
			session.noteLeaseFailure(releaseErr)
			if releaseErr != nil {
				cleanupErr = fmt.Errorf("%w: %w", ErrRunCleanup, releaseErr)
			}
			session.observeLeaseLoss(ownedCtx)
		})
		return cleanupErr
	}
	return session, nil
}

func (r *DurableRunner[T, E]) release(ctx context.Context, lease ExecutionLease) error {
	releaseCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), contextCancelSaveTimeout)
	defer cancel()
	return r.store.ReleaseExecution(releaseCtx, lease)
}

func (r *DurableRunner[T, E]) heartbeat(
	ctx context.Context,
	cancel context.CancelCauseFunc,
	lease ExecutionLease,
) func() {
	stop, done := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(max(r.options.LeaseTTL/executionHeartbeatDivisor, time.Nanosecond))
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-stop:
				return
			case <-ticker.C:
				if _, err := r.store.RenewExecution(ctx, lease, r.options.LeaseTTL); err != nil {
					noteSessionLeaseFailure(ctx, ErrLeaseLost)
					cancel(ErrLeaseLost)
					return
				}
			}
		}
	}()
	return func() { close(stop); <-done }
}
