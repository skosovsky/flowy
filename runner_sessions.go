package flowy

import (
	"context"
	"errors"
	"fmt"
	"time"
)

func (r *graphRunner[T, E]) RequestLocalHandoff(ctx context.Context, threadID string) error {
	if threadID == "" {
		return fmt.Errorf("%w: handoff requires threadID", ErrInvalidResumeToken)
	}
	value, ok := r.sessions.Load(threadID)
	if !ok {
		return fmt.Errorf("%w: thread %q", ErrNoActiveExecution, threadID)
	}
	session, ok := value.(*runSession)
	if !ok || session.cancel == nil {
		return fmt.Errorf("%w: thread %q", ErrNoActiveExecution, threadID)
	}
	session.cancel(ErrHandoffRequested)

	waitCtx := ctx
	if _, hasDeadline := ctx.Deadline(); !hasDeadline {
		var cancel context.CancelFunc
		waitCtx, cancel = context.WithTimeout(ctx, handoffCompletionTimeout)
		defer cancel()
	}
	select {
	case <-session.done:
		if err := waitCtx.Err(); err != nil {
			return fmt.Errorf("%w: %w", ErrHandoffNotCompleted, err)
		}
		if err := session.completionError(); err != nil {
			return err
		}
		return nil
	case <-waitCtx.Done():
		return fmt.Errorf("%w: %w", ErrHandoffNotCompleted, waitCtx.Err())
	}
}

func (r *graphRunner[T, E]) registerRunSession(
	threadID string,
	cancel context.CancelCauseFunc,
) (*runSession, error) {
	if _, loaded := r.sessions.Load(threadID); loaded {
		return nil, fmt.Errorf("%w: thread %q", ErrThreadAlreadyRunning, threadID)
	}
	session := newRunSession(cancel)
	if _, loaded := r.sessions.LoadOrStore(threadID, session); loaded {
		return nil, fmt.Errorf("%w: thread %q", ErrThreadAlreadyRunning, threadID)
	}
	return session, nil
}

func (r *graphRunner[T, E]) unregisterRunSessionIfSame(threadID string, session *runSession) {
	value, ok := r.sessions.Load(threadID)
	if !ok {
		return
	}
	if value == session {
		r.sessions.Delete(threadID)
	}
}

func newRunMetadata() RunMetadata {
	now := time.Now().UTC()
	seg := newSegmentInfo()
	return RunMetadata{
		Segment:          seg,
		SegmentStartTime: now,
		RetryCounts:      map[string]int{},
		BudgetCounts:     map[string]int{},
		StepCount:        0,
		TelemetryContext: nil,
		HandoffStatus:    HandoffStatusNone,
		HandoffPendingAt: time.Time{},
	}
}

func (r *graphRunner[T, E]) attachInvocation(
	ctx context.Context,
	inv runInvocationOptions[T, E],
	meta *RunMetadata,
) context.Context {
	runCtx := withRunMetadata(ctx, meta)
	if inv.bindings != nil {
		runCtx = inv.bindings.WithContext(runCtx)
	}
	if inv.leaseOwner != "" {
		runCtx = WithExecutionLease(runCtx, inv.lease)
	}
	return runCtx
}

func (r *graphRunner[T, E]) acquireLease(
	ctx context.Context,
	threadID string,
	inv *runInvocationOptions[T, E],
) error {
	if r.leaseManager == nil {
		if inv.leaseOwner != "" {
			return ErrExecutionCapability
		}
		return nil
	}
	if _, nativeCP := r.checkpointer.(NativeDeleteIfIdleCheckpointer); nativeCP {
		if _, paired := r.leaseManager.(NativeLeaseManager); !paired {
			return errors.New(
				"flowy: native DeleteIfIdle checkpointer requires paired native adapters/lease manager",
			)
		}
	}
	if inv.leaseOwner == "" {
		return ErrLeaseOwnerRequired
	}
	ttl := inv.leaseTTL
	if ttl <= 0 {
		ttl = defaultLeaseTTL
	}
	lease, err := r.leaseManager.Acquire(ctx, threadID, inv.leaseOwner, ttl)
	if err != nil {
		return err
	}
	if lease.ExecutionID != threadID || lease.Owner != inv.leaseOwner || lease.Incarnation == 0 {
		return ErrExecutionCapability
	}
	inv.lease = lease
	return nil
}

func (r *graphRunner[T, E]) releaseLease(
	ctx context.Context,
	threadID string,
	inv runInvocationOptions[T, E],
) error {
	if r.leaseManager == nil || inv.leaseOwner == "" {
		return nil
	}
	if inv.lease.ExecutionID != threadID {
		return ErrLeaseLost
	}
	releaseCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), contextCancelSaveTimeout)
	defer cancel()
	return r.leaseManager.Release(releaseCtx, inv.lease)
}

func (r *graphRunner[T, E]) leaseTTL(inv runInvocationOptions[T, E]) time.Duration {
	ttl := inv.leaseTTL
	if ttl <= 0 {
		ttl = defaultLeaseTTL
	}
	return ttl
}

func (r *graphRunner[T, E]) startLeaseHeartbeat(
	ctx context.Context,
	inv runInvocationOptions[T, E],
	cancelRun context.CancelCauseFunc,
) func() {
	if r.leaseManager == nil || inv.leaseOwner == "" {
		return func() {}
	}
	ttl := r.leaseTTL(inv)
	interval := leaseHeartbeatInterval(ttl)

	hbCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		renew := func() bool {
			if _, err := r.leaseManager.Renew(hbCtx, inv.lease, ttl); err != nil {
				if cancelRun != nil {
					cancelRun(ErrLeaseLost)
				}
				return false
			}
			return true
		}
		if !renew() {
			return
		}
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-hbCtx.Done():
				return
			case <-ticker.C:
				if !renew() {
					return
				}
			}
		}
	}()
	return func() {
		cancel()
		<-done
	}
}
