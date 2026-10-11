package flowy

import "context"

func childObservation(
	source ExecutionEnvelope,
	operation LifecycleOperation,
	identity, childID string,
) LifecycleObservation {
	event := executionObservation(source, operation, LifecycleFailed)
	event.WorkID, event.ChildID = identity, childID
	event.ParentExecutionID = source.ExecutionID
	return event
}

func childPublicationObservation(event LifecycleObservation, revision uint64, err error) LifecycleObservation {
	if err == nil {
		event.Stage, event.Revision = LifecycleReplayed, revision
		if revision > event.SourceRevision {
			event.Stage = LifecycleCommitted
		}
	}
	return event
}

func (c *executionCheckpointer[T, E]) prepareChildren(ctx context.Context, plan ChildGroupPlan,
	available map[string]int) (ChildGroupRecord, error) {
	c.mu.Lock()
	identity := boundChildGroupIdentity(c.envelope, plan.Key)
	event := childObservation(c.envelope, LifecycleChildLaunch, identity, "")
	event.Code = "child_group_prepared"
	group, err := c.prepareChildrenLocked(ctx, plan, available)
	event = childPublicationObservation(event, c.envelope.Revision, err)
	c.mu.Unlock()
	observeLifecycle(ctx, event)
	if err == nil && event.Stage == LifecycleReplayed {
		observeCachedChildren(ctx, event, group)
	}
	return group, err
}

func (c *executionCheckpointer[T, E]) beginChild(
	ctx context.Context,
	identity, childID string,
) (ChildInvocation, error) {
	c.mu.Lock()
	event := childObservation(c.envelope, LifecycleChildLaunch, identity, childID)
	invocation, err := c.beginChildLocked(ctx, identity, childID)
	event.ChildExecutionID = invocation.ExecutionID
	event.Code = "child_running"
	event = childPublicationObservation(event, c.envelope.Revision, err)
	c.mu.Unlock()
	observeLifecycle(ctx, event)
	return invocation, err
}

func (c *executionCheckpointer[T, E]) finishChild(ctx context.Context, identity string, invocation ChildInvocation,
	result ChildResult, dispatchErr error) error {
	c.mu.Lock()
	event := childObservation(c.envelope, LifecycleChildResolve, identity, invocation.ChildID)
	event.ChildExecutionID = invocation.ExecutionID
	event.Code = childObservationCode(result.State)
	if dispatchErr != nil {
		event.Code = "child_unknown"
	}
	err := c.finishChildLocked(ctx, identity, invocation, result, dispatchErr)
	// Unknown outcomes return Unresolved even after successful publication.
	// Revision advance under this mutex proves acknowledgement, unlike host return.
	if c.envelope.Revision > event.SourceRevision {
		event.Stage, event.Revision = LifecycleCommitted, c.envelope.Revision
	}
	c.mu.Unlock()
	observeLifecycle(ctx, event)
	return err
}

func (c *executionCheckpointer[T, E]) requestChildCancellation(ctx context.Context, identity string,
	expected ChildGroupRecord, request ChildCancelRequest) (ChildGroupRecord, error) {
	c.mu.Lock()
	event := childObservation(c.envelope, LifecycleChildCancel, identity, "")
	event.DecisionID, event.Code = request.ID, "child_cancel_requested"
	group, err := c.requestChildCancellationLocked(ctx, identity, expected, request)
	event = childPublicationObservation(event, c.envelope.Revision, err)
	c.mu.Unlock()
	observeLifecycle(ctx, event)
	return group, err
}

func (c *executionCheckpointer[T, E]) observeChildDispatch(
	ctx context.Context,
	identity string,
	invocation ChildInvocation,
) {
	c.mu.Lock()
	event := childObservation(c.envelope, LifecycleChildLaunch, identity, invocation.ChildID)
	event.Stage, event.ChildExecutionID = LifecycleStarted, invocation.ExecutionID
	c.mu.Unlock()
	observeLifecycle(ctx, event)
}

func childObservationCode(state ChildState) string {
	switch state {
	case ChildPlanned:
		return "child_planned"
	case ChildQueued:
		return "child_queued"
	case ChildRunning:
		return "child_running"
	case ChildCompleted:
		return "child_completed"
	case ChildFailed:
		return "child_failed"
	case ChildWaiting:
		return "child_waiting"
	case ChildUnknown:
		return "child_unknown"
	case ChildCanceled:
		return "child_canceled"
	default:
		return "invalid_state"
	}
}

func (c *executionCheckpointer[T, E]) observeChildNotificationFailure(ctx context.Context, identity string,
	child ChildRecord, request ChildCancelRequest) {
	c.mu.Lock()
	event := childObservation(c.envelope, LifecycleChildCancel, identity, child.Spec.ID)
	event.ChildExecutionID, event.DecisionID, event.Code = child.ExecutionID, request.ID, "child_notify_failed"
	c.mu.Unlock()
	observeLifecycle(ctx, event)
}

func observeCachedChildren(ctx context.Context, event LifecycleObservation, group ChildGroupRecord) {
	event.Operation = LifecycleChildResolve
	for _, child := range group.Children {
		switch child.State {
		case ChildPlanned, ChildQueued, ChildRunning:
			continue
		case ChildCompleted, ChildFailed, ChildCanceled, ChildWaiting, ChildUnknown:
			event.ChildID, event.ChildExecutionID, event.Code = child.Spec.ID, child.ExecutionID, childObservationCode(
				child.State,
			)
			observeLifecycle(ctx, event)
		}
	}
}
