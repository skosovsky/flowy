package flowy

import "context"

func waitObservation(
	source ExecutionEnvelope,
	operation LifecycleOperation,
	generation, decision string,
) LifecycleObservation {
	event := executionObservation(source, operation, LifecycleStarted)
	event.WorkID, event.DecisionID = generation, decision
	return event
}

func (c *executionCheckpointer[T, E]) armWait(ctx context.Context, expectedRevision uint64,
	snapshot Snapshot[T, E], spec DurableWaitSpec) (DurableWaitRecord, error) {
	c.mu.Lock()
	event := waitObservation(c.envelope, LifecycleWaitArm, spec.ID, "")
	event.Stage = LifecycleFailed
	record, err := c.armWaitLocked(ctx, expectedRevision, snapshot, spec)
	if err == nil {
		event.Stage, event.Revision = LifecycleCommitted, c.envelope.Revision
		event.WorkID, event.SegmentID = record.Generation, snapshot.RunMeta.Segment.SegmentID
	}
	c.mu.Unlock()
	observeLifecycle(ctx, event)
	return record, err
}
