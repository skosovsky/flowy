package flowy

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"time"
)

const durableWaitReason = "durable_wait"

func registerDurableWait(ctx context.Context, backend DurableWaitBackend, record DurableWaitRecord) error {
	if backend == nil || backend.WaitCapabilities() != record.Profile {
		return ErrExecutionCapability
	}
	if ctx.Err() != nil {
		return context.Cause(ctx)
	}
	if err := backend.RegisterWait(ctx, cloneDurableWait(record)); err != nil {
		event := lifecycleObservation(LifecycleWaitArm, LifecycleFailed, record.ExecutionID, record.Node)
		event.WorkID, event.SourceRevision, event.Code = record.Generation, record.ArmRevision, "wait_registration_failed"
		observeLifecycle(ctx, event)
		return errors.Join(ErrWaitRegistration, err)
	}
	if ctx.Err() != nil {
		return context.Cause(ctx)
	}
	return nil
}

func (c *executionCheckpointer[T, E]) armWaitLocked(ctx context.Context, expectedRevision uint64,
	snapshot Snapshot[T, E], spec DurableWaitSpec,
) (DurableWaitRecord, error) {
	if c.waitBackend == nil || c.waitProfile == nil || c.waitBackend.WaitCapabilities() != *c.waitProfile {
		return DurableWaitRecord{}, ErrExecutionCapability
	}
	if expectedRevision != c.stepRevision || snapshot.ThreadID != c.envelope.ExecutionID ||
		snapshot.ExecutionPointer != c.envelope.Progress.ExecutionPointer {
		return DurableWaitRecord{}, ErrConcurrencyConflict
	}
	if c.envelope.Revision == math.MaxUint64 {
		return DurableWaitRecord{}, ErrWaitInvalid
	}
	if err := c.resolvedActivitiesLocked(); err != nil {
		return DurableWaitRecord{}, err
	}
	record, err := planDurableWait(c.envelope.ExecutionID, snapshot.ExecutionPointer,
		c.envelope.Activation, c.envelope.Revision+1, spec, c.waitBackend.WaitCapabilities())
	if err != nil {
		return DurableWaitRecord{}, err
	}
	waits, err := executionWaits(c.envelope)
	if err != nil {
		return DurableWaitRecord{}, err
	}
	identity := waitIdentity(record.ExecutionID, record.Node, record.Activation, record.Spec.ID)
	if _, exists := waits[identity]; exists {
		return DurableWaitRecord{}, ErrWaitConflict
	}
	waits[identity] = record
	target := cloneExecutionEnvelope(c.envelope)
	target.WaitsPayload, err = json.Marshal(waits)
	if err != nil {
		return DurableWaitRecord{}, err
	}
	if _, err = c.persistSnapshotLocked(ctx, target, snapshot); err != nil {
		return DurableWaitRecord{}, err
	}
	return cloneDurableWait(record), nil
}

func (r *graphRunner[T, E]) applyDirectiveWait(ctx context.Context, threadID, current string,
	state T, meta RunMetadata, effects []E, revision uint64, directive Directive, sink eventSink[T, E],
) directiveStep[T, E] {
	if r.durable == nil || r.durable.waitBackend == nil {
		return terminalDirectiveStep(failedResultWithReason(state, effects, meta, current, durableWaitReason),
			ErrExecutionCapability)
	}
	if directive.wait == nil || directive.wait.Validate() != nil ||
		r.validateExecutionPointer(directive.wait.EventPointer) != nil ||
		r.validateExecutionPointer(directive.wait.TimeoutPointer) != nil {
		return terminalDirectiveStep(failedResultWithReason(state, effects, meta, current, durableWaitReason),
			ErrWaitInvalid)
	}
	meta.TelemetryContext = extractTelemetryContext(ctx)
	meta.Segment.EndTime = time.Now().UTC()
	meta.Segment.EndReason = SegmentEndSuspend
	record, err := r.durable.armWait(ctx, revision, Snapshot[T, E]{
		ThreadID: threadID, Revision: 0, State: state, Effects: effects, RunMeta: meta,
		ExecutionPointer: ExecutionPointer(current),
	}, *directive.wait)
	if err != nil {
		return terminalDirectiveStep(failedResultWithReason(state, effects, meta, current, durableWaitReason), err)
	}
	result := newRunResultSuspended(state, effects, meta, current, durableWaitReason)
	if registrationErr := registerDurableWait(ctx, r.durable.waitBackend, record); registrationErr != nil {
		return terminalDirectiveStep(result, registrationErr)
	}
	emitTerminalEvent(ctx, sink, newRunEventSuspendedNoError[T, E](current, state, durableWaitReason))
	return terminalDirectiveStep(result, nil)
}

func (r *DurableRunner[T, E]) checkArmedWaitCompatibility(envelope ExecutionEnvelope) error {
	if err := r.checkRuntimeProfile(envelope.RuntimeProfile); err != nil {
		return err
	}
	waits, err := executionWaits(envelope)
	if err != nil {
		return err
	}
	for _, record := range waits {
		if record.State != WaitArmed {
			continue
		}
		if r.waitBackend == nil || r.options.WaitProfile == nil || *r.options.WaitProfile != record.Profile ||
			r.waitBackend.WaitCapabilities() != record.Profile {
			return ErrExecutionCapability
		}
		if r.validatePointer(record.Spec.EventPointer) != nil || r.validatePointer(record.Spec.TimeoutPointer) != nil {
			return ErrExecutionIncompatible
		}
	}
	return nil
}

func (r *DurableRunner[T, E]) checkRuntimeProfile(profile *WaitCapabilityProfile) error {
	if profile == nil {
		return nil
	}
	if r.waitBackend == nil || r.options.WaitProfile == nil || *r.options.WaitProfile != *profile ||
		r.waitBackend.WaitCapabilities() != *profile {
		return ErrExecutionCapability
	}
	return nil
}

func (r *DurableRunner[T, E]) recoverArmedWait(ctx context.Context, envelope ExecutionEnvelope,
	snapshot Snapshot[T, E], sink eventSink[T, E],
) (RunResult[T, E], bool, error) {
	waits, err := executionWaits(envelope)
	if err != nil {
		return RunResult[T, E]{}, false, err
	}
	for _, record := range waits {
		if record.State != WaitArmed {
			continue
		}
		result := newRunResultSuspended(snapshot.State, snapshot.Effects, snapshot.RunMeta,
			string(snapshot.ExecutionPointer), durableWaitReason)
		result.ResumeToken = ResumeToken{ThreadID: envelope.ExecutionID, SnapshotRevision: envelope.Revision}
		if registrationErr := registerDurableWait(ctx, r.waitBackend, record); registrationErr != nil {
			return *result, true, registrationErr
		}
		event := waitObservation(envelope, LifecycleWaitArm, record.Generation, "")
		event.Stage, event.Revision = LifecycleReplayed, envelope.Revision
		observeLifecycle(ctx, event)
		if sink != nil {
			sink(
				ctx,
				newRunEventSuspendedNoError[T, E](string(snapshot.ExecutionPointer), snapshot.State, durableWaitReason),
			)
		}
		return *result, true, nil
	}
	return RunResult[T, E]{}, false, nil
}
