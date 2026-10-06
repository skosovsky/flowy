package flowy

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"math"
)

// WaitDeliveryContract declares pure host work independently of transport.
// Match may decode BYOT payload bytes under PayloadCodec; it must not perform
// external effects. Apply must be pure and may repeat after an uncommitted write.
type WaitDeliveryContract[T any] struct {
	MatcherLabel      string
	PayloadCodec      string
	ContinuationLabel string
	Match             func(context.Context, []byte) (bool, error)
	Apply             func(context.Context, T, WaitDelivery) (T, error)
}

// WaitDeliveryResult is a committed decision, not an implicit node execution.
// Replay identifies a cached decision with no new aggregate write.
type WaitDeliveryResult struct {
	Decision    WaitDecision
	ResumeToken ResumeToken
	Replay      bool
}

func (contract WaitDeliveryContract[T]) check(spec DurableWaitSpec) error {
	if contract.MatcherLabel != spec.MatcherLabel || contract.PayloadCodec != spec.PayloadCodec ||
		contract.ContinuationLabel != spec.ContinuationLabel {
		return ErrExecutionIncompatible
	}
	return nil
}

// DeliverWait atomically commits a delivery decision and, for its winner, state
// plus continuation under latest OCC and a live fence. Host owns authentication
// and permission checks. No durable decision is returned on a failed write.
func (r *DurableRunner[T, E]) DeliverWait(ctx context.Context, executionID string,
	delivery WaitDelivery, contract WaitDeliveryContract[T],
) (result WaitDeliveryResult, retErr error) { //nolint:nonamedreturns // Preserve committed result and cleanup error.
	if executionID == "" || delivery.Generation == "" || delivery.ID == "" ||
		!validRuntimeText(executionID, delivery.Generation, delivery.ID, delivery.CorrelationID) {
		return WaitDeliveryResult{}, ErrWaitInvalid
	}
	delivery.Payload = bytes.Clone(delivery.Payload)
	// Early delivery must not acquire/create a missing execution head.
	if _, err := r.waitDeliverySource(ctx, executionID, delivery.Generation); err != nil {
		return WaitDeliveryResult{}, err
	}
	session, err := r.acquireSession(ctx, executionID)
	if err != nil {
		return WaitDeliveryResult{}, err
	}
	defer func() { retErr = errors.Join(retErr, session.finish()) }()
	source, err := r.waitDeliverySource(session.ctx, executionID, delivery.Generation)
	if err != nil {
		return WaitDeliveryResult{}, err
	}
	session.ctx = restoreExecutionTelemetry(session.ctx, source)
	event := waitObservation(source, LifecycleWaitWinner, delivery.Generation, delivery.ID)
	observeLifecycle(session.ctx, event)
	event.Stage = LifecycleFailed
	defer func() { observeLifecycle(session.ctx, event) }()
	waits, err := executionWaits(source)
	if err != nil {
		return WaitDeliveryResult{}, err
	}
	identity, record, err := findWaitGeneration(waits, delivery.Generation)
	if err != nil {
		return WaitDeliveryResult{}, err
	}
	if contractErr := contract.check(record.Spec); contractErr != nil {
		return WaitDeliveryResult{}, contractErr
	}
	targetRecord, decision, replay, err := r.prepareWaitDelivery(session, source, record, delivery, contract)
	if err != nil {
		return WaitDeliveryResult{}, err
	}
	event.Code = "wait_" + string(decision.Status)
	if replay {
		event.Stage, event.Revision = LifecycleReplayed, source.Revision
		return WaitDeliveryResult{Decision: decision, Replay: true,
			ResumeToken: ResumeToken{ThreadID: executionID, SnapshotRevision: source.Revision}}, nil
	}
	target := cloneExecutionEnvelope(source)
	if decision.Status == WaitAccepted {
		target, err = r.applyWaitContinuation(session.ctx, target, record.Spec, delivery, contract)
		if err != nil {
			return WaitDeliveryResult{}, err
		}
	}
	waits[identity] = targetRecord
	target.WaitsPayload, err = json.Marshal(waits)
	if err != nil {
		return WaitDeliveryResult{}, err
	}
	if session.ctx.Err() != nil {
		return WaitDeliveryResult{}, context.Cause(session.ctx)
	}
	committed, err := commitExecution(session.ctx, r.store, source.Revision, session.lease, target)
	if err != nil {
		return WaitDeliveryResult{}, err
	}
	event.Stage, event.Revision = LifecycleCommitted, committed.Revision
	return WaitDeliveryResult{Decision: decision, Replay: false,
		ResumeToken: ResumeToken{ThreadID: executionID, SnapshotRevision: committed.Revision}}, nil
}

func findWaitGeneration(waits map[string]DurableWaitRecord, generation string) (string, DurableWaitRecord, error) {
	if len(waits) == 0 {
		return "", DurableWaitRecord{}, ErrWaitNotArmed
	}
	for identity, record := range waits {
		if record.Generation == generation {
			return identity, record, nil
		}
	}
	return "", DurableWaitRecord{}, ErrWaitUnknown
}

func (r *DurableRunner[T, E]) waitDeliverySource(
	ctx context.Context,
	id, generation string,
) (ExecutionEnvelope, error) {
	source, err := r.store.LoadExecution(ctx, id)
	if errors.Is(err, ErrThreadNotFound) {
		return ExecutionEnvelope{}, ErrWaitNotArmed
	}
	if err != nil {
		return ExecutionEnvelope{}, err
	}
	if err = ValidateExecutionIntegrity(source, id, source.Revision); err != nil {
		return ExecutionEnvelope{}, err
	}
	if err = validateExecutionCollections(source); err != nil {
		return ExecutionEnvelope{}, err
	}
	if err = validateExecutionSourceMetadata(source); err != nil {
		return ExecutionEnvelope{}, err
	}
	if err = r.checkRuntimeProfile(source.RuntimeProfile); err != nil {
		return ExecutionEnvelope{}, err
	}
	_, record, err := findWaitGenerationMustDecode(source, generation)
	if err != nil {
		return ExecutionEnvelope{}, err
	}
	if r.waitBackend == nil || r.options.WaitProfile == nil || record.Profile != *r.options.WaitProfile ||
		r.waitBackend.WaitCapabilities() != record.Profile {
		return ExecutionEnvelope{}, ErrExecutionCapability
	}
	if err = source.Descriptor.Check(r.descriptor); err != nil {
		return ExecutionEnvelope{}, err
	}
	if r.validatePointer(source.Progress.ExecutionPointer) != nil {
		return ExecutionEnvelope{}, ErrExecutionIncompatible
	}
	if r.validatePointer(record.Spec.EventPointer) != nil || r.validatePointer(record.Spec.TimeoutPointer) != nil {
		return ExecutionEnvelope{}, ErrExecutionIncompatible
	}
	return source, nil
}

func findWaitGenerationMustDecode(source ExecutionEnvelope, generation string) (string, DurableWaitRecord, error) {
	waits, err := executionWaits(source)
	if err != nil {
		return "", DurableWaitRecord{}, err
	}
	return findWaitGeneration(waits, generation)
}

func (r *DurableRunner[T, E]) prepareWaitDelivery(session *executionSession, source ExecutionEnvelope,
	record DurableWaitRecord, delivery WaitDelivery, contract WaitDeliveryContract[T],
) (DurableWaitRecord, WaitDecision, bool, error) {
	now := r.options.Clock.Now().UTC()
	target, decision, replay, err := prepareWaitDecision(record, delivery, source.Revision,
		session.lease.Incarnation, now, false)
	if err != nil || replay || record.State != WaitArmed || delivery.Kind != WaitEvent {
		return target, decision, replay, err
	}
	if contract.Match == nil {
		return DurableWaitRecord{}, WaitDecision{}, false, ErrWaitInvalid
	}
	if session.ctx.Err() != nil {
		return DurableWaitRecord{}, WaitDecision{}, false, context.Cause(session.ctx)
	}
	matched, matchErr := contract.Match(session.ctx, bytes.Clone(delivery.Payload))
	if matchErr != nil {
		return DurableWaitRecord{}, WaitDecision{}, false, matchErr
	}
	if session.ctx.Err() != nil {
		return DurableWaitRecord{}, WaitDecision{}, false, context.Cause(session.ctx)
	}
	return prepareWaitDecision(record, delivery, source.Revision, session.lease.Incarnation, now, matched)
}

func (r *DurableRunner[T, E]) applyWaitContinuation(ctx context.Context, target ExecutionEnvelope,
	spec DurableWaitSpec, delivery WaitDelivery, contract WaitDeliveryContract[T],
) (ExecutionEnvelope, error) {
	if contract.Apply == nil || target.Activation == math.MaxUint64 {
		return ExecutionEnvelope{}, ErrWaitInvalid
	}
	if ctx.Err() != nil {
		return ExecutionEnvelope{}, context.Cause(ctx)
	}
	state, err := r.stateCodec.Unmarshal(bytes.Clone(target.Progress.StatePayload))
	if err != nil {
		return ExecutionEnvelope{}, err
	}
	delivery.Payload = bytes.Clone(delivery.Payload)
	if ctx.Err() != nil {
		return ExecutionEnvelope{}, context.Cause(ctx)
	}
	state, err = contract.Apply(ctx, state, delivery)
	if err != nil {
		return ExecutionEnvelope{}, err
	}
	if ctx.Err() != nil {
		return ExecutionEnvelope{}, context.Cause(ctx)
	}
	encoded, err := r.stateCodec.Marshal(state)
	if err != nil {
		return ExecutionEnvelope{}, err
	}
	target.Progress.StatePayload = bytes.Clone(encoded)
	target.Progress.ExecutionPointer = spec.EventPointer
	if delivery.Kind == WaitTimer {
		target.Progress.ExecutionPointer = spec.TimeoutPointer
	}
	target.Activation++
	target.Progress.JournalReferences = nil
	target.Progress.ChildGroupReferences = nil
	return target, nil
}
