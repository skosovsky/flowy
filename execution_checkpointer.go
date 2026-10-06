package flowy

import (
	"bytes"
	"context"
	"sync"
)

type executionCheckpointer[T, E any] struct {
	mu                       sync.Mutex
	store                    ExecutionStore
	lease                    ExecutionLease
	envelope                 ExecutionEnvelope
	stateCodec               StateSerializer[T]
	effectsCodec             StateSerializer[[]E]
	stepRevision             uint64
	clock                    ExecutionClock
	retryRandom              func() uint64
	persistenceFailed        bool
	childCancellationSignals map[string]chan struct{}
	waitBackend              DurableWaitBackend
	waitProfile              *WaitCapabilityProfile
	forkPolicy               *ForkExecutionPolicy
	forkMode                 ForkActivityMode
}

func (c *executionCheckpointer[T, E]) Save(
	ctx context.Context,
	expectedRevision uint64,
	snapshot Snapshot[T, E],
) (uint64, error) {
	c.mu.Lock()
	event := executionObservation(c.envelope, LifecycleCheckpoint, LifecycleFailed)
	revision, err := c.saveLocked(ctx, expectedRevision, snapshot)
	c.mu.Unlock()
	observeCheckpointOutcome(ctx, event, revision, err)
	return revision, err
}

func (c *executionCheckpointer[T, E]) saveLocked(
	ctx context.Context,
	expectedRevision uint64,
	snapshot Snapshot[T, E],
) (uint64, error) {
	if expectedRevision != c.stepRevision || snapshot.ThreadID != c.lease.ExecutionID {
		return 0, ErrConcurrencyConflict
	}
	if err := c.resolvedActivitiesLocked(); err != nil {
		return 0, err
	}
	return c.persistSnapshotLocked(ctx, cloneExecutionEnvelope(c.envelope), snapshot)
}

func (c *executionCheckpointer[T, E]) persistSnapshotLocked(
	ctx context.Context,
	target ExecutionEnvelope,
	snapshot Snapshot[T, E],
) (uint64, error) {
	state, err := c.stateCodec.Marshal(snapshot.State)
	if err != nil {
		c.persistenceFailed = true
		return 0, err
	}
	effects, err := c.effectsCodec.Marshal(snapshot.Effects)
	if err != nil {
		c.persistenceFailed = true
		return 0, err
	}
	target.Progress.StatePayload, target.EffectsPayload = bytes.Clone(state), bytes.Clone(effects)
	target.Progress.ExecutionPointer, target.RunMeta = snapshot.ExecutionPointer, snapshot.RunMeta
	committed, err := commitExecution(ctx, c.store, c.envelope.Revision, c.lease, target)
	if err != nil {
		c.persistenceFailed = true
		return 0, err
	}
	c.envelope = cloneExecutionEnvelope(committed)
	c.stepRevision = committed.Revision
	return committed.Revision, nil
}

func (c *executionCheckpointer[T, E]) Load(_ context.Context, threadID string) (Snapshot[T, E], uint64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if threadID != c.lease.ExecutionID {
		return Snapshot[T, E]{}, 0, ErrInvalidSnapshot
	}
	return c.decode(c.envelope)
}

func (c *executionCheckpointer[T, E]) decode(envelope ExecutionEnvelope) (Snapshot[T, E], uint64, error) {
	if integrityErr := ValidateExecutionIntegrity(
		envelope,
		c.lease.ExecutionID,
		envelope.Revision,
	); integrityErr != nil {
		return Snapshot[T, E]{}, 0, integrityErr
	}
	if err := envelope.Descriptor.Check(c.envelope.Descriptor); err != nil {
		return Snapshot[T, E]{}, 0, err
	}
	if collectionsErr := validateExecutionCollections(envelope); collectionsErr != nil {
		return Snapshot[T, E]{}, 0, collectionsErr
	}
	state, err := c.stateCodec.Unmarshal(bytes.Clone(envelope.Progress.StatePayload))
	if err != nil {
		return Snapshot[T, E]{}, 0, err
	}
	effects, err := c.effectsCodec.Unmarshal(bytes.Clone(envelope.EffectsPayload))
	if err != nil {
		return Snapshot[T, E]{}, 0, err
	}
	envelope = cloneExecutionEnvelope(envelope)
	return Snapshot[T, E]{
		ThreadID:         envelope.ExecutionID,
		Revision:         envelope.Revision,
		ExecutionPointer: envelope.Progress.ExecutionPointer,
		State:            state,
		Effects:          effects,
		RunMeta:          envelope.RunMeta,
	}, envelope.Revision, nil
}

func (c *executionCheckpointer[T, E]) GetHistory(
	ctx context.Context,
	threadID string,
	limit int,
) ([]Snapshot[T, E], error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if threadID != c.lease.ExecutionID {
		return nil, ErrInvalidSnapshot
	}
	history, ok := c.store.(ExecutionHistoryStore)
	if !ok {
		return nil, ErrExecutionHistoryUnsupported
	}
	count := c.envelope.Revision
	if limit > 0 {
		count = min(count, uint64(limit))
	}
	result := make([]Snapshot[T, E], 0)
	for index := range count {
		envelope, err := history.LoadCheckpoint(ctx, threadID, c.envelope.Revision-index)
		if err != nil {
			return nil, err
		}
		if integrityErr := ValidateExecutionIntegrity(
			envelope,
			threadID,
			c.envelope.Revision-index,
		); integrityErr != nil {
			return nil, integrityErr
		}
		snapshot, _, err := c.decode(envelope)
		if err != nil {
			return nil, err
		}
		result = append(result, snapshot)
	}
	return result, nil
}

func (*executionCheckpointer[T, E]) Prune(context.Context, string, int) error {
	return ErrExecutionCapability
}
func (*executionCheckpointer[T, E]) Delete(context.Context, string) error {
	return ErrExecutionCapability
}
func (*executionCheckpointer[T, E]) DeleteIfIdle(context.Context, string) error {
	return ErrExecutionCapability
}

func (c *executionCheckpointer[T, E]) commitStepLocked(
	ctx context.Context,
	step directiveStep[T, E],
	directive directiveKind,
	state T,
	meta RunMetadata,
	effects []E,
) (uint64, error) {
	if directive == directiveWait {
		// Await already atomically saved state plus arm, or failed recoverably.
		return c.envelope.Revision, nil
	}
	pointer := ExecutionPointer(step.nextNode)
	if err := c.resolvedActivitiesLocked(); err != nil {
		return 0, err
	}
	target := cloneExecutionEnvelope(c.envelope)
	if step.terminal {
		if step.result == nil {
			return c.envelope.Revision, step.err
		}
		if step.result.Status != RunStatusCompleted && step.result.Status != RunStatusFailed {
			return c.envelope.Revision, nil
		}
		pointer = step.result.ExecutionPointer
		meta = step.result.RunMeta
		target.Terminal = makeExecutionTerminal(step.result.Status, step.result.Reason, step.err)
	}
	// A self retry is the same node activation. A normal graph cycle is not.
	if directive != directiveRetry || pointer != c.envelope.Progress.ExecutionPointer {
		target.Activation++
		target.Progress.JournalReferences = nil
		target.Progress.ChildGroupReferences = nil
	}
	return c.persistSnapshotLocked(
		ctx,
		target,
		Snapshot[T, E]{
			ThreadID:         c.lease.ExecutionID,
			Revision:         0,
			ExecutionPointer: pointer,
			State:            state,
			RunMeta:          meta,
			Effects:          effects,
		},
	)
}

func (c *executionCheckpointer[T, E]) resolvedActivitiesLocked() error {
	waits, err := executionWaits(c.envelope)
	if err != nil {
		return err
	}
	for _, wait := range waits {
		if wait.State == WaitArmed {
			return ErrWaitUnresolved
		}
	}
	if childrenErr := c.resolvedChildGroupsLocked(); childrenErr != nil {
		return childrenErr
	}
	journal, err := c.readJournal()
	if err != nil {
		return err
	}
	for _, activity := range journal {
		if activity.Activation != c.envelope.Activation {
			continue
		}
		switch activity.State {
		case ActivityRunning:
			return ErrActivityBusy
		case ActivityUnknown:
			return ErrActivityUnknown
		case ActivityPrepared:
			if len(activity.Attempts) > 0 {
				return activityRecordError(activity, c.envelope.Revision)
			}
			return ErrActivityConflict
		case ActivityFailed:
			// The host may handle a definitive failed outcome without redispatch.
			if activity.Classification == ActivityAmbiguous {
				return ErrActivityUnknown
			}
		case ActivityCompleted:
			// Only a persisted resolved outcome permits cursor advancement.
		default:
			return ErrActivityConflict
		}
	}
	return nil
}
