package flowy

import (
	"encoding/json"
	"maps"
)

// ValidateExecutionLifecycleBoundary checks runtime-owned dependencies without
// decoding opaque state/effects. Stores repeat this under publication locks.
// Allocation usage must be explicitly accounted for; timeout proves nothing.
func ValidateExecutionLifecycleBoundary(source ExecutionEnvelope) error {
	if err := ValidateExecutionIntegrity(source, source.ExecutionID, source.Revision); err != nil {
		return err
	}
	if err := source.Descriptor.Validate(); err != nil {
		return err
	}
	if err := validateExecutionSourceMetadata(source); err != nil {
		return err
	}
	if err := validateExecutionCollections(source); err != nil {
		return err
	}
	if source.Terminal == nil && source.RunMeta.StepCount <= 0 {
		return ErrExecutionLifecycleUnsafe
	}
	journal, err := executionActivityJournal(source)
	if err != nil {
		return err
	}
	if err = validateLifecycleJournal(source, journal); err != nil {
		return err
	}
	groups, err := executionChildGroups(source)
	if err != nil {
		return err
	}
	if err = validateLifecycleChildren(source, groups); err != nil {
		return err
	}
	waits, err := executionWaits(source)
	if err != nil {
		return err
	}
	for _, wait := range waits {
		if wait.State == WaitArmed {
			return ErrExecutionLifecycleUnsafe
		}
	}
	if len(source.Progress.ChildCursors) != 0 {
		return ErrExecutionLifecycleUnsafe
	}
	return nil
}

func validateRolloverRecordLimit(source ExecutionEnvelope, policy RolloverPolicy) error {
	if err := policy.Validate(); err != nil {
		return err
	}
	encoded, encodeErr := json.Marshal(source)
	if encodeErr != nil {
		return encodeErr
	}
	if len(encoded) > policy.MaxAggregateBytes {
		return ErrExecutionLifecycleLimit
	}
	journal, err := executionActivityJournal(source)
	if err != nil {
		return err
	}
	groups, err := executionChildGroups(source)
	if err != nil {
		return err
	}
	waits, err := executionWaits(source)
	if err != nil {
		return err
	}
	remaining := policy.MaxRecords - len(journal)
	if remaining < 0 {
		return ErrExecutionLifecycleLimit
	}
	for _, group := range groups {
		if len(group.Children) > remaining {
			return ErrExecutionLifecycleLimit
		}
		remaining -= len(group.Children)
	}
	if len(waits) > remaining {
		return ErrExecutionLifecycleLimit
	}
	return nil
}

// PrepareExecutionRolloverPublication validates and seals an atomic pair. It
// publishes nothing by itself. Store calls it with its authoritative source
// and live lease under locks, and must commit BOTH or NEITHER plus anchors.
func PrepareExecutionRolloverPublication(
	source, target ExecutionEnvelope,
	lease ExecutionLease,
) (ExecutionEnvelope, ExecutionEnvelope, RolloverReceipt, error) {
	if source.ExecutionID != lease.ExecutionID || lease.Incarnation == 0 || lease.Owner == "" {
		return ExecutionEnvelope{}, ExecutionEnvelope{}, RolloverReceipt{}, ErrLeaseLost
	}
	if err := validateRolloverSource(source); err != nil {
		return ExecutionEnvelope{}, ExecutionEnvelope{}, RolloverReceipt{}, err
	}
	if err := validateRolloverTarget(source, target); err != nil {
		return ExecutionEnvelope{}, ExecutionEnvelope{}, RolloverReceipt{}, err
	}
	if err := validateRolloverRecordLimit(source, target.Rollover.Policy); err != nil {
		return ExecutionEnvelope{}, ExecutionEnvelope{}, RolloverReceipt{}, err
	}
	target = cloneExecutionEnvelope(target)
	target.Revision = 1
	sealedTarget, err := SealExecutionEnvelope(target)
	if err != nil {
		return ExecutionEnvelope{}, ExecutionEnvelope{}, RolloverReceipt{}, err
	}
	encoded, encodeErr := json.Marshal(sealedTarget)
	if encodeErr != nil {
		return ExecutionEnvelope{}, ExecutionEnvelope{}, RolloverReceipt{}, encodeErr
	}
	if len(encoded) > target.Rollover.Policy.MaxTargetBytes {
		return ExecutionEnvelope{}, ExecutionEnvelope{}, RolloverReceipt{}, ErrExecutionLifecycleLimit
	}
	receipt := RolloverReceipt{
		Lineage: *target.Rollover,
		Target: HistoricalCheckpointReference{
			ExecutionID: target.ExecutionID,
			Revision:    1,
			Digest:      sealedTarget.Digest,
		},
		SourceRevision: source.Revision + 1,
	}
	transferred := cloneExecutionEnvelope(source)
	transferred.Revision++
	transferred.Transfer = &receipt
	transferred.Terminal = &ExecutionTerminal{Status: RunStatusTransferred, Reason: "rollover", Failure: nil}
	transferred, err = SealExecutionEnvelope(transferred)
	return transferred, sealedTarget, receipt, err
}

func validateRolloverTarget(source, target ExecutionEnvelope) error {
	lineage := target.Rollover
	if lineage == nil || lineage.Validate() != nil || lineage.TargetID != target.ExecutionID ||
		lineage.Source.ExecutionID != source.ExecutionID ||
		lineage.Source.Revision != source.Revision ||
		lineage.Source.Digest != source.Digest ||
		lineage.SourceDescriptor != source.Descriptor ||
		lineage.TargetDescriptor != target.Descriptor ||
		target.Revision != 0 ||
		target.Digest != "" ||
		target.Activation != 1 ||
		target.Transfer != nil ||
		target.Terminal != nil ||
		target.Fork != nil ||
		target.Import != nil ||
		target.Migration != nil ||
		len(target.JournalPayload) != 0 ||
		len(target.ChildrenPayload) != 0 ||
		len(target.WaitsPayload) != 0 ||
		len(target.Progress.ChildCursors) != 0 ||
		len(target.Progress.JournalReferences) != 0 ||
		len(target.Progress.ChildGroupReferences) != 0 {
		return ErrExecutionRolloverConflict
	}
	if err := validateExecutionCollections(target); err != nil {
		return err
	}
	if target.RunMeta.StepCount != 0 || !maps.Equal(target.RunMeta.RetryCounts, source.RunMeta.RetryCounts) ||
		!maps.Equal(target.RunMeta.BudgetCounts, source.RunMeta.BudgetCounts) {
		return ErrExecutionLifecycleUnsafe
	}
	limit := lineage.Policy.MaxTargetBytes
	if len(target.Progress.StatePayload) > limit ||
		len(target.EffectsPayload) > limit-len(target.Progress.StatePayload) {
		return ErrExecutionLifecycleLimit
	}
	return nil
}

// ValidateExecutionRolloverTransition forbids ordinary writes from creating or
// changing transfer authority. Only atomic CommitRollover creates these fields.
func ValidateExecutionRolloverTransition(previous *ExecutionEnvelope, next ExecutionEnvelope) error {
	if previous == nil {
		if next.Rollover != nil || next.Transfer != nil {
			return ErrExecutionRolloverConflict
		}
		return nil
	}
	if previous.Transfer != nil {
		return ErrExecutionTransferred
	}
	if !equalRolloverLineage(previous.Rollover, next.Rollover) || next.Transfer != nil {
		return ErrExecutionCorrupt
	}
	return nil
}

func equalRolloverLineage(left, right *RolloverLineage) bool {
	return (left == nil && right == nil) || (left != nil && right != nil && *left == *right)
}

// ValidateExecutionLifecycleAnchors checks independently retained creation and
// transfer metadata. Older exact source checkpoints remain historical only.
func ValidateExecutionLifecycleAnchors(
	envelope ExecutionEnvelope,
	incoming *RolloverReceipt,
	outgoing *RolloverReceipt,
) error {
	var lineage *RolloverLineage
	if incoming != nil {
		lineage = &incoming.Lineage
	}
	if !equalRolloverLineage(envelope.Rollover, lineage) {
		return ErrExecutionCorrupt
	}
	if incoming != nil && (incoming.Validate() != nil || incoming.Target.ExecutionID != envelope.ExecutionID) {
		return ErrExecutionCorrupt
	}
	if incoming != nil && envelope.Revision == incoming.Target.Revision && envelope.Digest != incoming.Target.Digest {
		return ErrExecutionCorrupt
	}
	if outgoing == nil {
		if envelope.Transfer != nil {
			return ErrExecutionCorrupt
		}
		return nil
	}
	if outgoing.Validate() != nil || outgoing.Lineage.Source.ExecutionID != envelope.ExecutionID {
		return ErrExecutionCorrupt
	}
	if envelope.Revision < outgoing.SourceRevision {
		if envelope.Transfer != nil {
			return ErrExecutionCorrupt
		}
		if envelope.Revision == outgoing.Lineage.Source.Revision && envelope.Digest != outgoing.Lineage.Source.Digest {
			return ErrExecutionCorrupt
		}
		return nil
	}
	if envelope.Transfer == nil || *envelope.Transfer != *outgoing || envelope.Revision != outgoing.SourceRevision ||
		envelope.Terminal == nil ||
		envelope.Terminal.Status != RunStatusTransferred {
		return ErrExecutionCorrupt
	}
	return nil
}

// CloneExecutionEnvelope returns detached runtime data for host maintenance.
func CloneExecutionEnvelope(envelope ExecutionEnvelope) ExecutionEnvelope {
	return cloneExecutionEnvelope(envelope)
}

func validateLifecycleJournal(source ExecutionEnvelope, journal map[string]ActivityRecord) error {
	for _, record := range journal {
		if record.State != ActivityCompleted && record.State != ActivityFailed {
			return ErrExecutionLifecycleUnsafe
		}
		if source.Terminal == nil && record.Activation == source.Activation {
			return ErrExecutionLifecycleUnsafe
		}
	}
	return nil
}

func validateLifecycleChildren(source ExecutionEnvelope, groups map[string]ChildGroupRecord) error {
	for _, group := range groups {
		if len(group.MergedIDs) != len(group.Children) ||
			(source.Terminal == nil && group.Activation == source.Activation) {
			return ErrExecutionLifecycleUnsafe
		}
		for _, child := range group.Children {
			if !settledChild(child) {
				return ErrExecutionLifecycleUnsafe
			}
			if len(child.Spec.Allocation) != 0 {
				claim, exists := group.BudgetReturns[child.Spec.ID]
				if !exists || claim.ChildRevision != child.Revision {
					return ErrExecutionLifecycleUnsafe
				}
			}
		}
	}
	return nil
}

func validateRolloverSource(source ExecutionEnvelope) error {
	if source.Fork != nil || source.Transfer != nil ||
		(source.Terminal != nil && source.Terminal.Status != RunStatusCompleted) ||
		source.Revision == ^uint64(0) {
		return ErrExecutionLifecycleUnsafe
	}
	if err := ValidateExecutionLifecycleBoundary(source); err != nil {
		return err
	}
	return nil
}
