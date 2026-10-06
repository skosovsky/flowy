package flowy

// ValidateExecutionForkAnchor compares raw lineage with independently retained
// creation metadata. Stores must not derive anchor from the envelope being read.
func ValidateExecutionForkAnchor(envelope ExecutionEnvelope, anchor *ForkLineage) error {
	previous := envelope
	previous.Fork = anchor
	return ValidateExecutionForkTransition(&previous, envelope)
}

// ValidateExecutionForkTransition enforces immutable creation provenance.
// Stores call this with their committed predecessor under the same atomic
// fencing/OCC boundary as the write. Nil previous means initial creation only.
func ValidateExecutionForkTransition(previous *ExecutionEnvelope, next ExecutionEnvelope) error {
	if next.Fork != nil && (next.Fork.Validate() != nil || next.Fork.TargetID != next.ExecutionID) {
		return ErrExecutionCorrupt
	}
	if previous == nil {
		return nil
	}
	if previous.ExecutionID != next.ExecutionID || (previous.Fork == nil) != (next.Fork == nil) {
		return ErrExecutionCorrupt
	}
	if previous.Fork != nil && *previous.Fork != *next.Fork {
		return ErrExecutionCorrupt
	}
	return nil
}
