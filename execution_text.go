package flowy

import "unicode/utf8"

// validRuntimeText prevents JSON normalization from aliasing runtime identities.
// Empty strings are handled by each field's own required/optional contract.
func validRuntimeText(values ...string) bool {
	for _, value := range values {
		if !utf8.ValidString(value) {
			return false
		}
	}
	return true
}

func validMigrationText(state ExecutionProgress) bool {
	if !validRuntimeText(string(state.ExecutionPointer)) {
		return false
	}
	for key, pointer := range state.ChildCursors {
		if !validRuntimeText(key, string(pointer)) {
			return false
		}
	}
	for _, references := range []map[string]string{state.JournalReferences, state.ChildGroupReferences} {
		for key, identity := range references {
			if !validRuntimeText(key, identity) {
				return false
			}
		}
	}
	return true
}

func validChildGroupText(group ChildGroupRecord) bool {
	plan := group.Plan
	if !validRuntimeText(group.ParentID, string(group.Node), plan.Key, plan.Label, plan.MergeLabel,
		plan.BudgetLabel, plan.CancelLabel) || !validChildCapacity(group.Capacity) {
		return false
	}
	for _, spec := range plan.Children {
		if !validRuntimeText(spec.ID) || !validChildCapacity(spec.Allocation) {
			return false
		}
	}
	for _, child := range group.Children {
		if !validRuntimeText(child.Spec.ID, child.ExecutionID, child.WaitID, child.Error) ||
			!validChildCapacity(child.Spec.Allocation) {
			return false
		}
	}
	if group.CancelRequest != nil && !validRuntimeText(group.CancelRequest.ID, group.CancelRequest.Reason) {
		return false
	}
	for _, decision := range group.BudgetReturns {
		if !validRuntimeText(decision.DecisionID, decision.Reason, decision.Evidence) {
			return false
		}
	}
	return true
}
