package flowy

func boundChildGroupIdentity(envelope ExecutionEnvelope, key string) string {
	if identity, exists := envelope.Progress.ChildGroupReferences[key]; exists {
		return identity
	}
	return childExecutionIdentity(
		envelope.ExecutionID,
		envelope.Progress.ExecutionPointer,
		envelope.Activation,
		key,
		"",
	)
}

func currentChildGroup(envelope ExecutionEnvelope, group ChildGroupRecord) bool {
	identity := childExecutionIdentity(group.ParentID, group.Node, group.Activation, group.Plan.Key, "")
	return group.ParentID == envelope.ExecutionID && group.Activation == envelope.Activation &&
		boundChildGroupIdentity(envelope, group.Plan.Key) == identity
}

func validateChildGroupReferences(envelope ExecutionEnvelope, groups map[string]ChildGroupRecord) error {
	for key, identity := range envelope.Progress.ChildGroupReferences {
		group, exists := groups[identity]
		if !exists || key != group.Plan.Key || group.Activation != envelope.Activation {
			return ErrExecutionCorrupt
		}
	}
	for _, group := range groups {
		if len(group.MergedIDs) != len(group.Children) && !currentChildGroup(envelope, group) {
			return ErrExecutionCorrupt
		}
	}
	return nil
}
