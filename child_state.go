package flowy

func validateChildRecordState(child ChildRecord) error {
	if !validRuntimeText(child.WaitID) || !validChildWaitResolution(child) || !validChildCancelConfirmation(child) {
		return ErrExecutionCorrupt
	}
	return validateChildStateMachine(child)
}

func validateChildStateMachine(child ChildRecord) error {
	if child.CancelConfirmed && (!child.CancelRequested || child.State != ChildCanceled) {
		return ErrExecutionCorrupt
	}
	noOutcome := child.Error == "" && len(child.Result) == 0 && child.WaitID == ""
	settled := child.Revision > 2 && child.Incarnation > 0
	valid := false
	switch child.State {
	case ChildPlanned:
		valid = child.Revision == 0 && child.Incarnation == 0 && noOutcome
	case ChildQueued:
		valid = child.Revision > 0 && child.Incarnation == 0 && noOutcome
	case ChildRunning:
		valid = child.Revision > 1 && child.Incarnation > 0 && noOutcome
	case ChildCompleted:
		valid = settled && child.Error == "" && child.WaitID == ""
	case ChildFailed:
		valid = settled && child.Error != "" && child.WaitID == ""
	case ChildWaiting:
		valid = settled && child.WaitID != "" && child.Error == ""
	case ChildUnknown:
		valid = settled && child.WaitID == ""
	case ChildCanceled:
		valid = child.Revision > 0 && child.CancelConfirmed
	}
	if valid {
		return nil
	}
	return ErrExecutionCorrupt
}
