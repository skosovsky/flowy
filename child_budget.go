package flowy

// PlanChildBudgetReturn computes detached unused named units from an explicit
// host consumption claim. It never releases a reservation or changes runtime
// accounting. Completion alone is not evidence of zero consumption.
func PlanChildBudgetReturn(child ChildRecord, used map[string]int) (map[string]int, error) {
	if validateChildRecordState(child) != nil {
		return nil, ErrChildJoinInvalid
	}
	if !settledChild(child) {
		return nil, ErrChildrenUnresolved
	}
	for name, units := range used {
		allocated, exists := child.Spec.Allocation[name]
		if !exists || name == "" || units < 0 || allocated < units {
			return nil, ErrBudgetExceeded
		}
	}
	returned := make(map[string]int, len(child.Spec.Allocation))
	for name, allocated := range child.Spec.Allocation {
		consumed, explicit := used[name]
		if name == "" || allocated < 0 || !explicit {
			return nil, ErrBudgetExceeded
		}
		returned[name] = allocated - consumed
	}
	return returned, nil
}
