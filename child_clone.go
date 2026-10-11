package flowy

import (
	"bytes"
	"maps"
	"slices"
)

// detachedChildGroup only copies memory ownership. Wire admission remains separate.
func detachedChildGroup(group ChildGroupRecord) ChildGroupRecord {
	group.Plan.Children = slices.Clone(group.Plan.Children)
	for i := range group.Plan.Children {
		group.Plan.Children[i] = cloneChildSpec(group.Plan.Children[i])
	}
	group.Children = slices.Clone(group.Children)
	for i := range group.Children {
		group.Children[i] = detachedChildRecord(group.Children[i])
	}
	group.MergedIDs, group.MergedResult = slices.Clone(group.MergedIDs), bytes.Clone(group.MergedResult)
	group.Capacity = maps.Clone(group.Capacity)
	if group.CancelRequest != nil {
		request := *group.CancelRequest
		group.CancelRequest = &request
	}
	group.BudgetReturns = maps.Clone(group.BudgetReturns)
	for id, record := range group.BudgetReturns {
		record.Used, record.Returned = maps.Clone(record.Used), maps.Clone(record.Returned)
		group.BudgetReturns[id] = record
	}
	return group
}

func detachedChildRecord(child ChildRecord) ChildRecord {
	child.Spec, child.Result = cloneChildSpec(child.Spec), bytes.Clone(child.Result)
	if child.WaitResolution != nil {
		record := *child.WaitResolution
		child.WaitResolution = &record
	}
	if child.OutcomeResolution != nil {
		record := *child.OutcomeResolution
		child.OutcomeResolution = &record
	}
	if child.CancelConfirmation != nil {
		record := *child.CancelConfirmation
		child.CancelConfirmation = &record
	}
	return child
}
