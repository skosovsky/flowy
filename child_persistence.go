package flowy

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"maps"
	"slices"
)

type childGroupContextKey struct{}
type childGroupBackend interface {
	prepareChildren(context.Context, ChildGroupPlan, map[string]int) (ChildGroupRecord, error)
	runChildren(context.Context, ChildGroupPlan, map[string]int, ChildDispatcher) (ChildGroupRecord, error)
	joinChildren(context.Context, ChildGroupRecord, ChildMerge) ([]byte, error)
	cancelChildren(context.Context, ChildGroupRecord, ChildCancelRequest, ChildCancelNotifier) (ChildGroupRecord, error)
	returnChildBudget(context.Context, ChildGroupRecord, ChildBudgetReturn) (ChildGroupRecord, error)
}

// PrepareChildren durably fixes the child plan before any launch. Repeated calls
// replay the original detached group; they do not replenish its allocation.
func PrepareChildren(ctx context.Context, plan ChildGroupPlan, available map[string]int) (ChildGroupRecord, error) {
	backend, ok := ctx.Value(childGroupContextKey{}).(childGroupBackend)
	if !ok {
		return ChildGroupRecord{}, ErrExecutionCapability
	}
	return backend.prepareChildren(ctx, plan, available)
}

func childPlanBytes(plan ChildGroupPlan) []byte {
	plan.Children = slices.Clone(plan.Children)
	slices.SortFunc(plan.Children, func(a, b ChildSpec) int { return compareChildIDs(a.ID, b.ID) })
	encoded, _ := json.Marshal(plan)
	return encoded
}

func detachedChildGroup(group ChildGroupRecord) ChildGroupRecord {
	encoded, _ := json.Marshal(group)
	var result ChildGroupRecord
	_ = json.Unmarshal(encoded, &result)
	return result
}

func (c *executionCheckpointer[T, E]) prepareChildren(
	ctx context.Context,
	plan ChildGroupPlan,
	available map[string]int,
) (ChildGroupRecord, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	groups, err := executionChildGroups(c.envelope)
	if err != nil {
		return ChildGroupRecord{}, err
	}
	identity := boundChildGroupIdentity(c.envelope, plan.Key)
	if prior, exists := groups[identity]; exists {
		if !maps.Equal(prior.Capacity, available) {
			return ChildGroupRecord{}, ErrBudgetExceeded
		}
		if !bytes.Equal(childPlanBytes(prior.Plan), childPlanBytes(plan)) {
			return ChildGroupRecord{}, ErrChildJoinInvalid
		}
		return detachedChildGroup(prior), nil
	}
	group, err := PlanChildGroup(
		c.envelope.ExecutionID,
		c.envelope.Progress.ExecutionPointer,
		c.envelope.Activation,
		plan,
		available,
	)
	if err != nil {
		return ChildGroupRecord{}, err
	}
	groups[identity] = group
	if budgetErr := validateChildCapacityLedger(groups); budgetErr != nil {
		return ChildGroupRecord{}, budgetErr
	}
	target := cloneExecutionEnvelope(c.envelope)
	target.ChildrenPayload, err = json.Marshal(groups)
	if err != nil {
		return ChildGroupRecord{}, err
	}
	committed, err := c.store.CommitExecution(ctx, c.envelope.Revision, c.lease, target)
	if err != nil {
		c.persistenceFailed = true
		return ChildGroupRecord{}, err
	}
	c.envelope = cloneExecutionEnvelope(committed)
	return detachedChildGroup(group), nil
}

func executionChildGroups(envelope ExecutionEnvelope) (map[string]ChildGroupRecord, error) {
	groups := make(map[string]ChildGroupRecord)
	if len(envelope.ChildrenPayload) == 0 {
		return groups, validateChildGroupReferences(envelope, groups)
	}
	if err := json.Unmarshal(envelope.ChildrenPayload, &groups); err != nil {
		return nil, errors.Join(ErrExecutionCorrupt, err)
	}
	if groups == nil {
		return nil, ErrExecutionCorrupt
	}
	for identity, group := range groups {
		if !validChildGroupProvenance(group, envelope.Revision) {
			return nil, ErrExecutionCorrupt
		}
		if identity != childExecutionIdentity(group.ParentID, group.Node, group.Activation, group.Plan.Key, "") ||
			group.ParentID != envelope.ExecutionID || group.Activation == 0 || group.Activation > envelope.Activation ||
			validateChildGroup(group) != nil {
			return nil, ErrExecutionCorrupt
		}
	}
	if referenceErr := validateChildGroupReferences(envelope, groups); referenceErr != nil {
		return nil, referenceErr
	}
	if ledgerErr := validateChildCapacityLedger(groups); ledgerErr != nil {
		return nil, errors.Join(ErrExecutionCorrupt, ledgerErr)
	}
	return groups, nil
}

func validChildGroupProvenance(group ChildGroupRecord, revision uint64) bool {
	if group.CancelRequest != nil && group.CancelRequest.SourceRevision >= revision {
		return false
	}
	for _, child := range group.Children {
		if child.CancelConfirmation != nil && (child.CancelConfirmation.SourceRevision >= revision ||
			group.CancelRequest == nil || child.CancelConfirmation.RequestID != group.CancelRequest.ID ||
			child.CancelConfirmation.SourceRevision <= group.CancelRequest.SourceRevision) {
			return false
		}
		if child.WaitResolution != nil && child.WaitResolution.SourceRevision >= revision {
			return false
		}
	}
	for _, returned := range group.BudgetReturns {
		if returned.SourceRevision >= revision {
			return false
		}
	}
	return true
}

func validateChildGroup(group ChildGroupRecord) error {
	base, err := PlanChildGroup(group.ParentID, group.Node, group.Activation, group.Plan, group.Capacity)
	if err != nil {
		return ErrExecutionCorrupt
	}
	if len(group.Children) != len(base.Children) || !validChildCancellation(group) || !validChildJoin(group) {
		return ErrExecutionCorrupt
	}
	if !validChildBudgetReturns(group) {
		return ErrExecutionCorrupt
	}
	if !bytes.Equal(childPlanBytes(base.Plan), childPlanBytes(group.Plan)) {
		return ErrExecutionCorrupt
	}
	for index, child := range group.Children {
		original := base.Children[index]
		if child.ExecutionID != original.ExecutionID ||
			!bytes.Equal(childSpecEncoding(child.Spec), childSpecEncoding(original.Spec)) ||
			validateChildRecordState(child) != nil {
			return ErrExecutionCorrupt
		}
	}
	return nil
}

func childSpecEncoding(spec ChildSpec) []byte { encoded, _ := json.Marshal(spec); return encoded }

func (c *executionCheckpointer[T, E]) persistChildGroupsLocked(
	ctx context.Context,
	groups map[string]ChildGroupRecord,
) error {
	target := cloneExecutionEnvelope(c.envelope)
	payload, err := json.Marshal(groups)
	if err != nil {
		return err
	}
	target.ChildrenPayload = payload
	committed, err := c.store.CommitExecution(ctx, c.envelope.Revision, c.lease, target)
	if err != nil {
		c.persistenceFailed = true
		return err
	}
	c.envelope = cloneExecutionEnvelope(committed)
	return nil
}

func (c *executionCheckpointer[T, E]) resolvedChildGroupsLocked() error {
	groups, err := executionChildGroups(c.envelope)
	if err != nil {
		return err
	}
	for _, group := range groups {
		if group.Activation == c.envelope.Activation && len(group.MergedIDs) != len(group.Children) {
			return ErrChildrenUnresolved
		}
	}
	return nil
}
