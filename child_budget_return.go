package flowy

import (
	"bytes"
	"context"
	"maps"
	"time"
)

// ChildBudgetReturn is a host usage claim, not a monetary ledger operation.
type ChildBudgetReturn struct {
	ChildID       string
	ChildRevision uint64
	DecisionID    string
	Reason        string
	Evidence      string
	Used          map[string]int
}

type ChildBudgetReturnRecord struct {
	ChildRevision  uint64         `json:"child_revision"`
	DecisionID     string         `json:"decision_id"`
	Reason         string         `json:"reason"`
	Evidence       string         `json:"evidence"`
	Used           map[string]int `json:"used"`
	Returned       map[string]int `json:"returned"`
	SourceRevision uint64         `json:"source_revision"`
	Incarnation    uint64         `json:"incarnation"`
	At             time.Time      `json:"at"`
}

// ReturnChildBudget commits unused named units once. The current detached group
// is an exact assertion; replay uses the recorded decision without replenishing
// capacity again. Unknown children cannot release their allocation.
func ReturnChildBudget(ctx context.Context, group ChildGroupRecord, claim ChildBudgetReturn) (ChildGroupRecord, error) {
	backend, ok := ctx.Value(childGroupContextKey{}).(childGroupBackend)
	if !ok {
		return ChildGroupRecord{}, ErrExecutionCapability
	}
	if claim.ChildID == "" || claim.ChildRevision == 0 || claim.DecisionID == "" || claim.Reason == "" ||
		!validRuntimeText(claim.ChildID, claim.DecisionID, claim.Evidence) ||
		claim.Evidence == "" || !validChildGroupText(group) {
		return ChildGroupRecord{}, ErrChildJoinInvalid
	}
	claim.Used = maps.Clone(claim.Used)
	return backend.returnChildBudget(ctx, group, claim)
}

func (c *executionCheckpointer[T, E]) returnChildBudget(ctx context.Context, expected ChildGroupRecord,
	claim ChildBudgetReturn) (ChildGroupRecord, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	groups, err := executionChildGroups(c.envelope)
	if err != nil {
		return ChildGroupRecord{}, err
	}
	identity := childExecutionIdentity(expected.ParentID, expected.Node, expected.Activation, expected.Plan.Key, "")
	group, exists := groups[identity]
	if !exists || !bytes.Equal(childGroupEncoding(group), childGroupEncoding(expected)) ||
		!currentChildGroup(c.envelope, group) {
		return ChildGroupRecord{}, ErrChildRevision
	}
	if prior, returned := group.BudgetReturns[claim.ChildID]; returned {
		if prior.DecisionID != claim.DecisionID || prior.ChildRevision != claim.ChildRevision ||
			prior.Reason != claim.Reason || prior.Evidence != claim.Evidence || !maps.Equal(prior.Used, claim.Used) {
			return ChildGroupRecord{}, ErrChildRevision
		}
		return detachedChildGroup(group), nil
	}
	child, found := childForBudgetReturn(group, claim.ChildID)
	if !found || child.Revision != claim.ChildRevision {
		return ChildGroupRecord{}, ErrChildRevision
	}
	returned, err := PlanChildBudgetReturn(child, claim.Used)
	if err != nil {
		return ChildGroupRecord{}, err
	}
	if group.BudgetReturns == nil {
		group.BudgetReturns = make(map[string]ChildBudgetReturnRecord)
	}
	group.BudgetReturns[claim.ChildID] = ChildBudgetReturnRecord{
		ChildRevision:  claim.ChildRevision,
		DecisionID:     claim.DecisionID,
		Reason:         claim.Reason,
		Evidence:       claim.Evidence,
		Used:           maps.Clone(claim.Used),
		Returned:       returned,
		SourceRevision: c.envelope.Revision,
		Incarnation:    c.lease.Incarnation,
		At:             c.clock.Now().UTC(),
	}
	groups[identity] = group
	if err := c.persistChildGroupsLocked(ctx, groups); err != nil {
		return ChildGroupRecord{}, err
	}
	return detachedChildGroup(group), nil
}

func childForBudgetReturn(group ChildGroupRecord, id string) (ChildRecord, bool) {
	for _, child := range group.Children {
		if child.Spec.ID == id {
			return child, true
		}
	}
	return ChildRecord{}, false
}

func validChildBudgetReturns(group ChildGroupRecord) bool {
	for id, record := range group.BudgetReturns {
		child, exists := childForBudgetReturn(group, id)
		if !exists || child.Revision != record.ChildRevision || record.DecisionID == "" || record.Reason == "" ||
			record.Evidence == "" ||
			record.SourceRevision == 0 ||
			record.Incarnation == 0 ||
			record.At.IsZero() {
			return false
		}
		returned, err := PlanChildBudgetReturn(child, record.Used)
		if err != nil || !maps.Equal(returned, record.Returned) {
			return false
		}
	}
	return true
}

func validChildCapacity(capacity map[string]int) bool {
	for name, units := range capacity {
		if name == "" || !validRuntimeText(name) || units < 0 {
			return false
		}
	}
	return true
}

func validateChildCapacityLedger(groups map[string]ChildGroupRecord) error {
	remaining := make(map[uint64]map[string]int)
	capacities := make(map[uint64]map[string]int)
	for _, group := range groups {
		if prior, exists := capacities[group.Activation]; exists {
			if !maps.Equal(prior, group.Capacity) {
				return ErrBudgetExceeded
			}
		} else {
			capacities[group.Activation] = group.Capacity
			remaining[group.Activation] = maps.Clone(group.Capacity)
		}
		for _, child := range group.Children {
			for name, allocation := range child.Spec.Allocation {
				outstanding := allocation - group.BudgetReturns[child.Spec.ID].Returned[name]
				available, exists := remaining[group.Activation][name]
				if !exists || outstanding < 0 || outstanding > available {
					return ErrBudgetExceeded
				}
				remaining[group.Activation][name] = available - outstanding
			}
		}
	}
	return nil
}
