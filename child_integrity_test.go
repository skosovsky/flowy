package flowy

import (
	"encoding/json"
	"errors"
	"testing"
	"time"
)

func confirmedChildFixture(t *testing.T) ChildGroupRecord {
	t.Helper()
	group, err := PlanChildGroup("run", "node", 1, ChildGroupPlan{
		Key: "group", Label: "isolated", MergeLabel: "ordered", BudgetLabel: "fixed", CancelLabel: "confirmed",
		MaxConcurrency: 1, FailurePolicy: ChildCollectErrors, Children: []ChildSpec{{ID: "child"}},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	group.CancelRequested = true
	group.CancelRequest = &ChildCancelRequestRecord{
		ID:             "stop",
		Reason:         "host requested",
		SourceRevision: 2,
		Incarnation:    1,
		At:             now,
	}
	child := &group.Children[0]
	child.State, child.Revision, child.Incarnation = ChildCanceled, 5, 1
	child.CancelRequested, child.CancelConfirmed = true, true
	child.CancelConfirmation = &ChildCancelConfirmationRecord{
		RequestID:      "stop",
		DecisionID:     "confirmed",
		Reason:         "worker stopped",
		Evidence:       "host evidence",
		PriorState:     ChildWaiting,
		ChildRevision:  4,
		SourceRevision: 3,
		Incarnation:    2,
		At:             now,
	}
	return group
}

func TestChildGroupsRejectForgedJoinAndCancellationProvenance(t *testing.T) {
	for name, mutate := range map[string]func(*ChildGroupRecord){
		"request identity":             func(g *ChildGroupRecord) { g.CancelRequest.ID = "" },
		"request fence":                func(g *ChildGroupRecord) { g.CancelRequest.Incarnation = 0 },
		"request source":               func(g *ChildGroupRecord) { g.CancelRequest.SourceRevision = 4 },
		"request timestamp":            func(g *ChildGroupRecord) { g.CancelRequest.At = time.Time{} },
		"foreign confirmation":         func(g *ChildGroupRecord) { g.Children[0].CancelConfirmation.RequestID = "foreign" },
		"decision identity":            func(g *ChildGroupRecord) { g.Children[0].CancelConfirmation.DecisionID = "" },
		"evidence":                     func(g *ChildGroupRecord) { g.Children[0].CancelConfirmation.Evidence = "" },
		"confirmation fence":           func(g *ChildGroupRecord) { g.Children[0].CancelConfirmation.Incarnation = 0 },
		"confirmation timestamp":       func(g *ChildGroupRecord) { g.Children[0].CancelConfirmation.At = time.Time{} },
		"confirmation before request":  func(g *ChildGroupRecord) { g.Children[0].CancelConfirmation.SourceRevision = 2 },
		"confirmation future revision": func(g *ChildGroupRecord) { g.Children[0].CancelConfirmation.SourceRevision = 4 },
		"unaddressed child revision":   func(g *ChildGroupRecord) { g.Children[0].CancelConfirmation.ChildRevision-- },
		"completed prior state":        func(g *ChildGroupRecord) { g.Children[0].CancelConfirmation.PriorState = ChildCompleted },
		"missing confirmation":         func(g *ChildGroupRecord) { g.Children[0].CancelConfirmed = false },
		"retained live wait":           func(g *ChildGroupRecord) { g.Children[0].WaitID = "external" },
		"foreign merged identity":      func(g *ChildGroupRecord) { g.MergedIDs = []string{"foreign"} },
		"duplicate merged identity":    func(g *ChildGroupRecord) { g.MergedIDs = []string{"child", "child"} },
		"uncommitted merge result":     func(g *ChildGroupRecord) { g.MergedResult = []byte("invented") },
	} {
		t.Run(name, func(t *testing.T) {
			// Arrange: a valid group before the selected semantic corruption.
			group := confirmedChildFixture(t)
			if err := validateChildGroup(group); err != nil || !validChildGroupProvenance(group, 4) {
				t.Fatalf("invalid fixture: %v", err)
			}
			originalIdentity := childExecutionIdentity(group.ParentID, group.Node, group.Activation, group.Plan.Key, "")
			originalPayload, marshalErr := json.Marshal(map[string]ChildGroupRecord{originalIdentity: group})
			if marshalErr != nil {
				t.Fatal(marshalErr)
			}
			parsed, parseErr := executionChildGroups(ExecutionEnvelope{ExecutionID: "run", Revision: 4, Activation: 1,
				Progress: MigrationState{ExecutionPointer: "node"}, ChildrenPayload: originalPayload})
			if parseErr != nil || len(parsed) != 1 || parsed[originalIdentity].Children[0].State != ChildCanceled {
				t.Fatalf("valid fixture did not decode: %v", parseErr)
			}
			// Act: a valid JSON payload/hash cannot make a forged FSM legitimate.
			mutate(&group)
			identity := childExecutionIdentity(group.ParentID, group.Node, group.Activation, group.Plan.Key, "")
			payload, err := json.Marshal(map[string]ChildGroupRecord{identity: group})
			if err != nil {
				t.Fatal(err)
			}
			_, err = executionChildGroups(ExecutionEnvelope{ExecutionID: "run", Revision: 4, Activation: 1,
				Progress: MigrationState{ExecutionPointer: "node"}, ChildrenPayload: payload})
			// Assert.
			if !errors.Is(err, ErrExecutionCorrupt) {
				t.Fatalf("forged group accepted: %v", err)
			}
		})
	}
}

func TestChildCancellationConfirmationRejectsRevisionExhaustion(t *testing.T) {
	// Arrange: the current wait has no revision space for its resolution.
	group := confirmedChildFixture(t)
	child := &group.Children[0]
	child.State, child.Revision, child.CancelConfirmed, child.CancelConfirmation = ChildWaiting, ^uint64(0), false, nil
	child.WaitID = "external"
	decision := ChildCancelConfirmation{Node: group.Node, Activation: group.Activation, GroupKey: group.Plan.Key,
		ChildID: child.Spec.ID, ExecutionID: child.ExecutionID, ChildRevision: child.Revision,
		RequestID: "stop", DecisionID: "confirmed", Reason: "stopped", Evidence: "host evidence"}
	// Act.
	err := applyChildCancelConfirmation(&group, decision, 4, 2, time.Now().UTC())
	// Assert: no wraparound and no fabricated canceled state.
	if !errors.Is(err, ErrChildRevision) || group.Children[0].Revision != ^uint64(0) ||
		group.Children[0].State != ChildWaiting {
		t.Fatalf("revision overflow: %v child=%+v", err, group.Children[0])
	}
}
