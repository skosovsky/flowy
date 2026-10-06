package flowy

import (
	"errors"
	"testing"
)

func childPlanForTest() ChildGroupPlan {
	return ChildGroupPlan{
		Key:            "group",
		Label:          "isolated",
		MergeLabel:     "ordered",
		BudgetLabel:    "fixed",
		CancelLabel:    "confirmed",
		MaxConcurrency: 2,
		FailurePolicy:  ChildCollectErrors,
		Children: []ChildSpec{
			{ID: "b", Input: []byte("input"), Allocation: map[string]int{"work": 1}},
			{ID: "a", Input: []byte("input"), Allocation: map[string]int{"work": 2}},
		},
	}
}

func TestChildPlanDetachesInputsAndSortsStableIDs(t *testing.T) {
	t.Parallel()
	// Arrange.
	plan := childPlanForTest()
	available := map[string]int{"work": 3}
	// Act.
	group, err := PlanChildGroup("parent", "node", 1, plan, available)
	if err != nil {
		t.Fatal(err)
	}
	group.Children[0].Spec.Input[0] = 'X'
	group.Children[0].Spec.Allocation["work"] = 99
	// Assert: host inputs, group plan and sibling inputs do not alias mutable child state.
	if group.Children[0].Spec.ID != "a" || group.Children[1].Spec.ID != "b" || available["work"] != 3 ||
		string(plan.Children[1].Input) != "input" || string(group.Plan.Children[0].Input) != "input" ||
		group.Plan.Children[0].Allocation["work"] != 2 || string(group.Children[1].Spec.Input) != "input" {
		t.Fatalf("plan lost isolation/order: %+v", group)
	}
	if group.Children[0].ExecutionID == group.Children[1].ExecutionID || group.Children[0].State != ChildPlanned ||
		group.Children[0].Revision != 0 {
		t.Fatal("uncommitted children share identity or imply dispatch")
	}
}

func TestChildPlanRejectsInvalidContractsAndAllocations(t *testing.T) {
	t.Parallel()
	for name, mutate := range map[string]func(*ChildGroupPlan){
		"duplicate":             func(p *ChildGroupPlan) { p.Children[1].ID = p.Children[0].ID },
		"missing id":            func(p *ChildGroupPlan) { p.Children[0].ID = "" },
		"missing merge":         func(p *ChildGroupPlan) { p.MergeLabel = "" },
		"missing budget":        func(p *ChildGroupPlan) { p.BudgetLabel = "" },
		"missing cancellation":  func(p *ChildGroupPlan) { p.CancelLabel = "" },
		"unbounded concurrency": func(p *ChildGroupPlan) { p.MaxConcurrency = 0 },
		"unknown policy":        func(p *ChildGroupPlan) { p.FailurePolicy = "unsupported" },
		"empty group":           func(p *ChildGroupPlan) { p.Children = nil },
		"overspend":             func(p *ChildGroupPlan) { p.Children[1].Allocation["work"] = 3 },
		"negative allocation":   func(p *ChildGroupPlan) { p.Children[0].Allocation["work"] = -1 },
		"unknown counter":       func(p *ChildGroupPlan) { p.Children[0].Allocation["money"] = 0 },
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			// Arrange.
			plan := childPlanForTest()
			mutate(&plan)
			// Act.
			_, err := PlanChildGroup("parent", "node", 1, plan, map[string]int{"work": 3})
			// Assert.
			if err == nil {
				t.Fatal("invalid launch plan accepted")
			}
			if name == "duplicate" && !errors.Is(err, ErrChildDuplicate) {
				t.Fatalf("duplicate error: %v", err)
			}
		})
	}
}

func TestChildIdentityIncludesUnambiguousStructuredOwnership(t *testing.T) {
	t.Parallel()
	// Arrange/Act.
	first := childExecutionIdentity("parent", "node", 1, "a:b", "c")
	second := childExecutionIdentity("parent", "node", 1, "a", "b:c")
	cycle := childExecutionIdentity("parent", "node", 2, "a:b", "c")
	otherParent := childExecutionIdentity("other", "node", 1, "a:b", "c")
	// Assert.
	if first == second || first == cycle || first == otherParent ||
		first != childExecutionIdentity("parent", "node", 1, "a:b", "c") {
		t.Fatal("child address collision or unstable identity")
	}
}
