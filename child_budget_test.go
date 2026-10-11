package flowy

import (
	"errors"
	"testing"
)

func TestChildBudgetReturnPlanningIsDetachedAndRequiresSettledEvidence(t *testing.T) {
	// Arrange: host claims consumed units, independent of the child's success/failure.
	group := confirmedChildFixture(t)
	child := group.Children[0]
	child.Spec.Allocation = map[string]int{"units": 9, "requests": 3}
	used := map[string]int{"units": 4, "requests": 1}
	// Act.
	returned, err := ComputeChildBudgetReturn(child, used)
	// Assert: exact subtraction, with no mutation or shared return map.
	if err != nil || returned["units"] != 5 || returned["requests"] != 2 || used["units"] != 4 ||
		child.Spec.Allocation["units"] != 9 {
		t.Fatalf("return calculation: %v returned=%v", err, returned)
	}
	returned["units"] = 100
	if child.Spec.Allocation["units"] != 9 || used["units"] != 4 {
		t.Fatal("return aliases allocation/usage")
	}
}

func TestChildBudgetReturnRejectsUnknownAndInvalidUsage(t *testing.T) {
	for _, scenario := range []string{"unresolved child", "negative", "excess", "foreign", "invalid allocation", "missing consumption"} {
		t.Run(scenario, func(t *testing.T) {
			// Arrange.
			child := confirmedChildFixture(t).Children[0]
			child.Spec.Allocation = map[string]int{"units": 9}
			used := map[string]int{"units": 4}
			want := ErrBudgetExceeded
			switch scenario {
			case "unresolved child":
				child.State, child.CancelConfirmed, child.CancelConfirmation = ChildUnknown, false, nil
				want = ErrChildrenUnresolved
			case "negative":
				used["units"] = -1
			case "excess":
				used["units"] = 10
			case "foreign":
				used["money"] = 1
			case "invalid allocation":
				child.Spec.Allocation["units"] = -1
			case "missing consumption":
				delete(used, "units")
			}
			// Act.
			_, err := ComputeChildBudgetReturn(child, used)
			// Assert: no release is authorized for unresolved work or unbounded claims.
			if !errors.Is(err, want) {
				t.Fatalf("invalid return accepted: %v want=%v", err, want)
			}
		})
	}
}
