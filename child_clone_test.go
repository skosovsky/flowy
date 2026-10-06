package flowy

import (
	"reflect"
	"testing"
	"time"
)

func childCloneOwnershipFixture() ChildGroupRecord {
	return ChildGroupRecord{
		Plan: ChildGroupPlan{
			Label:    string([]byte{0xff}),
			Children: []ChildSpec{{ID: "plan", Input: []byte("plan"), Allocation: map[string]int{"units": 1}}},
		},
		Children: []ChildRecord{
			{Spec: ChildSpec{ID: "child", Input: []byte("input"), Allocation: map[string]int{"units": 2}},
				Result: []byte("result"), WaitResolution: &ChildWaitResolutionRecord{DecisionID: "wait"},
				OutcomeResolution:  &ChildOutcomeResolutionRecord{DecisionID: "outcome"},
				CancelConfirmation: &ChildCancelConfirmationRecord{DecisionID: "confirmed"}},
		},
		MergedIDs:    []string{"child"},
		MergedResult: []byte("merged"),
		Capacity:     map[string]int{"units": 3},
		CancelRequest: &ChildCancelRequestRecord{
			ID: "cancel",
			At: time.Date(10000, time.January, 1, 0, 0, 0, 0, time.UTC),
		},
		BudgetReturns: map[string]ChildBudgetReturnRecord{
			"child": {Used: map[string]int{"units": 1}, Returned: map[string]int{"units": 1}},
		},
	}
}

func TestChildGroupCopyOwnsEveryReferenceWithoutWireAdmission(t *testing.T) {
	// Arrange: invalid wire text/time must survive a pure memory copy unchanged.
	original := childCloneOwnershipFixture()
	// Act.
	copied := detachedChildGroup(original)
	if !reflect.DeepEqual(original, copied) {
		t.Fatalf("copy changed value: %+v", copied)
	}
	copied.Plan.Children[0].Input[0] = 'X'
	copied.Plan.Children[0].Allocation["units"] = 99
	copied.Children[0].Spec.Input[0] = 'X'
	copied.Children[0].Spec.Allocation["units"] = 99
	copied.Children[0].Result[0] = 'X'
	copied.Children[0].WaitResolution.DecisionID = "changed"
	copied.Children[0].OutcomeResolution.DecisionID = "changed"
	copied.Children[0].CancelConfirmation.DecisionID = "changed"
	copied.MergedIDs[0] = "changed"
	copied.MergedResult[0] = 'X'
	copied.Capacity["units"] = 99
	copied.CancelRequest.ID = "changed"
	record := copied.BudgetReturns["child"]
	record.Used["units"], record.Returned["units"] = 99, 99
	copied.BudgetReturns["added"] = ChildBudgetReturnRecord{}
	// Assert: source has all original values, without JSON normalization or lost fields.
	if !reflect.DeepEqual(original, childCloneOwnershipFixture()) {
		t.Fatalf("source mutated: %+v", original)
	}
}

func TestChildGroupCopyPreservesNilAndEmptyCollections(t *testing.T) {
	for _, original := range []ChildGroupRecord{{}, {Plan: ChildGroupPlan{Children: []ChildSpec{}}, Children: []ChildRecord{},
		MergedIDs: []string{}, MergedResult: []byte{}, Capacity: map[string]int{}, BudgetReturns: map[string]ChildBudgetReturnRecord{}},
	} {
		// Arrange/Act: both nil and allocated-empty shapes are legitimate memory values.
		copied := detachedChildGroup(original)
		// Assert.
		if !reflect.DeepEqual(original, copied) {
			t.Fatalf("shape changed: original=%+v copied=%+v", original, copied)
		}
	}
}
