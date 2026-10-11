package flowy

import (
	"context"
	"errors"
	"testing"
)

type childTextBackend struct {
	childGroupBackend

	calls int
}

func (p *childTextBackend) joinChildren(ctx context.Context, group ChildGroupRecord, merge ChildMerge) ([]byte, error) {
	p.calls++
	return merge(ctx, group.Children)
}

func (p *childTextBackend) cancelChildren(ctx context.Context, group ChildGroupRecord,
	_ ChildCancelRequest, _ ChildCancelNotifier,
) (ChildGroupRecord, error) {
	p.calls++
	return group, ctx.Err()
}

func (p *childTextBackend) returnChildBudget(ctx context.Context, group ChildGroupRecord,
	_ ChildBudgetReturn,
) (ChildGroupRecord, error) {
	p.calls++
	return group, ctx.Err()
}

func TestChildAssertionsInvalidTextRejectBeforeBackend(t *testing.T) {
	t.Parallel()
	for name, mutate := range map[string]func(*ChildGroupRecord){
		"parent": func(g *ChildGroupRecord) { g.ParentID = string([]byte{0xff}) },
		"node":   func(g *ChildGroupRecord) { g.Node = ExecutionPointer(string([]byte{0xff})) },
		"key":    func(g *ChildGroupRecord) { g.Plan.Key = string([]byte{0xff}) },
		"label":  func(g *ChildGroupRecord) { g.Plan.MergeLabel = string([]byte{0xff}) },
		"child":  func(g *ChildGroupRecord) { g.Children[0].ExecutionID = string([]byte{0xff}) },
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			// Arrange: a forged assertion must not alias valid stored JSON metadata.
			group, err := PlanChildGroup("parent", "node", 1, childPlanForTest(), map[string]int{"work": 3})
			if err != nil {
				t.Fatal(err)
			}
			mutate(&group)
			probe := &childTextBackend{childGroupBackend: nil, calls: 0}
			ctx := context.WithValue(context.Background(), childGroupContextKey{}, probe)
			// Act.
			_, joinErr := JoinChildren(
				ctx,
				group,
				func(context.Context, []ChildRecord) ([]byte, error) { return nil, nil },
			)
			_, cancelErr := CancelChildren(ctx, group, ChildCancelRequest{ID: "cancel", Reason: "stop"},
				func(context.Context, ChildCancelNotice) error { return nil })
			_, budgetErr := ReturnChildBudget(ctx, group, ChildBudgetReturn{ChildID: "a", ChildRevision: 3,
				DecisionID: "return", Reason: "settled", Evidence: "receipt"})
			// Assert.
			if !errors.Is(joinErr, ErrChildInvalid) || !errors.Is(cancelErr, ErrChildInvalid) ||
				!errors.Is(budgetErr, ErrChildInvalid) || probe.calls != 0 {
				t.Fatalf(
					"invalid assertion reached backend: join=%v cancel=%v budget=%v calls=%d",
					joinErr,
					cancelErr,
					budgetErr,
					probe.calls,
				)
			}
		})
	}
}

func TestChildDecisionReasonAdmissionBeforeBackend(t *testing.T) {
	t.Parallel()
	for _, reason := range []string{"bad\xff", "bad\xe2\x82", "Корректный Unicode", "real \ufffd"} {
		t.Run(reason, func(t *testing.T) {
			t.Parallel()
			// Arrange.
			group, planErr := PlanChildGroup("parent", "node", 1, childPlanForTest(), map[string]int{"work": 3})
			if planErr != nil {
				t.Fatal(planErr)
			}
			probe := &childTextBackend{childGroupBackend: nil, calls: 0}
			ctx := context.WithValue(context.Background(), childGroupContextKey{}, probe)
			// Act.
			_, cancelErr := CancelChildren(
				ctx,
				group,
				ChildCancelRequest{ID: "cancel", Reason: reason},
				func(context.Context, ChildCancelNotice) error { return nil },
			)
			_, budgetErr := ReturnChildBudget(
				ctx,
				group,
				ChildBudgetReturn{
					ChildID:       "a",
					ChildRevision: 3,
					DecisionID:    "return",
					Reason:        reason,
					Evidence:      "receipt",
				},
			)
			// Assert.
			if !validRuntimeText(reason) {
				if !errors.Is(cancelErr, ErrChildInvalid) || !errors.Is(budgetErr, ErrChildInvalid) ||
					probe.calls != 0 {
					t.Fatalf("cancel=%v budget=%v calls=%d", cancelErr, budgetErr, probe.calls)
				}
			} else if cancelErr != nil || budgetErr != nil || probe.calls != 2 {
				t.Fatalf("valid reason rejected: cancel=%v budget=%v calls=%d", cancelErr, budgetErr, probe.calls)
			}
		})
	}
}
