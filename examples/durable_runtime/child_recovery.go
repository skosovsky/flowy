package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/skosovsky/flowy"
	"github.com/skosovsky/flowy/testutil"
)

// Host lookup supplies opaque evidence/result; lease expiry alone proves nothing
// about remote work. Memory storage demonstrates this API, not crash durability.
func childRecoveryDemo(ctx context.Context) error {
	store := testutil.NewMemoryExecutionStore(nil)
	dispatches := 0
	plan := flowy.ChildGroupPlan{
		Key:            "remote",
		Label:          "host-bytes-v1",
		MergeLabel:     "receipt-v1",
		BudgetLabel:    "fixed",
		CancelLabel:    "host-confirmed",
		MaxConcurrency: 1,
		FailurePolicy:  flowy.ChildCollectErrors,
		Children:       []flowy.ChildSpec{{ID: "remote-write", Input: []byte("request")}},
	}
	runner, err := bind(store, "child-recovery", func(ctx context.Context, s state) (state, flowy.Directive, error) {
		group, runErr := flowy.RunChildren(
			ctx,
			plan,
			nil,
			func(context.Context, flowy.ChildInvocation) (flowy.ChildResult, error) {
				dispatches++
				return flowy.ChildResult{State: flowy.ChildUnknown}, nil
			},
		)
		if runErr != nil {
			return s, flowy.End(), runErr
		}
		receipt, joinErr := flowy.JoinChildren(
			ctx,
			group,
			func(_ context.Context, outcomes []flowy.ChildRecord) ([]byte, error) { return outcomes[0].Result, nil },
		)
		s.Value = len(receipt)
		return s, flowy.End(), joinErr
	}, options())
	if err != nil {
		return err
	}
	pending, err := runner.Start(ctx, "child-recovery-demo", state{})
	if pending == nil || !errors.Is(err, flowy.ErrChildrenUnresolved) {
		return fmt.Errorf("expected unknown child: %w", err)
	}
	source, err := store.LoadExecution(ctx, "child-recovery-demo")
	if err != nil {
		return err
	}
	var groups map[string]flowy.ChildGroupRecord
	if err = json.Unmarshal(source.ChildrenPayload, &groups); err != nil {
		return err
	}
	var decision flowy.ChildOutcomeResolution
	for _, group := range groups {
		child := group.Children[0]
		decision = flowy.ChildOutcomeResolution{
			Node:          group.Node,
			Activation:    group.Activation,
			GroupKey:      group.Plan.Key,
			GroupLabel:    group.Plan.Label,
			ChildID:       child.Spec.ID,
			ExecutionID:   child.ExecutionID,
			ChildRevision: child.Revision,
			DecisionID:    "host-found-receipt",
			Reason:        "remote lookup completed",
			Evidence:      "host-receipt-reference",
			Result:        flowy.ChildResult{State: flowy.ChildCompleted, Payload: []byte("receipt")},
		}
	}
	token, err := runner.ResolveChildOutcome(ctx, pending.ResumeToken, decision)
	if err != nil {
		return err
	}
	replay, err := runner.ResolveChildOutcome(ctx, token, decision)
	if err != nil || replay != token {
		return fmt.Errorf("decision replay: %w", err)
	}
	result, err := runner.Resume(ctx, token)
	if err != nil || result == nil || result.State.Value != len("receipt") || dispatches != 1 {
		return fmt.Errorf("child recovery: dispatches=%d result=%+v err=%w", dispatches, result, err)
	}
	return nil
}
