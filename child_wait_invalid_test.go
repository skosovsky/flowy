package flowy_test

import (
	"context"
	"errors"
	"testing"

	"github.com/skosovsky/flowy"
	"github.com/skosovsky/flowy/testutil"
)

func TestChildWaitInvalidAddressAndCommitFaultPreserveHistory(t *testing.T) {
	for _, scenario := range []string{"node", "activation", "group", "child", "execution", "revision", "wait", "decision", "result", "commit"} {
		t.Run(scenario, func(t *testing.T) {
			// Arrange: the raw waiting child is the only allowed mutation target.
			ctx := context.Background()
			base := testutil.NewMemoryExecutionStore(nil)
			store := &faultExecutionStore{ExecutionStore: base}
			if scenario == "commit" {
				store.failAt = 6
			}
			runner := childLaunchRunner(
				t,
				store,
				persistedChildPlan(),
				func(context.Context, flowy.ChildInvocation) (flowy.ChildResult, error) {
					return flowy.ChildResult{State: flowy.ChildWaiting, WaitID: "external"}, nil
				},
			)
			first, err := runner.Start(ctx, "invalid", durableTestState{})
			if first == nil || !errors.Is(err, flowy.ErrChildrenUnresolved) {
				t.Fatalf("wait not committed: %v", err)
			}
			source, err := base.LoadExecution(ctx, "invalid")
			if err != nil {
				t.Fatal(err)
			}
			decision := childWaitDecision(storedChildGroup(t, base, "invalid"), 0)
			mutateChildWaitDecision(&decision, scenario)
			// Act.
			_, resolveErr := runner.ResolveChildWait(ctx, first.ResumeToken, decision)
			latest, loadErr := base.LoadExecution(ctx, "invalid")
			// Assert: no replacement history, fake acknowledgement or partial result.
			if resolveErr == nil || loadErr != nil || latest.Revision != source.Revision ||
				latest.Digest != source.Digest ||
				string(latest.ChildrenPayload) != string(source.ChildrenPayload) {
				t.Fatalf("invalid resolution changed history: resolve=%v load=%v", resolveErr, loadErr)
			}
		})
	}
}

func mutateChildWaitDecision(decision *flowy.ChildWaitResolution, scenario string) {
	switch scenario {
	case "node":
		decision.Node = "missing"
	case "activation":
		decision.Activation++
	case "group":
		decision.GroupKey = "missing"
	case "child":
		decision.ChildID = "missing"
	case "execution":
		decision.ExecutionID = "foreign"
	case "revision":
		decision.ChildRevision++
	case "wait":
		decision.WaitID = "foreign"
	case "decision":
		decision.DecisionID = ""
	case "result":
		decision.Result.State = flowy.ChildRunning
	}
}
