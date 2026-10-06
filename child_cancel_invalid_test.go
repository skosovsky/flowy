package flowy_test

import (
	"context"
	"errors"
	"testing"

	"github.com/skosovsky/flowy"
	"github.com/skosovsky/flowy/testutil"
)

func TestChildCancellationInvalidConfirmationAndCommitFaultPreserveHistory(t *testing.T) {
	for _, scenario := range []string{"node", "activation", "group", "child", "execution", "revision", "request", "decision", "reason", "evidence", "commit"} {
		t.Run(scenario, func(t *testing.T) {
			// Arrange: confirmation is only permitted at the exact requested child revision.
			ctx := context.Background()
			base := testutil.NewMemoryExecutionStore(nil)
			store := &faultExecutionStore{ExecutionStore: base}
			if scenario == "commit" {
				store.failAt = 7
			}
			runner := cancelWaitingRunner(t, store)
			first, err := runner.Start(ctx, "invalid", durableTestState{})
			if first == nil || !errors.Is(err, flowy.ErrChildrenUnresolved) {
				t.Fatalf("requested wait missing: %v", err)
			}
			source, err := base.LoadExecution(ctx, "invalid")
			if err != nil {
				t.Fatal(err)
			}
			decision := cancellationDecision(storedChildGroup(t, base, "invalid"))
			mutateChildCancellationDecision(&decision, scenario)
			// Act.
			_, confirmErr := runner.ConfirmChildCancellation(ctx, first.ResumeToken, decision)
			latest, loadErr := base.LoadExecution(ctx, "invalid")
			// Assert: an invalid decision or unavailable commit gives no false acknowledgement.
			if confirmErr == nil || loadErr != nil || latest.Revision != source.Revision ||
				latest.Digest != source.Digest ||
				string(latest.ChildrenPayload) != string(source.ChildrenPayload) {
				t.Fatalf("invalid confirmation changed history: confirm=%v load=%v", confirmErr, loadErr)
			}
			assertChildConfirmationRetry(ctx, t, scenario, runner, first.ResumeToken, decision)
		})
	}
}

func assertChildConfirmationRetry(
	ctx context.Context,
	t *testing.T,
	scenario string,
	runner *flowy.DurableRunner[durableTestState, flowy.NoEffect],
	token flowy.ResumeToken,
	decision flowy.ChildCancelConfirmation,
) {
	t.Helper()
	if scenario != "commit" {
		return
	}
	if _, err := runner.ConfirmChildCancellation(ctx, token, decision); err != nil {
		t.Fatalf("failed decision was falsely consumed: %v", err)
	}
}

func mutateChildCancellationDecision(decision *flowy.ChildCancelConfirmation, scenario string) {
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
	case "request":
		decision.RequestID = "foreign"
	case "decision":
		decision.DecisionID = ""
	case "reason":
		decision.Reason = ""
	case "evidence":
		decision.Evidence = ""
	}
}
