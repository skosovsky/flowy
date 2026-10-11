package flowy_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"unicode/utf8"

	"github.com/skosovsky/flowy"
	"github.com/skosovsky/flowy/checkpoint"
	"github.com/skosovsky/flowy/testutil"
)

//nolint:gocognit // Keep invalid/valid admission and reload replay in one independently arranged matrix.
func TestWaitCancellationUnicodeAdmissionAndExactReplay(t *testing.T) {
	t.Parallel()
	for _, reason := range []string{"отмена — 确认", "literal \ufffd", "bad\xff", "bad\xe2\x82"} {
		t.Run(reason, func(t *testing.T) {
			t.Parallel()
			// Arrange: arm and reload through serialized memory storage.
			store := newWaitRegistrationStore()
			var calls atomic.Int32
			runner := deliveryRunner(t, store, &calls, checkpoint.JSONSerializer[durableTestState]{})
			armed, err := runner.Start(context.Background(), "run", durableTestState{})
			if err != nil {
				t.Fatal(err)
			}
			delivery := deliveryForArmedWait(t, store)
			request := cancellationForDelivery(delivery)
			request.Reason = reason
			before, err := store.LoadExecution(context.Background(), "run")
			if err != nil {
				t.Fatal(err)
			}
			// Act.
			canceled, cancelErr := runner.CancelWait(context.Background(), armed.ResumeToken, request)
			after, loadErr := store.LoadExecution(context.Background(), "run")
			// Assert.
			if !utf8.ValidString(reason) {
				if !errors.Is(cancelErr, flowy.ErrWaitInvalid) || loadErr != nil || after.Digest != before.Digest ||
					after.Revision != before.Revision {
					t.Fatalf(
						"invalid reason mutated: err=%v load=%v before=%d after=%d",
						cancelErr,
						loadErr,
						before.Revision,
						after.Revision,
					)
				}
				return
			}
			if cancelErr != nil || loadErr != nil {
				t.Fatalf("cancel=%v load=%v", cancelErr, loadErr)
			}
			// A separately created runner has no local acknowledgement/cache to rely on.
			reloaded := deliveryRunner(t, store, &calls, checkpoint.JSONSerializer[durableTestState]{})
			replayed, replayErr := reloaded.CancelWait(context.Background(), armed.ResumeToken, request)
			final, finalErr := store.LoadExecution(context.Background(), "run")
			records, recordsErr := flowy.InspectExecutionWaits(final)
			if replayErr != nil || finalErr != nil || recordsErr != nil || replayed != canceled ||
				final.Digest != after.Digest ||
				len(records) != 1 ||
				records[0].Cancellation.Reason != reason {
				t.Fatalf("replay=%+v err=%v final=%v records=%v", replayed, replayErr, finalErr, recordsErr)
			}
		})
	}
}

//nolint:gocognit // Exercise both persisted cancellation and rejection with the same reload scenario.
func TestChildCancelUnicodeReasonReplaysAfterReload(t *testing.T) {
	t.Parallel()
	for _, reason := range []string{"отмена — 确认", "literal \ufffd", "bad\xff", "bad\xe2\x82"} {
		t.Run(reason, func(t *testing.T) {
			t.Parallel()
			// Arrange: one waiting remote child; acknowledgement must not settle it.
			store := testutil.NewMemoryExecutionStore(nil)
			var dispatches, notices atomic.Int32
			node := func(ctx context.Context, state durableTestState) (durableTestState, flowy.Directive, error) {
				group, runErr := flowy.RunChildren(
					ctx,
					persistedChildPlan(),
					nil,
					func(context.Context, flowy.ChildInvocation) (flowy.ChildResult, error) {
						dispatches.Add(1)
						return flowy.ChildResult{State: flowy.ChildWaiting, WaitID: "external"}, nil
					},
				)
				if !errors.Is(runErr, flowy.ErrChildrenUnresolved) {
					return state, flowy.End(), runErr
				}
				before, loadErr := store.LoadExecution(ctx, "cancel-text")
				if loadErr != nil {
					return state, flowy.End(), loadErr
				}
				_, cancelErr := flowy.CancelChildren(
					ctx,
					group,
					flowy.ChildCancelRequest{ID: "cancel", Reason: reason},
					func(context.Context, flowy.ChildCancelNotice) error { notices.Add(1); return nil },
				)
				after, afterErr := store.LoadExecution(ctx, "cancel-text")
				if !utf8.ValidString(reason) {
					if !errors.Is(cancelErr, flowy.ErrChildInvalid) || afterErr != nil ||
						before.Digest != after.Digest ||
						notices.Load() != 0 {
						return state, flowy.End(), errors.New("invalid cancellation mutated/notified")
					}
				} else if cancelErr != nil || afterErr != nil {
					return state, flowy.End(), errors.Join(cancelErr, afterErr)
				}
				return state, flowy.End(), flowy.ErrChildrenUnresolved
			}
			runner := childJoinRunner(t, store, node)
			// Act: reload a committed request under a new runner/session.
			first, err := runner.Start(context.Background(), "cancel-text", durableTestState{})
			if !errors.Is(err, flowy.ErrChildrenUnresolved) {
				t.Fatal(err)
			}
			second := childJoinRunner(t, store, node)
			_, resumeErr := second.Resume(context.Background(), first.ResumeToken)
			group := storedChildGroup(t, store, "cancel-text")
			// Assert.
			if !errors.Is(resumeErr, flowy.ErrChildrenUnresolved) || dispatches.Load() != 1 {
				t.Fatalf("err=%v dispatches=%d", resumeErr, dispatches.Load())
			}
			if utf8.ValidString(reason) {
				if notices.Load() != 2 || group.CancelRequest == nil || group.CancelRequest.Reason != reason {
					t.Fatalf("notices=%d group=%+v", notices.Load(), group)
				}
			} else if notices.Load() != 0 || group.CancelRequest != nil {
				t.Fatalf("invalid reason persisted: %+v notices=%d", group, notices.Load())
			}
		})
	}
}

//nolint:gocognit // Assert codec reload, publication identity and the capacity ledger in one scenario.
func TestChildBudgetReturnUnicodeAdmissionAndExactReplay(t *testing.T) {
	t.Parallel()
	for _, reason := range []string{"возврат — 确认", "literal \ufffd", "bad\xff", "bad\xe2\x82"} {
		t.Run(reason, func(t *testing.T) {
			t.Parallel()
			// Arrange: a completed child owns eight of ten units; the claim returns five.
			store := testutil.NewMemoryExecutionStore(nil)
			var dispatches atomic.Int32
			node := func(ctx context.Context, state durableTestState) (durableTestState, flowy.Directive, error) {
				group, err := flowy.RunChildren(ctx, budgetChildPlan("first", 8), map[string]int{"units": 10},
					func(context.Context, flowy.ChildInvocation) (flowy.ChildResult, error) {
						dispatches.Add(1)
						return flowy.ChildResult{State: flowy.ChildCompleted}, nil
					})
				if err != nil {
					return state, flowy.End(), err
				}
				claim := flowy.ChildBudgetReturn{ChildID: group.Children[0].Spec.ID,
					ChildRevision: group.Children[0].Revision, DecisionID: "usage", Reason: reason,
					Evidence: "meter", Used: map[string]int{"units": 3}}
				before, err := store.LoadExecution(ctx, "budget-text")
				if err != nil {
					return state, flowy.End(), err
				}
				priorReturn := len(group.BudgetReturns) != 0
				// Act: the second execution reaches this call using a deserialized group.
				returned, returnErr := flowy.ReturnChildBudget(ctx, group, claim)
				after, loadErr := store.LoadExecution(ctx, "budget-text")
				// Assert: invalid text and acknowledged replay have no publication.
				if !utf8.ValidString(reason) {
					if !errors.Is(returnErr, flowy.ErrChildInvalid) || loadErr != nil ||
						before.Digest != after.Digest {
						return state, flowy.End(), errors.New("invalid budget claim mutated ledger")
					}
				} else {
					if returnErr != nil || loadErr != nil {
						return state, flowy.End(), errors.Join(returnErr, loadErr)
					}
					record := returned.BudgetReturns[claim.ChildID]
					if record.Reason != reason || record.Returned["units"] != 5 ||
						(priorReturn && before.Digest != after.Digest) {
						return state, flowy.End(), errors.New("budget replay changed identity or ledger")
					}
				}
				return state, flowy.End(), flowy.ErrChildrenUnresolved
			}
			firstRunner := childJoinRunner(t, store, node)
			first, err := firstRunner.Start(context.Background(), "budget-text", durableTestState{})
			if !errors.Is(err, flowy.ErrChildrenUnresolved) {
				t.Fatal(err)
			}
			secondRunner := childJoinRunner(t, store, node)
			_, err = secondRunner.Resume(context.Background(), first.ResumeToken)
			group := storedChildGroup(t, store, "budget-text")
			if !errors.Is(err, flowy.ErrChildrenUnresolved) || dispatches.Load() != 1 {
				t.Fatalf("resume=%v dispatches=%d", err, dispatches.Load())
			}
			if utf8.ValidString(reason) {
				if record := group.BudgetReturns[group.Children[0].Spec.ID]; record.Reason != reason ||
					record.Returned["units"] != 5 {
					t.Fatalf("persisted return=%+v", record)
				}
			} else if len(group.BudgetReturns) != 0 {
				t.Fatalf("invalid claim persisted: %+v", group.BudgetReturns)
			}
		})
	}
}
