package flowy_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/skosovsky/flowy"
	"github.com/skosovsky/flowy/checkpoint"
	"github.com/skosovsky/flowy/testutil"
)

type releaseFailureStore struct {
	flowy.ExecutionStore

	failure  error
	contexts []context.Context
}

func (s *releaseFailureStore) ReleaseExecution(ctx context.Context, lease flowy.ExecutionLease) error {
	s.contexts = append(s.contexts, ctx)
	return errors.Join(s.ExecutionStore.ReleaseExecution(ctx, lease), s.failure)
}

//nolint:cyclop,gocognit,gocritic,gocyclo // Entry-point/admission matrix keeps expected causes next to each call.
func TestDurableCleanupPreservesResultsAndAdmissionErrors(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"start", "stream", "resume", "resume-stream", "rejected-start", "rejected-stream", "rejected-resume", "rejected-resume-stream"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			// Arrange: storage releases ownership but reports its cleanup failure.
			cleanupErr := errors.New("release failed")
			store := &releaseFailureStore{
				ExecutionStore: testutil.NewMemoryExecutionStore(nil),
				failure:        cleanupErr,
				contexts:       nil,
			}
			builder := flowy.NewGraph[durableTestState, flowy.NoEffect](
				func(_, update durableTestState) durableTestState { return update },
			)
			builder.AddNode("done", func(_ context.Context, state durableTestState) (durableTestState, flowy.Directive, error) {
				return state, flowy.End(), nil
			}).
				SetEntryPoint("done").
				AllowNoOutgoingRoute("done")
			graph, err := builder.Compile()
			if err != nil {
				t.Fatal(err)
			}
			runner, err := flowy.NewDurableRunner(
				graph,
				store,
				durableDescriptor("current"),
				checkpoint.JSONSerializer[durableTestState]{},
				checkpoint.JSONSerializer[[]flowy.NoEffect]{},
				flowy.DurableOptions{Owner: "worker", LeaseTTL: time.Minute},
			)
			if err != nil {
				t.Fatal(err)
			}
			token := flowy.ResumeToken{ThreadID: "run", SnapshotRevision: 1}
			if mode != "start" && mode != "stream" {
				initial, initialErr := runner.Start(context.Background(), "run", durableTestState{Value: 42})
				if !errors.Is(initialErr, cleanupErr) {
					t.Fatal(initialErr)
				}
				token = initial.ResumeToken
			}
			if mode == "rejected-resume" || mode == "rejected-resume-stream" {
				token.SnapshotRevision = 0
			}
			before := len(store.contexts)
			// Act.
			var result *flowy.RunResult[durableTestState, flowy.NoEffect]
			switch mode {
			case "start", "rejected-start":
				result, err = runner.Start(context.Background(), "run", durableTestState{Value: 42})
			case "resume", "rejected-resume":
				result, err = runner.Resume(context.Background(), token)
			case "stream", "resume-stream", "rejected-stream", "rejected-resume-stream":
				var handle flowy.StreamHandle[durableTestState, flowy.NoEffect]
				if mode == "stream" || mode == "rejected-stream" {
					handle, err = runner.Stream(context.Background(), "run", durableTestState{Value: 42})
				} else {
					handle, err = runner.ResumeStream(context.Background(), token)
				}
				if err == nil {
					result, err = handle.WaitResult()
				}
			}
			// Assert: cleanup never discards the known result nor the admission cause.
			if !errors.Is(err, flowy.ErrRunCleanup) || !errors.Is(err, cleanupErr) {
				t.Fatalf("result=%+v err=%v", result, err)
			}
			if mode == "rejected-start" || mode == "rejected-stream" {
				if result != nil || !errors.Is(err, flowy.ErrConcurrencyConflict) {
					t.Fatalf("result=%+v err=%v", result, err)
				}
			} else if mode == "rejected-resume" || mode == "rejected-resume-stream" {
				if result != nil || !errors.Is(err, flowy.ErrConcurrencyConflict) {
					t.Fatalf("result=%+v err=%v", result, err)
				}
			} else if result == nil || result.Status != flowy.RunStatusCompleted || result.State.Value != 42 {
				t.Fatalf("result=%+v err=%v", result, err)
			}
			if len(store.contexts) != before+1 {
				t.Fatal("cleanup not called exactly once")
			}
			ctx := store.contexts[before]
			deadline, ok := ctx.Deadline()
			if !ok || time.Until(deadline) > 5*time.Second || ctx.Err() != context.Canceled {
				t.Fatalf("cleanup deadline=%v err=%v", deadline, ctx.Err())
			}
		})
	}
}

type waitReleaseFailureStore struct {
	*waitRegistrationStore

	failure error
}

func (s *waitReleaseFailureStore) ReleaseExecution(ctx context.Context, lease flowy.ExecutionLease) error {
	return errors.Join(s.waitRegistrationStore.ReleaseExecution(ctx, lease), s.failure)
}

//nolint:gocognit // Verify both committed wait operations and their release failures.
func TestDurableWaitCleanupRetainsCommittedDecision(t *testing.T) {
	t.Parallel()
	for _, operation := range []string{"deliver", "cancel"} {
		t.Run(operation, func(t *testing.T) {
			t.Parallel()
			// Arrange: arm a wait with a store that will fail its subsequent release.
			backing := newWaitRegistrationStore()
			store := &waitReleaseFailureStore{waitRegistrationStore: backing, failure: nil}
			var calls, matches, applies atomic.Int32
			runner, err := waitRunnerForTest(
				t,
				store,
				&backing.profile,
				&calls,
				checkpoint.JSONSerializer[durableTestState]{},
				waitDeliveryClock{now: waitSpecForTest().Deadline},
			)
			if err != nil {
				t.Fatal(err)
			}
			armed, err := runner.Start(context.Background(), "run", durableTestState{})
			if err != nil {
				t.Fatal(err)
			}
			delivery := deliveryForArmedWait(t, backing)
			store.failure = errors.New("release failed")
			// Act.
			var token flowy.ResumeToken
			if operation == "deliver" {
				decision, deliverErr := runner.DeliverWait(
					context.Background(),
					"run",
					delivery,
					deliveryContract(&matches, &applies),
				)
				token, err = decision.ResumeToken, deliverErr
				if decision.Decision.Status != flowy.WaitAccepted {
					t.Fatalf("decision=%+v err=%v", decision, err)
				}
			} else {
				token, err = runner.CancelWait(
					context.Background(),
					armed.ResumeToken,
					cancellationForDelivery(delivery),
				)
			}
			// Assert: the token remains authoritative despite the cleanup error.
			if !errors.Is(err, flowy.ErrRunCleanup) || !errors.Is(err, store.failure) ||
				token.SnapshotRevision <= armed.ResumeToken.SnapshotRevision {
				t.Fatalf("token=%+v err=%v", token, err)
			}
			persisted, loadErr := backing.LoadExecution(context.Background(), "run")
			if loadErr != nil || persisted.Revision != token.SnapshotRevision {
				t.Fatalf("persisted=%+v err=%v", persisted, loadErr)
			}
		})
	}
}
