package flowy_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/skosovsky/flowy"
	"github.com/skosovsky/flowy/testutil"
)

type executionTextProbeStore struct {
	flowy.ExecutionStore

	loads atomic.Int32
}

func (s *executionTextProbeStore) LoadExecution(ctx context.Context, id string) (flowy.ExecutionEnvelope, error) {
	s.loads.Add(1)
	return s.ExecutionStore.LoadExecution(ctx, id)
}

func TestInvalidRuntimeTextRejectsBeforeStore(t *testing.T) {
	t.Parallel()
	// Arrange: no execution exists, so any store call exposes a missing preflight guard.
	ctx := context.Background()
	store := &executionTextProbeStore{ExecutionStore: testutil.NewMemoryExecutionStore(nil), loads: atomic.Int32{}}
	var nodes atomic.Int32
	runner := importRunner(t, store, &nodes)
	invalid := string([]byte{0xff})
	token := flowy.ResumeToken{ThreadID: "run", SnapshotRevision: 1}
	// Act: operation IDs must reject before reads, leases, codecs or host callbacks.
	_, deliveryErr := runner.DeliverWait(ctx, "run", flowy.WaitDelivery{Generation: "generation", ID: invalid},
		flowy.WaitDeliveryContract[durableTestState]{})
	_, cancellationErr := runner.CancelWait(ctx, token, flowy.WaitCancellation{
		Generation: "generation", ID: invalid, Reason: "stop", Evidence: "receipt"})
	_, activityErr := runner.ResolveActivity(ctx, token, flowy.ActivityResolution{
		Identity: "activity", InputDigest: "digest", Implementation: "host", DecisionID: invalid,
		Reason: "confirm", Evidence: "receipt"})
	_, childErr := runner.ResolveChildWait(ctx, token, flowy.ChildWaitResolution{
		Node: "node", Activation: 1, GroupKey: "group", ChildID: "child", ExecutionID: "child-execution",
		ChildRevision: 3, WaitID: "wait", DecisionID: invalid, Result: flowy.ChildResult{State: flowy.ChildCompleted}})
	_, confirmationErr := runner.ConfirmChildCancellation(ctx, token, flowy.ChildCancelConfirmation{
		Node: "node", Activation: 1, GroupKey: "group", ChildID: "child", ExecutionID: "child-execution",
		ChildRevision: 3, RequestID: "request", DecisionID: invalid, Reason: "stop", Evidence: "receipt"})
	// Assert.
	if !errors.Is(deliveryErr, flowy.ErrWaitInvalid) || !errors.Is(cancellationErr, flowy.ErrWaitInvalid) ||
		!errors.Is(activityErr, flowy.ErrActivityConflict) || !errors.Is(childErr, flowy.ErrChildJoinInvalid) ||
		!errors.Is(confirmationErr, flowy.ErrChildJoinInvalid) || store.loads.Load() != 0 || nodes.Load() != 0 {
		t.Fatalf(
			"invalid identity reached runtime: delivery=%v cancel=%v activity=%v child=%v confirmation=%v loads=%d nodes=%d",
			deliveryErr,
			cancellationErr,
			activityErr,
			childErr,
			confirmationErr,
			store.loads.Load(),
			nodes.Load(),
		)
	}
}

func TestImportInvalidTextRejectsBeforeTransform(t *testing.T) {
	t.Parallel()
	for _, field := range []string{"source-id", "format", "importer-id"} {
		t.Run(field, func(t *testing.T) {
			t.Parallel()
			// Arrange.
			ctx := context.Background()
			store := &executionTextProbeStore{
				ExecutionStore: testutil.NewMemoryExecutionStore(nil),
				loads:          atomic.Int32{},
			}
			var nodes, transforms atomic.Int32
			runner := importRunner(t, store, &nodes)
			source, importer := legacySource(), legacyImporter()
			switch field {
			case "source-id":
				source.ID = string([]byte{0xff})
			case "format":
				source.Format = string([]byte{0xff})
				importer.Format = source.Format
			case "importer-id":
				importer.ID = string([]byte{0xff})
			}
			transform := importer.Transform
			importer.Transform = func(payload []byte) (flowy.ImportedExecutionState, error) {
				transforms.Add(1)
				return transform(payload)
			}
			// Act.
			token, err := runner.Import(ctx, "target", source, importer)
			// Assert.
			if !errors.Is(err, flowy.ErrExecutionImportInvalid) || token.SnapshotRevision != 0 ||
				store.loads.Load() != 0 || transforms.Load() != 0 || nodes.Load() != 0 {
				t.Fatalf("invalid label reached importer: token=%+v err=%v loads=%d transforms=%d nodes=%d",
					token, err, store.loads.Load(), transforms.Load(), nodes.Load())
			}
		})
	}
}

func TestInvalidDecisionReasonRejectsBeforeStore(t *testing.T) {
	t.Parallel()
	for _, reason := range []string{"bad\xff", "bad\xe2\x82"} {
		t.Run(reason, func(t *testing.T) {
			t.Parallel()
			// Arrange: no execution exists; admission must precede even a read.
			store := &executionTextProbeStore{
				ExecutionStore: testutil.NewMemoryExecutionStore(nil),
				loads:          atomic.Int32{},
			}
			var nodes atomic.Int32
			runner := importRunner(t, store, &nodes)
			token := flowy.ResumeToken{ThreadID: "run", SnapshotRevision: 1}
			// Act.
			_, cancelErr := runner.CancelWait(
				context.Background(),
				token,
				flowy.WaitCancellation{Generation: "generation", ID: "cancel", Reason: reason, Evidence: "receipt"},
			)
			_, activityErr := runner.ResolveActivity(
				context.Background(),
				token,
				flowy.ActivityResolution{
					Identity:       "activity",
					InputDigest:    "digest",
					Implementation: "host",
					DecisionID:     "decision",
					Reason:         reason,
					Evidence:       "receipt",
				},
			)
			_, confirmErr := runner.ConfirmChildCancellation(
				context.Background(),
				token,
				flowy.ChildCancelConfirmation{
					Node:          "node",
					Activation:    1,
					GroupKey:      "group",
					ChildID:       "child",
					ExecutionID:   "child-execution",
					ChildRevision: 3,
					RequestID:     "request",
					DecisionID:    "confirm",
					Reason:        reason,
					Evidence:      "receipt",
				},
			)
			// Assert.
			if !errors.Is(cancelErr, flowy.ErrWaitInvalid) || !errors.Is(activityErr, flowy.ErrActivityConflict) ||
				!errors.Is(confirmErr, flowy.ErrChildJoinInvalid) ||
				store.loads.Load() != 0 ||
				nodes.Load() != 0 {
				t.Fatalf(
					"cancel=%v activity=%v confirm=%v loads=%d nodes=%d",
					cancelErr,
					activityErr,
					confirmErr,
					store.loads.Load(),
					nodes.Load(),
				)
			}
		})
	}
}
