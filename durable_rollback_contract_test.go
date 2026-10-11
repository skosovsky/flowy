package flowy_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/skosovsky/flowy"
	"github.com/skosovsky/flowy/checkpoint"
	"github.com/skosovsky/flowy/testutil"
)

type rollbackFailureCodec struct {
	failed               atomic.Bool
	commitErr, decodeErr error
}

func (c *rollbackFailureCodec) Marshal(state int) ([]byte, error) {
	if c.failed.Load() {
		return nil, c.commitErr
	}
	return checkpoint.JSONSerializer[int]{}.Marshal(state)
}
func (c *rollbackFailureCodec) Unmarshal(payload []byte) (int, error) {
	if c.failed.Load() {
		return 0, c.decodeErr
	}
	return checkpoint.JSONSerializer[int]{}.Unmarshal(payload)
}

func TestDurableRollbackDecodeFailureIsDiagnostic(t *testing.T) {
	// Arrange: codecs work through initial admission, then both publication and restore fail.
	codec := &rollbackFailureCodec{
		commitErr: errors.New("cannot encode step"),
		decodeErr: errors.New("cannot decode entry"),
	}
	b := flowy.NewGraph[int, flowy.NoEffect](func(_, update int) int { return update })
	b.AddNode("node", func(context.Context, int) (int, flowy.Directive, error) {
		codec.failed.Store(true)
		return 42, flowy.End(), nil
	}).SetEntryPoint("node").AllowNoOutgoingRoute("node")
	graph, err := b.Compile()
	if err != nil {
		t.Fatal(err)
	}
	store := testutil.NewMemoryExecutionStore(nil)
	runner, err := flowy.NewDurableRunner(graph, store, durableDescriptor("rollback"), codec,
		checkpoint.JSONSerializer[[]flowy.NoEffect]{}, flowy.DurableOptions{Owner: "test", LeaseTTL: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	// Act.
	result, err := runner.Start(context.Background(), "diagnostic", 7)
	// Assert: both causes and explicit diagnostic classification survive; storage remains authoritative.
	if !errors.Is(err, codec.commitErr) || !errors.Is(err, codec.decodeErr) ||
		!errors.Is(err, flowy.ErrDurableStateUnavailable) || result == nil || result.State != 42 ||
		!strings.HasPrefix(result.Reason, "diagnostic local state:") || result.ResumeToken.SnapshotRevision != 1 {
		t.Fatalf("result=%+v error=%v", result, err)
	}
	stored, err := store.LoadExecution(context.Background(), "diagnostic")
	if err != nil || string(stored.Progress.StatePayload) != "7" {
		t.Fatalf("stored=%+v err=%v", stored, err)
	}
}

func TestActivityCallbackReentryAndInvalidClassificationObservation(t *testing.T) {
	// Arrange: a dispatcher attempts a nested boundary; invalid classification is private host text.
	observer := &activityObserver{}
	ctx := flowy.WithLifecycleObserver(context.Background(), observer)
	store := testutil.NewMemoryExecutionStore(nil)
	b := flowy.NewGraph[int, flowy.NoEffect](func(_, update int) int { return update })
	b.AddNode("node", func(ctx context.Context, state int) (int, flowy.Directive, error) {
		_, err := flowy.CallActivity(ctx, flowy.ActivityRequest{Key: "outer", Implementation: "v1",
			Dispatch: func(callbackCtx context.Context, _ flowy.ActivityInvocation) ([]byte, error) {
				_, nestedErr := flowy.CallActivity(
					callbackCtx,
					flowy.ActivityRequest{Key: "nested", Implementation: "v1",
						Dispatch: func(context.Context, flowy.ActivityInvocation) ([]byte, error) {
							t.Error("nested dispatch ran")
							return nil, nil
						},
					},
				)
				if !errors.Is(nestedErr, flowy.ErrExecutionCapability) {
					t.Errorf("nested=%v", nestedErr)
				}
				return nil, errors.New("private downstream error")
			},
			Classify: func(error) flowy.ActivityFailureDecision {
				return flowy.ActivityFailureDecision{Class: "private invalid class"}
			},
		})
		return state, flowy.End(), err
	}).SetEntryPoint("node").AllowNoOutgoingRoute("node")
	graph, err := b.Compile()
	if err != nil {
		t.Fatal(err)
	}
	runner, err := flowy.NewDurableRunner(graph, store, durableDescriptor("callbacks"),
		checkpoint.JSONSerializer[int]{}, checkpoint.JSONSerializer[[]flowy.NoEffect]{},
		flowy.DurableOptions{Owner: "test", LeaseTTL: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	// Act.
	_, err = runner.Start(ctx, "callbacks", 1)
	// Assert: unknown remains unknown; nested activity has no journal entry and diagnostic is bounded.
	if !errors.Is(err, flowy.ErrActivityUnknown) {
		t.Fatalf("err=%v", err)
	}
	head, err := store.LoadExecution(ctx, "callbacks")
	if err != nil {
		t.Fatal(err)
	}
	var journal map[string]flowy.ActivityRecord
	if err = json.Unmarshal(head.JournalPayload, &journal); err != nil || len(journal) != 1 {
		t.Fatalf("journal=%+v err=%v", journal, err)
	}
	diagnostics := 0
	for _, event := range observer.snapshot() {
		if event.Code == "invalid_classification" {
			diagnostics++
		}
		if strings.Contains(event.Code, "private") {
			t.Fatalf("private code=%+v", event)
		}
	}
	if diagnostics != 1 {
		t.Fatalf("diagnostics=%d", diagnostics)
	}
}
