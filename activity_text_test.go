package flowy_test

import (
	"context"
	"encoding/json"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/skosovsky/flowy"
	"github.com/skosovsky/flowy/checkpoint"
	"github.com/skosovsky/flowy/testutil"
)

func TestActivityInvalidUTF8RejectsBeforeIntent(t *testing.T) {
	t.Parallel()
	// Arrange: distinct invalid byte keys previously normalized to one JSON key.
	ctx := context.Background()
	store := testutil.NewMemoryExecutionStore(nil)
	var dispatches atomic.Int32
	identities := make(map[string]bool)
	builder := flowy.NewGraph[durableTestState, flowy.NoEffect](
		func(_, update durableTestState) durableTestState { return update })
	builder.AddNode("node", func(ctx context.Context, state durableTestState) (durableTestState, flowy.Directive, error) {
		for _, key := range []string{string([]byte{0xff}), string([]byte{0xfe}), "\uFFFD", "合法"} {
			request := flowy.ActivityRequest{
				Key: key, Implementation: "stable", Input: []byte{0xff, 0xfe},
				Dispatch: func(_ context.Context, invocation flowy.ActivityInvocation) ([]byte, error) {
					dispatches.Add(1)
					identities[invocation.Identity] = true
					return []byte("outcome"), nil
				},
			}
			outcome, err := flowy.CallActivity(ctx, request)
			if key == "\uFFFD" || key == "合法" {
				if err != nil || outcome.Origin != flowy.ActivityLive {
					return state, flowy.Fail("valid text"), errors.New("valid Unicode did not dispatch independently")
				}
			} else if !errors.Is(err, flowy.ErrExecutionCapability) {
				return state, flowy.Fail("invalid text"), errors.New("invalid key was accepted")
			}
		}
		return state, flowy.End(), nil
	}).
		AllowNoOutgoingRoute("node").
		SetEntryPoint("node")
	graph, err := builder.Compile()
	if err != nil {
		t.Fatal(err)
	}
	runner, err := flowy.NewDurableRunner(graph, store, durableDescriptor("current"),
		checkpoint.JSONSerializer[durableTestState]{}, checkpoint.JSONSerializer[[]flowy.NoEffect]{},
		flowy.DurableOptions{Owner: "worker", LeaseTTL: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	// Act.
	_, runErr := runner.Start(ctx, "run", durableTestState{})
	// Assert: invalid keys never made intents; opaque input bytes are still allowed.
	envelope, loadErr := store.LoadExecution(ctx, "run")
	var journal map[string]flowy.ActivityRecord
	journalErr := json.Unmarshal(envelope.JournalPayload, &journal)
	if runErr != nil || loadErr != nil || journalErr != nil || len(journal) != 2 ||
		dispatches.Load() != 2 || len(identities) != 2 {
		t.Fatalf("identity normalization: run=%v load=%v journal=%v entries=%d dispatches=%d identities=%d",
			runErr, loadErr, journalErr, len(journal), dispatches.Load(), len(identities))
	}
}
