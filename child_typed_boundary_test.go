package flowy_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/skosovsky/flowy"
	"github.com/skosovsky/flowy/checkpoint"
	"github.com/skosovsky/flowy/testutil"
)

type brokenTypedOutputCodec struct{}

func (brokenTypedOutputCodec) Marshal(typedChildOutput) ([]byte, error) {
	return nil, errors.New("result encoding unavailable")
}

func (brokenTypedOutputCodec) Unmarshal([]byte) (typedChildOutput, error) {
	return typedChildOutput{}, errors.New("result decoding unavailable")
}

func TestTypedChildCodecFailureDoesNotAuthorizeReplay(t *testing.T) {
	for _, scenario := range []string{"input decode", "outcome encode"} {
		t.Run(scenario, func(t *testing.T) {
			// Arrange: decoding fails before the worker or encoding fails after its effect.
			ctx := context.Background()
			store := testutil.NewMemoryExecutionStore(nil)
			plan := persistedChildPlan()
			var calls atomic.Int32
			var resultCodec flowy.StateSerializer[typedChildOutput] = checkpoint.JSONSerializer[typedChildOutput]{}
			wantCalls := int32(0)
			if scenario == "outcome encode" {
				payload, err := (checkpoint.JSONSerializer[typedChildInput]{}).Marshal(
					typedChildInput{ID: "child", Values: []int{1}},
				)
				if err != nil {
					t.Fatal(err)
				}
				plan.Children[0].Input = payload
				resultCodec, wantCalls = brokenTypedOutputCodec{}, 1
			}
			dispatch, err := flowy.TypedChildDispatcher(
				checkpoint.JSONSerializer[typedChildInput]{},
				resultCodec,
				func(_ context.Context, invocation flowy.TypedChildInvocation[typedChildInput]) (flowy.TypedChildResult[typedChildOutput], error) {
					calls.Add(1)
					return flowy.TypedChildResult[typedChildOutput]{
						State:  flowy.ChildCompleted,
						Result: typedChildOutput{ID: invocation.Input.ID, Value: 1},
					}, nil
				},
			)
			if err != nil {
				t.Fatal(err)
			}
			runner := childLaunchRunner(t, store, plan, dispatch)
			// Act.
			first, err := runner.Start(ctx, "codec", durableTestState{})
			if first == nil || !errors.Is(err, flowy.ErrChildrenUnresolved) {
				t.Fatalf("ambiguous codec failure not retained: %v", err)
			}
			_, resumeErr := runner.Resume(ctx, first.ResumeToken)
			group := storedChildGroup(t, store, "codec")
			// Assert: no blind retry or fabricated successful/definitively failed result.
			if !errors.Is(resumeErr, flowy.ErrChildrenUnresolved) || calls.Load() != wantCalls ||
				group.Children[0].State != flowy.ChildUnknown ||
				len(group.Children[0].Result) != 0 ||
				group.Children[0].Error == "" {
				t.Fatalf("codec replay: %v calls=%d child=%+v", resumeErr, calls.Load(), group.Children[0])
			}
		})
	}
}
