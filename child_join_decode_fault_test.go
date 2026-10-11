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

var errMergedDecode = errors.New("merged decode unavailable")

type mergedDecodeFaultCodec struct{ fail atomic.Bool }

func (*mergedDecodeFaultCodec) Marshal(value int) ([]byte, error) {
	return checkpoint.JSONSerializer[int]{}.Marshal(value)
}
func (c *mergedDecodeFaultCodec) Unmarshal(payload []byte) (int, error) {
	if c.fail.Load() {
		return 0, errMergedDecode
	}
	return checkpoint.JSONSerializer[int]{}.Unmarshal(payload)
}

func TestTypedJoinDecodeFailureAfterCommitReplaysWithoutMerge(t *testing.T) {
	// Arrange: merge encoding succeeds; only decoding its already committed result fails.
	ctx := context.Background()
	store := testutil.NewMemoryExecutionStore(nil)
	codec := &mergedDecodeFaultCodec{}
	codec.fail.Store(true)
	var dispatches, merges atomic.Int32
	plan := persistedChildPlan()
	runner := childJoinRunner(
		t,
		store,
		func(ctx context.Context, state durableTestState) (durableTestState, flowy.Directive, error) {
			group, err := flowy.RunChildren(
				ctx,
				plan,
				nil,
				func(context.Context, flowy.ChildInvocation) (flowy.ChildResult, error) {
					dispatches.Add(1)
					return flowy.ChildResult{State: flowy.ChildCompleted, Payload: []byte("5")}, nil
				},
			)
			if err != nil {
				return state, flowy.End(), err
			}
			result, err := flowy.JoinTypedChildren(ctx, group, checkpoint.JSONSerializer[int]{}, codec,
				func(_ context.Context, outcomes []flowy.TypedChildOutcome[int]) (int, error) {
					merges.Add(1)
					return outcomes[0].Result, nil
				})
			if err != nil {
				return state, flowy.End(), err
			}
			state.Value = result
			return state, flowy.End(), nil
		},
	)
	// Act: caller sees an error after the persisted merge succeeded.
	first, err := runner.Start(ctx, "decode", durableTestState{})
	if first == nil || !errors.Is(err, flowy.ErrChildCodec) || !errors.Is(err, errMergedDecode) {
		t.Fatalf("first=%+v err=%v", first, err)
	}
	group := storedChildGroup(t, store, "decode")
	if len(group.MergedIDs) != 1 || string(group.MergedResult) != "5" || merges.Load() != 1 {
		t.Fatalf("committed group=%+v merges=%d", group, merges.Load())
	}
	codec.fail.Store(false)
	decoded, decodeErr := codec.Unmarshal(group.MergedResult)
	result, resumeErr := runner.Resume(ctx, first.ResumeToken)
	// Assert: inspect/redecode recovers bytes; terminal failure replays without reentering the node.
	if decodeErr != nil || decoded != 5 || !errors.Is(resumeErr, flowy.ErrExecutionFailed) || result == nil ||
		result.Status != flowy.RunStatusFailed ||
		result.State.Value != 0 ||
		dispatches.Load() != 1 ||
		merges.Load() != 1 {
		t.Fatalf(
			"result=%+v err=%v decoded=%d decode=%v dispatch=%d merge=%d",
			result,
			resumeErr,
			decoded,
			decodeErr,
			dispatches.Load(),
			merges.Load(),
		)
	}
}

func TestTypedJoinDecodeHandledInActivationReadsCachedJoin(t *testing.T) {
	// Arrange: a node handles postcommit decoding failure before selecting a terminal directive.
	ctx := context.Background()
	store := testutil.NewMemoryExecutionStore(nil)
	codec := &mergedDecodeFaultCodec{}
	codec.fail.Store(true)
	var dispatches, merges atomic.Int32
	plan := persistedChildPlan()
	runner := childJoinRunner(
		t,
		store,
		func(ctx context.Context, state durableTestState) (durableTestState, flowy.Directive, error) {
			group, err := flowy.RunChildren(
				ctx,
				plan,
				nil,
				func(context.Context, flowy.ChildInvocation) (flowy.ChildResult, error) {
					dispatches.Add(1)
					return flowy.ChildResult{State: flowy.ChildCompleted, Payload: []byte("5")}, nil
				},
			)
			if err != nil {
				return state, flowy.End(), err
			}
			_, err = flowy.JoinTypedChildren(ctx, group, checkpoint.JSONSerializer[int]{}, codec,
				func(_ context.Context, outcomes []flowy.TypedChildOutcome[int]) (int, error) {
					merges.Add(1)
					return outcomes[0].Result, nil
				})
			if !errors.Is(err, errMergedDecode) {
				return state, flowy.End(), errors.New("expected postcommit decode failure")
			}
			current, err := flowy.PrepareChildren(ctx, plan, nil)
			if err != nil {
				return state, flowy.End(), err
			}
			payload, err := flowy.JoinChildren(
				ctx,
				current,
				func(context.Context, []flowy.ChildRecord) ([]byte, error) {
					merges.Add(100)
					return nil, errors.New("cached join invoked merge")
				},
			)
			if err != nil {
				return state, flowy.End(), err
			}
			codec.fail.Store(false)
			state.Value, err = codec.Unmarshal(payload)
			return state, flowy.End(), err
		},
	)
	// Act.
	result, err := runner.Start(ctx, "handled", durableTestState{})
	// Assert: the committed group is read with a fresh assertion, no repeated work.
	if err != nil || result == nil || result.State.Value != 5 || merges.Load() != 1 || dispatches.Load() != 1 {
		t.Fatalf("result=%+v err=%v merges=%d dispatches=%d", result, err, merges.Load(), dispatches.Load())
	}
}
