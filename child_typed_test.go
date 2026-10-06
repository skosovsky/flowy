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

type typedChildInput struct {
	ID     string
	Values []int
}

type typedChildOutput struct {
	ID    string
	Value int
}

func TestTypedChildProjectionIsolatesParentAndRejectsDuplicatesBeforeCalls(t *testing.T) {
	// Arrange: both projections mutate their own parent copy.
	ctx := context.Background()
	parent := map[string]int{"value": 1}
	var projections atomic.Int32
	project := func(_ context.Context, copyParent map[string]int, id string) (typedChildInput, error) {
		projections.Add(1)
		value := copyParent["value"]
		copyParent["value"] = 99
		return typedChildInput{ID: id, Values: []int{value}}, nil
	}
	specs := []flowy.ChildProjectionSpec{{ID: "b"}, {ID: "a"}}
	// Act.
	inputs, err := flowy.ProjectChildSpecs(
		ctx,
		parent,
		specs,
		checkpoint.JSONSerializer[map[string]int]{},
		checkpoint.JSONSerializer[typedChildInput]{},
		project,
	)
	_, duplicateErr := flowy.ProjectChildSpecs(ctx, parent, []flowy.ChildProjectionSpec{{ID: "a"}, {ID: "a"}},
		checkpoint.JSONSerializer[map[string]int]{}, checkpoint.JSONSerializer[typedChildInput]{}, project)
	// Assert: no partial duplicate projection and no parent/sibling aliasing.
	if err != nil || !errors.Is(duplicateErr, flowy.ErrChildDuplicate) || projections.Load() != 2 ||
		parent["value"] != 1 {
		t.Fatalf(
			"projection isolation: %v duplicate=%v calls=%d parent=%v",
			err,
			duplicateErr,
			projections.Load(),
			parent,
		)
	}
	for _, spec := range inputs {
		decoded, decodeErr := (checkpoint.JSONSerializer[typedChildInput]{}).Unmarshal(spec.Input)
		if decodeErr != nil || decoded.Values[0] != 1 || decoded.ID != spec.ID {
			t.Fatalf("sibling input changed: %v input=%+v", decodeErr, decoded)
		}
	}
}

func TestTypedChildrenDurableOrderedJoinAndReplay(t *testing.T) {
	// Arrange: host-owned input/output types use existing codecs and runtime boundaries.
	ctx := context.Background()
	store := testutil.NewMemoryExecutionStore(nil)
	plan := persistedChildPlan()
	inputs, err := flowy.ProjectChildSpecs(
		ctx,
		map[string]int{"value": 1},
		[]flowy.ChildProjectionSpec{{ID: "b"}, {ID: "a"}},
		checkpoint.JSONSerializer[map[string]int]{},
		checkpoint.JSONSerializer[typedChildInput]{},
		func(_ context.Context, parent map[string]int, id string) (typedChildInput, error) {
			return typedChildInput{ID: id, Values: []int{parent["value"]}}, nil
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	plan.Children = inputs
	var dispatches, merges atomic.Int32
	dispatch, err := flowy.TypedChildDispatcher(
		checkpoint.JSONSerializer[typedChildInput]{},
		checkpoint.JSONSerializer[typedChildOutput]{},
		func(_ context.Context, invocation flowy.TypedChildInvocation[typedChildInput]) (flowy.TypedChildResult[typedChildOutput], error) {
			dispatches.Add(1)
			value := invocation.Input.Values[0]
			invocation.Input.Values[0] = 99
			return flowy.TypedChildResult[typedChildOutput]{
				State:  flowy.ChildCompleted,
				Result: typedChildOutput{ID: invocation.Input.ID, Value: value},
			}, nil
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	runner := childJoinRunner(
		t,
		store,
		func(ctx context.Context, state durableTestState) (durableTestState, flowy.Directive, error) {
			group, runErr := flowy.RunChildren(ctx, plan, nil, dispatch)
			if runErr != nil {
				return state, flowy.End(), runErr
			}
			result, joinErr := flowy.JoinTypedChildren(
				ctx,
				group,
				checkpoint.JSONSerializer[typedChildOutput]{},
				checkpoint.JSONSerializer[string]{},
				func(_ context.Context, outcomes []flowy.TypedChildOutcome[typedChildOutput]) (string, error) {
					merges.Add(1)
					if !outcomes[0].HasResult || !outcomes[1].HasResult || outcomes[0].Result.Value != 1 ||
						outcomes[1].Result.Value != 1 {
						return "", errors.New("typed result lost or aliased")
					}
					return outcomes[0].Result.ID + outcomes[1].Result.ID, nil
				},
			)
			if joinErr != nil {
				return state, flowy.End(), joinErr
			}
			group, runErr = flowy.PrepareChildren(ctx, plan, nil)
			if runErr == nil {
				result, runErr = flowy.JoinTypedChildren(
					ctx,
					group,
					checkpoint.JSONSerializer[typedChildOutput]{},
					checkpoint.JSONSerializer[string]{},
					func(context.Context, []flowy.TypedChildOutcome[typedChildOutput]) (string, error) {
						merges.Add(100)
						return "", errors.New("committed typed merge repeated")
					},
				)
			}
			if result != "ab" {
				return state, flowy.End(), errors.New("merge order changed")
			}
			return state, flowy.End(), runErr
		},
	)
	// Act.
	_, err = runner.Start(ctx, "typed", durableTestState{})
	// Assert.
	if err != nil || dispatches.Load() != 2 || merges.Load() != 1 {
		t.Fatalf("typed join: %v dispatches=%d merges=%d", err, dispatches.Load(), merges.Load())
	}
}
