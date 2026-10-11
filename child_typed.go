package flowy

import (
	"bytes"
	"context"
	"errors"
	"maps"
)

var ErrChildCodec = errors.New("flowy: child codec failure")

type ChildProjectionSpec struct {
	ID         string
	Allocation map[string]int
}

// ProjectChildSpecs gives each pure projection a separately decoded parent.
// Host codecs must produce independent objects; no partial plan is returned.
func ProjectChildSpecs[P, I any](
	ctx context.Context,
	parent P,
	specs []ChildProjectionSpec,
	parentCodec StateSerializer[P],
	inputCodec StateSerializer[I],
	project func(context.Context, P, string) (I, error),
) ([]ChildSpec, error) {
	if parentCodec == nil || inputCodec == nil || project == nil || len(specs) == 0 {
		return nil, ErrChildInvalid
	}
	if err := validateChildProjectionSpecs(specs); err != nil {
		return nil, err
	}
	encoded, err := parentCodec.Marshal(parent)
	if err != nil {
		return nil, errors.Join(ErrChildCodec, err)
	}
	encoded = bytes.Clone(encoded)
	result := make([]ChildSpec, 0, len(specs))
	for _, spec := range specs {
		if contextErr := ctx.Err(); contextErr != nil {
			return nil, contextErr
		}
		copyParent, decodeErr := parentCodec.Unmarshal(bytes.Clone(encoded))
		if decodeErr != nil {
			return nil, errors.Join(ErrChildCodec, decodeErr)
		}
		input, projectErr := project(ctx, copyParent, spec.ID)
		if projectErr != nil {
			return nil, projectErr
		}
		payload, encodeErr := inputCodec.Marshal(input)
		if encodeErr != nil {
			return nil, errors.Join(ErrChildCodec, encodeErr)
		}
		result = append(
			result,
			ChildSpec{ID: spec.ID, Input: bytes.Clone(payload), Allocation: maps.Clone(spec.Allocation)},
		)
	}
	if contextErr := ctx.Err(); contextErr != nil {
		return nil, contextErr
	}
	return result, nil
}

func validateChildProjectionSpecs(specs []ChildProjectionSpec) error {
	seen := make(map[string]bool)
	for _, spec := range specs {
		if spec.ID == "" || !validRuntimeText(spec.ID) || !validChildCapacity(spec.Allocation) {
			return ErrChildInvalid
		}
		if seen[spec.ID] {
			return ErrChildDuplicate
		}
		seen[spec.ID] = true
	}
	return nil
}

type TypedChildInvocation[I any] struct {
	Runtime ChildInvocation
	Input   I
}

type TypedChildResult[O any] struct {
	State  ChildState
	Result O
	Error  string
	WaitID string
}

func TypedChildDispatcher[I, O any](inputCodec StateSerializer[I], resultCodec StateSerializer[O],
	worker func(context.Context, TypedChildInvocation[I]) (TypedChildResult[O], error)) (ChildDispatcher, error) {
	if inputCodec == nil || resultCodec == nil || worker == nil {
		return nil, ErrChildInvalid
	}
	return func(ctx context.Context, invocation ChildInvocation) (ChildResult, error) {
		input, err := inputCodec.Unmarshal(bytes.Clone(invocation.Input))
		if err != nil {
			return ChildResult{}, errors.Join(ErrChildCodec, err)
		}
		if contextErr := ctx.Err(); contextErr != nil {
			return ChildResult{}, contextErr
		}
		outcome, err := worker(ctx, TypedChildInvocation[I]{Runtime: invocation, Input: input})
		if err != nil {
			return ChildResult{}, err
		}
		if !validChildResultState(outcome.State) {
			return ChildResult{}, ErrChildInvalid
		}
		payload, err := resultCodec.Marshal(outcome.Result)
		if err != nil {
			return ChildResult{}, errors.Join(ErrChildCodec, err)
		}
		return ChildResult{
			State:   outcome.State,
			Payload: bytes.Clone(payload),
			Error:   outcome.Error,
			WaitID:  outcome.WaitID,
		}, nil
	}, nil
}

type TypedChildOutcome[O any] struct {
	ID              string
	ExecutionID     string
	Revision        uint64
	State           ChildState
	Result          O
	HasResult       bool
	Error           string
	WaitID          string
	CancelRequested bool
	CancelConfirmed bool
}

func DecodeChildOutcomes[O any](group ChildGroupRecord, codec StateSerializer[O]) ([]TypedChildOutcome[O], error) {
	if codec == nil || validateChildGroup(group) != nil {
		return nil, ErrChildInvalid
	}
	return decodeTypedChildRecords(group.Children, codec)
}

func decodeTypedChildRecords[O any](children []ChildRecord, codec StateSerializer[O]) ([]TypedChildOutcome[O], error) {
	outcomes := make([]TypedChildOutcome[O], 0, len(children))
	for _, child := range children {
		var result O
		hasResult := child.State == ChildCompleted || len(child.Result) != 0
		if hasResult {
			decoded, err := codec.Unmarshal(bytes.Clone(child.Result))
			if err != nil {
				return nil, errors.Join(ErrChildCodec, err)
			}
			result = decoded
		}
		outcomes = append(outcomes, TypedChildOutcome[O]{
			ID:              child.Spec.ID,
			ExecutionID:     child.ExecutionID,
			Revision:        child.Revision,
			State:           child.State,
			Result:          result,
			HasResult:       hasResult,
			Error:           child.Error,
			WaitID:          child.WaitID,
			CancelRequested: child.CancelRequested,
			CancelConfirmed: child.CancelConfirmed,
		})
	}
	return outcomes, nil
}

// JoinTypedChildren may return ErrChildCodec after a successful join commit
// when decoding cached merged bytes fails. Inspect/redecode the committed group
// or replay with its current assertion; do not repeat merge/dispatch as recovery.
func JoinTypedChildren[O, R any](ctx context.Context, group ChildGroupRecord, outcomeCodec StateSerializer[O],
	mergedCodec StateSerializer[R], merge func(context.Context, []TypedChildOutcome[O]) (R, error)) (R, error) {
	var zero R
	if outcomeCodec == nil || mergedCodec == nil || merge == nil {
		return zero, ErrChildInvalid
	}
	payload, err := JoinChildren(ctx, group, func(ctx context.Context, children []ChildRecord) ([]byte, error) {
		outcomes, decodeErr := decodeTypedChildRecords(children, outcomeCodec)
		if decodeErr != nil {
			return nil, decodeErr
		}
		result, mergeErr := merge(ctx, outcomes)
		if mergeErr != nil {
			return nil, mergeErr
		}
		encoded, encodeErr := mergedCodec.Marshal(result)
		if encodeErr != nil {
			return nil, errors.Join(ErrChildCodec, encodeErr)
		}
		return bytes.Clone(encoded), nil
	})
	if err != nil {
		return zero, err
	}
	result, err := mergedCodec.Unmarshal(bytes.Clone(payload))
	if err != nil {
		return zero, errors.Join(ErrChildCodec, err)
	}
	return result, nil
}
