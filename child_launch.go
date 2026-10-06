package flowy

import (
	"bytes"
	"context"
	"errors"
	"fmt"
)

var ErrChildBusy = errors.New("flowy: child already running")

// ChildInvocation supplies stable ownership and the committed child revision.
// Host interprets isolated BYOT bytes and runs its worker under this identity.
type ChildInvocation struct {
	ParentID    string
	GroupKey    string
	ChildID     string
	ExecutionID string
	Revision    uint64
	Input       []byte
	Allocation  map[string]int
}

type ChildResult struct {
	State   ChildState
	Payload []byte
	Error   string
	WaitID  string
}

type ChildDispatcher func(context.Context, ChildInvocation) (ChildResult, error)

// RunChildren commits intent and each launch/outcome, with bounded concurrency.
// Returned resolved outcomes still require an explicit persisted parent join.
func RunChildren(
	ctx context.Context,
	plan ChildGroupPlan,
	available map[string]int,
	dispatch ChildDispatcher,
) (ChildGroupRecord, error) {
	backend, ok := ctx.Value(childGroupContextKey{}).(childGroupBackend)
	if !ok || dispatch == nil {
		return ChildGroupRecord{}, ErrExecutionCapability
	}
	return backend.runChildren(ctx, plan, available, dispatch)
}

type childDispatchCompletion struct{ err error }

func (c *executionCheckpointer[T, E]) runChildren(ctx context.Context, plan ChildGroupPlan, available map[string]int,
	dispatch ChildDispatcher) (ChildGroupRecord, error) {
	if c.forkMode == ForkFake {
		if c.forkPolicy == nil || c.forkPolicy.FakeChild == nil {
			return ChildGroupRecord{}, ErrForkPolicy
		}
		dispatch = c.forkPolicy.FakeChild
	}
	if c.forkMode == ForkLive {
		liveDispatch := dispatch
		dispatch = func(ctx context.Context, invocation ChildInvocation) (ChildResult, error) {
			if gateErr := c.authorizeForkDispatch(ctx); gateErr != nil {
				return ChildResult{}, gateErr
			}
			return liveDispatch(ctx, invocation)
		}
	}
	group, err := c.prepareChildren(ctx, plan, available)
	if err != nil {
		return ChildGroupRecord{}, err
	}
	identity := childExecutionIdentity(group.ParentID, group.Node, group.Activation, group.Plan.Key, "")
	group, err = c.recoverChildLaunches(ctx, identity)
	if err != nil {
		return group, err
	}
	return c.launchChildGroup(ctx, identity, group, dispatch)
}

func launchableChildIDs(group ChildGroupRecord) []string {
	if group.CancelRequested {
		return nil
	}
	ids := make([]string, 0, len(group.Children))
	for _, child := range group.Children {
		if child.State == ChildPlanned || child.State == ChildQueued {
			ids = append(ids, child.Spec.ID)
		}
	}
	return ids
}

func (c *executionCheckpointer[T, E]) launchChildGroup(ctx context.Context, identity string, group ChildGroupRecord,
	dispatch ChildDispatcher) (ChildGroupRecord, error) {
	workCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	childCtx := context.WithValue(workCtx, activityContextKey{}, struct{}{})
	childCtx = context.WithValue(childCtx, childGroupContextKey{}, struct{}{})
	childCtx = context.WithValue(childCtx, executionLeaseKey{}, struct{}{})
	results := make(chan childDispatchCompletion, min(group.Plan.MaxConcurrency, len(group.Children)))
	next, active := 0, 0
	ids := launchableChildIDs(group)
	cancelRequested := c.childCancellationSignal(identity)
	for {
		select {
		case <-cancelRequested:
			return c.childLaunchResult(identity)
		default:
		}
		for active < group.Plan.MaxConcurrency && next < len(ids) {
			childID := ids[next]
			next++
			invocation, launchErr := c.beginChild(workCtx, identity, childID)
			if launchErr != nil {
				return c.currentChildGroup(identity), launchErr
			}
			active++
			go c.dispatchChild(childCtx, identity, invocation, dispatch, results)
		}
		if active == 0 {
			return c.childLaunchResult(identity)
		}
		select {
		case <-cancelRequested:
			return c.childLaunchResult(identity)
		case <-ctx.Done():
			return c.currentChildGroup(identity), context.Cause(ctx)
		case result := <-results:
			active--
			if result.err != nil {
				return c.currentChildGroup(identity), result.err
			}
			if group.Plan.FailurePolicy == ChildFailFast && c.childGroupHasFailure(identity) {
				return c.currentChildGroup(identity), ErrChildrenUnresolved
			}
		}
	}
}

func (c *executionCheckpointer[T, E]) dispatchChild(ctx context.Context, identity string, invocation ChildInvocation,
	dispatch ChildDispatcher, results chan<- childDispatchCompletion) {
	outcome, dispatchErr := invokeChildDispatcher(ctx, invocation, dispatch)
	finishErr := c.finishChild(context.WithoutCancel(ctx), identity, invocation, outcome, dispatchErr)
	results <- childDispatchCompletion{err: finishErr}
}

func invokeChildDispatcher(
	ctx context.Context,
	invocation ChildInvocation,
	dispatch ChildDispatcher,
) (ChildResult, error) {
	if err := ctx.Err(); err != nil {
		return ChildResult{}, err
	}
	attempt := childDispatchAttempt{outcome: ChildResult{State: "", Payload: nil, Error: "", WaitID: ""}, err: nil}
	attempt.invoke(ctx, invocation, dispatch)
	return attempt.outcome, attempt.err
}

type childDispatchAttempt struct {
	outcome ChildResult
	err     error
}

func (attempt *childDispatchAttempt) invoke(ctx context.Context, invocation ChildInvocation, dispatch ChildDispatcher) {
	defer func() {
		if recovered := recover(); recovered != nil {
			attempt.err = fmt.Errorf("flowy: child dispatcher panicked: %v", recovered)
		}
	}()
	attempt.outcome, attempt.err = dispatch(ctx, invocation)
}

func (c *executionCheckpointer[T, E]) currentChildGroup(identity string) ChildGroupRecord {
	c.mu.Lock()
	defer c.mu.Unlock()
	groups, _ := executionChildGroups(c.envelope)
	return detachedChildGroup(groups[identity])
}

func (c *executionCheckpointer[T, E]) childGroupHasFailure(identity string) bool {
	group := c.currentChildGroup(identity)
	for _, child := range group.Children {
		if child.State == ChildFailed {
			return true
		}
	}
	return false
}

func (c *executionCheckpointer[T, E]) childLaunchResult(identity string) (ChildGroupRecord, error) {
	group := c.currentChildGroup(identity)
	for _, child := range group.Children {
		if child.State != ChildCompleted && child.State != ChildFailed && child.State != ChildCanceled {
			return group, ErrChildrenUnresolved
		}
	}
	return group, nil
}

func (c *executionCheckpointer[T, E]) recoverChildLaunches(
	ctx context.Context,
	identity string,
) (ChildGroupRecord, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	groups, err := executionChildGroups(c.envelope)
	if err != nil {
		return ChildGroupRecord{}, err
	}
	group := groups[identity]
	changed, pending := false, false
	for index, child := range group.Children {
		if child.State == ChildRunning {
			if child.Revision == ^uint64(0) {
				return ChildGroupRecord{}, ErrChildRevision
			}
			if child.Incarnation == c.lease.Incarnation {
				return detachedChildGroup(group), ErrChildBusy
			}
			group.Children[index].State = ChildUnknown
			group.Children[index].Revision++
			changed = true
		}
		pending = pending || child.State == ChildUnknown || child.State == ChildRunning ||
			(group.Plan.FailurePolicy == ChildFailFast && child.State == ChildFailed && !group.CancelRequested)
	}
	if changed {
		groups[identity] = group
		if err := c.persistChildGroupsLocked(ctx, groups); err != nil {
			return ChildGroupRecord{}, err
		}
	}
	if pending {
		return detachedChildGroup(group), ErrChildrenUnresolved
	}
	return detachedChildGroup(group), nil
}

func (c *executionCheckpointer[T, E]) beginChild(
	ctx context.Context,
	identity, childID string,
) (ChildInvocation, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	groups, err := executionChildGroups(c.envelope)
	if err != nil {
		return ChildInvocation{}, err
	}
	group := groups[identity]
	for index, child := range group.Children {
		if child.Spec.ID != childID {
			continue
		}
		if group.CancelRequested || child.Revision > ^uint64(0)-2 {
			return ChildInvocation{}, ErrChildRevision
		}
		if child.State != ChildPlanned && child.State != ChildQueued {
			return ChildInvocation{}, ErrChildRevision
		}
		if child.State == ChildPlanned {
			child.State, child.Revision = ChildQueued, child.Revision+1
			group.Children[index] = child
			groups[identity] = group
			if err := c.persistChildGroupsLocked(ctx, groups); err != nil {
				return ChildInvocation{}, err
			}
		}
		child.State, child.Revision, child.Incarnation = ChildRunning, child.Revision+1, c.lease.Incarnation
		group.Children[index] = child
		groups[identity] = group
		if err := c.persistChildGroupsLocked(ctx, groups); err != nil {
			return ChildInvocation{}, err
		}
		spec := cloneChildSpec(child.Spec)
		return ChildInvocation{
			ParentID:    group.ParentID,
			GroupKey:    group.Plan.Key,
			ChildID:     spec.ID,
			ExecutionID: child.ExecutionID,
			Revision:    child.Revision,
			Input:       spec.Input,
			Allocation:  spec.Allocation,
		}, nil
	}
	return ChildInvocation{}, ErrChildJoinInvalid
}

func (c *executionCheckpointer[T, E]) finishChild(ctx context.Context, identity string, invocation ChildInvocation,
	result ChildResult, dispatchErr error) error {
	result.Payload = bytes.Clone(result.Payload)
	c.mu.Lock()
	defer c.mu.Unlock()
	groups, err := executionChildGroups(c.envelope)
	if err != nil {
		return err
	}
	group := groups[identity]
	for index, child := range group.Children {
		if child.Spec.ID != invocation.ChildID {
			continue
		}
		if !childInvocationMatches(child, invocation, c.lease.Incarnation) {
			return ErrChildRevision
		}
		if dispatchErr != nil {
			result = ChildResult{State: ChildUnknown, Payload: nil, Error: dispatchErr.Error(), WaitID: ""}
		}
		if !validChildResultState(result.State) {
			return ErrChildJoinInvalid
		}
		child.State, child.Result, child.Error, child.WaitID = result.State, result.Payload, result.Error, result.WaitID
		if child.State == ChildCanceled && child.CancelRequested {
			child.CancelConfirmed = true
		}
		child.Revision++
		if err := validateChildRecordState(child); err != nil {
			return err
		}
		group.Children[index] = child
		groups[identity] = group
		if err := c.persistChildGroupsLocked(ctx, groups); err != nil {
			return err
		}
		if child.State == ChildUnknown {
			return ErrChildrenUnresolved
		}
		return nil
	}
	return ErrChildJoinInvalid
}

func validChildResultState(state ChildState) bool {
	switch state {
	case ChildCompleted, ChildFailed, ChildWaiting, ChildUnknown, ChildCanceled:
		return true
	case ChildPlanned, ChildQueued, ChildRunning:
		return false
	}
	return false
}

func childInvocationMatches(child ChildRecord, invocation ChildInvocation, incarnation uint64) bool {
	matchesRevision := child.Revision == invocation.Revision ||
		(child.CancelRequested && invocation.Revision < ^uint64(0) && child.Revision == invocation.Revision+1)
	return child.State == ChildRunning && matchesRevision && child.Revision < ^uint64(0) &&
		child.Incarnation == incarnation
}
