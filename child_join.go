package flowy

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
)

// ChildMerge is a pure host projection of detached, ID-ordered outcomes. It
// must not perform external effects: a failed commit can require recomputation.
type ChildMerge func(context.Context, []ChildRecord) ([]byte, error)

// JoinChildren atomically persists an ordered merge of settled children. The
// supplied group is an exact revision assertion, not a source of new outcomes.
// Once committed, replay returns the cached bytes without invoking merge.
func JoinChildren(ctx context.Context, group ChildGroupRecord, merge ChildMerge) ([]byte, error) {
	backend, ok := ctx.Value(childGroupContextKey{}).(childGroupBackend)
	if !ok {
		return nil, ErrExecutionCapability
	}
	if merge == nil || !validChildGroupText(group) {
		return nil, ErrChildJoinInvalid
	}
	return backend.joinChildren(ctx, group, merge)
}

func validChildJoin(group ChildGroupRecord) bool {
	if len(group.MergedIDs) == 0 {
		return len(group.MergedResult) == 0
	}
	if len(group.MergedIDs) != len(group.Children) {
		return false
	}
	for index, child := range group.Children {
		if group.MergedIDs[index] != child.Spec.ID || !settledChild(child) {
			return false
		}
	}
	return true
}

func settledChild(child ChildRecord) bool {
	return child.State == ChildCompleted || child.State == ChildFailed || child.State == ChildCanceled
}

func childGroupEncoding(group ChildGroupRecord) []byte {
	encoded, _ := json.Marshal(group)
	return encoded
}

func (c *executionCheckpointer[T, E]) joinChildren(ctx context.Context, expected ChildGroupRecord,
	merge ChildMerge) ([]byte, error) {
	identity := childExecutionIdentity(expected.ParentID, expected.Node, expected.Activation, expected.Plan.Key, "")
	c.mu.Lock()
	groups, err := executionChildGroups(c.envelope)
	group, exists := groups[identity]
	if err != nil || !exists || !currentChildGroup(c.envelope, group) ||
		!bytes.Equal(childGroupEncoding(group), childGroupEncoding(expected)) {
		c.mu.Unlock()
		return nil, errors.Join(ErrChildRevision, err)
	}
	if len(group.MergedIDs) == len(group.Children) {
		event := childObservation(c.envelope, LifecycleChildJoin, identity, "")
		event.Stage, event.Revision = LifecycleReplayed, c.envelope.Revision
		c.mu.Unlock()
		observeLifecycle(ctx, event)
		return bytes.Clone(group.MergedResult), nil
	}
	for _, child := range group.Children {
		if !settledChild(child) {
			c.mu.Unlock()
			return nil, ErrChildrenUnresolved
		}
	}
	revision := c.envelope.Revision
	event := childObservation(c.envelope, LifecycleChildJoin, identity, "")
	event.Stage = LifecycleStarted
	c.mu.Unlock()
	observeLifecycle(ctx, event)
	event.Stage = LifecycleFailed
	if contextErr := ctx.Err(); contextErr != nil {
		observeLifecycle(ctx, event)
		return nil, contextErr
	}
	result, err := merge(ctx, detachedChildGroup(group).Children)
	if err != nil {
		observeLifecycle(ctx, event)
		return nil, errors.Join(ErrChildMergeConflict, err)
	}
	result = bytes.Clone(result)
	c.mu.Lock()
	defer func() {
		c.mu.Unlock()
		observeLifecycle(ctx, event)
	}()
	if contextErr := ctx.Err(); contextErr != nil {
		return nil, contextErr
	}
	if c.envelope.Revision != revision {
		return nil, ErrChildMergeConflict
	}
	group.MergedIDs = make([]string, len(group.Children))
	for index, child := range group.Children {
		group.MergedIDs[index] = child.Spec.ID
	}
	group.MergedResult = result
	groups[identity] = group
	if err := c.persistChildGroupsLocked(ctx, groups); err != nil {
		return nil, err
	}
	event.Stage, event.Revision = LifecycleCommitted, c.envelope.Revision
	return bytes.Clone(result), nil
}
