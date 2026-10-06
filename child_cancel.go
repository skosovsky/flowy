package flowy

import (
	"bytes"
	"context"
	"time"
)

type ChildCancelRequest struct {
	ID     string
	Reason string
}

type ChildCancelRequestRecord struct {
	ID             string    `json:"id"`
	Reason         string    `json:"reason"`
	SourceRevision uint64    `json:"source_revision"`
	Incarnation    uint64    `json:"incarnation"`
	At             time.Time `json:"at"`
}

type ChildCancelNotice struct {
	ParentID      string
	GroupKey      string
	ChildID       string
	ExecutionID   string
	ChildRevision uint64
	RequestID     string
	Reason        string
}

// ChildCancelNotifier delivers an idempotent stop request. A nil error is only
// delivery acknowledgement, never proof of remote termination or rollback.
type ChildCancelNotifier func(context.Context, ChildCancelNotice) error

// CancelChildren persists the request before notification and stops admission.
// Repeating the same request can repeat notification with the same identities.
func CancelChildren(ctx context.Context, group ChildGroupRecord, request ChildCancelRequest,
	notify ChildCancelNotifier) (ChildGroupRecord, error) {
	backend, ok := ctx.Value(childGroupContextKey{}).(childGroupBackend)
	if !ok {
		return ChildGroupRecord{}, ErrExecutionCapability
	}
	if request.ID == "" || !validRuntimeText(request.ID) || request.Reason == "" || notify == nil ||
		!validChildGroupText(group) {
		return ChildGroupRecord{}, ErrChildJoinInvalid
	}
	return backend.cancelChildren(ctx, group, request, notify)
}

func (c *executionCheckpointer[T, E]) cancelChildren(ctx context.Context, expected ChildGroupRecord,
	request ChildCancelRequest, notify ChildCancelNotifier) (ChildGroupRecord, error) {
	if c.forkMode == ForkFake {
		notify = func(context.Context, ChildCancelNotice) error { return nil }
	}
	if c.forkMode == ForkLive {
		liveNotify := notify
		notify = func(ctx context.Context, notice ChildCancelNotice) error {
			if err := c.authorizeForkDispatch(ctx); err != nil {
				return err
			}
			return liveNotify(ctx, notice)
		}
	}
	identity := childExecutionIdentity(expected.ParentID, expected.Node, expected.Activation, expected.Plan.Key, "")
	group, err := c.requestChildCancellation(ctx, identity, expected, request)
	if err != nil {
		return group, err
	}
	for _, child := range group.Children {
		if !child.CancelRequested || settledChild(child) {
			continue
		}
		if contextErr := ctx.Err(); contextErr != nil {
			return c.currentChildGroup(identity), contextErr
		}
		if notifyErr := notify(ctx, ChildCancelNotice{ParentID: group.ParentID, GroupKey: group.Plan.Key,
			ChildID: child.Spec.ID, ExecutionID: child.ExecutionID, ChildRevision: child.Revision,
			RequestID: request.ID, Reason: request.Reason}); notifyErr != nil {
			return c.currentChildGroup(identity), notifyErr
		}
	}
	return c.currentChildGroup(identity), nil
}

func (c *executionCheckpointer[T, E]) requestChildCancellation(ctx context.Context, identity string,
	expected ChildGroupRecord, request ChildCancelRequest) (ChildGroupRecord, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	groups, err := executionChildGroups(c.envelope)
	if err != nil {
		return ChildGroupRecord{}, err
	}
	group, exists := groups[identity]
	if !exists || !currentChildGroup(c.envelope, group) ||
		!bytes.Equal(childGroupEncoding(group), childGroupEncoding(expected)) || len(group.MergedIDs) != 0 {
		return ChildGroupRecord{}, ErrChildRevision
	}
	if group.CancelRequest != nil {
		if group.CancelRequest.ID != request.ID || group.CancelRequest.Reason != request.Reason {
			return ChildGroupRecord{}, ErrChildRevision
		}
		return detachedChildGroup(group), nil
	}
	group.CancelRequested = true
	group.CancelRequest = &ChildCancelRequestRecord{ID: request.ID, Reason: request.Reason,
		SourceRevision: c.envelope.Revision, Incarnation: c.lease.Incarnation, At: c.clock.Now().UTC()}
	for index, child := range group.Children {
		if settledChild(child) {
			continue
		}
		if child.Revision == ^uint64(0) || (child.State == ChildRunning && child.Revision >= ^uint64(0)-1) {
			return ChildGroupRecord{}, ErrChildRevision
		}
		child.CancelRequested = true
		child.Revision++
		if child.State == ChildPlanned || child.State == ChildQueued {
			child.State, child.CancelConfirmed = ChildCanceled, true
		}
		group.Children[index] = child
	}
	groups[identity] = group
	if err := c.persistChildGroupsLocked(ctx, groups); err != nil {
		return ChildGroupRecord{}, err
	}
	if signal := c.childCancellationSignals[identity]; signal != nil {
		close(signal)
	}
	return detachedChildGroup(group), nil
}

func (c *executionCheckpointer[T, E]) childCancellationSignal(identity string) <-chan struct{} {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.childCancellationSignals == nil {
		c.childCancellationSignals = make(map[string]chan struct{})
	}
	if signal := c.childCancellationSignals[identity]; signal != nil {
		return signal
	}
	signal := make(chan struct{})
	c.childCancellationSignals[identity] = signal
	groups, _ := executionChildGroups(c.envelope)
	if groups[identity].CancelRequested {
		close(signal)
	}
	return signal
}

func validChildCancellation(group ChildGroupRecord) bool {
	request := group.CancelRequest
	if !group.CancelRequested {
		if request != nil {
			return false
		}
		for _, child := range group.Children {
			if child.CancelRequested || child.CancelConfirmed {
				return false
			}
		}
		return true
	}
	if request == nil || request.ID == "" || request.Reason == "" || request.SourceRevision == 0 ||
		request.Incarnation == 0 ||
		request.At.IsZero() {
		return false
	}
	for _, child := range group.Children {
		if child.State == ChildPlanned || child.State == ChildQueued {
			return false
		}
		if !settledChild(child) && !child.CancelRequested {
			return false
		}
	}
	return true
}
