package flowy

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"time"
)

// ChildWaitResolution addresses a settled external wait, not an instruction to
// dispatch a child. Host validates authorization and the business result.
type ChildWaitResolution struct {
	Node          ExecutionPointer
	Activation    uint64
	GroupKey      string
	ChildID       string
	ExecutionID   string
	ChildRevision uint64
	WaitID        string
	DecisionID    string
	Result        ChildResult
}

type ChildWaitResolutionRecord struct {
	WaitID         string    `json:"wait_id"`
	DecisionID     string    `json:"decision_id"`
	ChildRevision  uint64    `json:"child_revision"`
	SourceRevision uint64    `json:"source_revision"`
	Incarnation    uint64    `json:"incarnation"`
	At             time.Time `json:"at"`
}

// ResolveChildWait atomically resolves exactly one waiting child without
// invoking codecs, nodes or dispatch. It does not resume the parent implicitly.
//
//nolint:nonamedreturns // Preserve the decision token while joining session cleanup errors.
func (r *DurableRunner[T, E]) ResolveChildWait(ctx context.Context, token ResumeToken,
	resolution ChildWaitResolution) (result ResumeToken, retErr error) {
	if resolution.Node == "" || resolution.Activation == 0 || resolution.GroupKey == "" || resolution.ChildID == "" ||
		!validRuntimeText(string(resolution.Node), resolution.GroupKey, resolution.ChildID, resolution.ExecutionID,
			resolution.WaitID, resolution.DecisionID, resolution.Result.Error) ||
		resolution.ExecutionID == "" || resolution.ChildRevision == 0 || resolution.WaitID == "" || resolution.DecisionID == "" ||
		(resolution.Result.State != ChildCompleted && resolution.Result.State != ChildFailed) || resolution.Result.WaitID != "" {
		return ResumeToken{}, ErrChildJoinInvalid
	}
	resolution.Result.Payload = bytes.Clone(resolution.Result.Payload)
	session, err := r.acquireSession(ctx, token.ThreadID)
	if err != nil {
		return ResumeToken{}, err
	}
	defer func() { retErr = errors.Join(retErr, session.finish()) }()
	source, err := r.childMutationSource(session.ctx, token)
	if err != nil {
		return ResumeToken{}, err
	}
	if source.Activation != resolution.Activation {
		return ResumeToken{}, ErrChildRevision
	}
	session.ctx = restoreExecutionTelemetry(session.ctx, source)
	event := childObservation(
		source,
		LifecycleChildResolve,
		childExecutionIdentity(
			source.ExecutionID,
			resolution.Node,
			resolution.Activation,
			resolution.GroupKey,
			"",
		),
		resolution.ChildID,
	)
	event.Stage, event.ChildExecutionID, event.DecisionID = LifecycleStarted, resolution.ExecutionID, resolution.DecisionID
	event.Node = resolution.Node
	observeLifecycle(session.ctx, event)
	event.Stage = LifecycleFailed
	defer func() { observeLifecycle(session.ctx, event) }()
	groups, err := executionChildGroups(source)
	if err != nil {
		return ResumeToken{}, err
	}
	identity := childExecutionIdentity(
		source.ExecutionID,
		resolution.Node,
		resolution.Activation,
		resolution.GroupKey,
		"",
	)
	group, exists := groups[identity]
	if !exists || !currentChildGroup(source, group) || len(group.MergedIDs) != 0 {
		return ResumeToken{}, ErrChildRevision
	}
	if resolveErr := applyChildWaitResolution(
		&group,
		resolution,
		source.Revision,
		session.lease.Incarnation,
		r.options.Clock.Now().UTC(),
	); resolveErr != nil {
		return ResumeToken{}, resolveErr
	}
	groups[identity] = group
	target := cloneExecutionEnvelope(source)
	target.ChildrenPayload, err = json.Marshal(groups)
	if err != nil {
		return ResumeToken{}, err
	}
	if session.ctx.Err() != nil {
		return ResumeToken{}, context.Cause(session.ctx)
	}
	committed, err := commitExecution(session.ctx, r.store, source.Revision, session.lease, target)
	if err != nil {
		return ResumeToken{}, err
	}
	event.Stage, event.Revision, event.Code = LifecycleCommitted, committed.Revision, childObservationCode(
		resolution.Result.State,
	)
	return ResumeToken{ThreadID: token.ThreadID, SnapshotRevision: committed.Revision}, nil
}

func applyChildWaitResolution(
	group *ChildGroupRecord,
	resolution ChildWaitResolution,
	sourceRevision, incarnation uint64,
	at time.Time,
) error {
	for index, child := range group.Children {
		if child.Spec.ID != resolution.ChildID {
			continue
		}
		if child.ExecutionID != resolution.ExecutionID || child.Revision != resolution.ChildRevision ||
			child.Revision == ^uint64(
				0,
			) || child.State != ChildWaiting || child.WaitID != resolution.WaitID || child.WaitResolution != nil {
			return ErrChildRevision
		}
		child.State, child.Result, child.Error, child.WaitID = resolution.Result.State, resolution.Result.Payload, resolution.Result.Error, ""
		child.Revision++
		child.WaitResolution = &ChildWaitResolutionRecord{WaitID: resolution.WaitID, DecisionID: resolution.DecisionID,
			ChildRevision: resolution.ChildRevision, SourceRevision: sourceRevision, Incarnation: incarnation, At: at}
		if err := validateChildRecordState(child); err != nil {
			return err
		}
		group.Children[index] = child
		return nil
	}
	return ErrChildRevision
}

func validChildWaitResolution(child ChildRecord) bool {
	decision := child.WaitResolution
	if decision == nil {
		return true
	}
	return decision.WaitID != "" && decision.DecisionID != "" && decision.SourceRevision > 0 &&
		validRuntimeText(decision.WaitID, decision.DecisionID, child.Error) &&
		decision.Incarnation > 0 &&
		!decision.At.IsZero() &&
		decision.ChildRevision > 2 &&
		decision.ChildRevision < ^uint64(0) &&
		child.Revision == decision.ChildRevision+1 &&
		(child.State == ChildCompleted || child.State == ChildFailed) &&
		child.WaitID == ""
}
