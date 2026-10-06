package flowy

import (
	"context"
	"encoding/json"
	"time"
)

// ChildCancelConfirmation carries host evidence of stopping, not a timeout or
// acknowledgement. Runtime does not authenticate or establish its truth.
type ChildCancelConfirmation struct {
	Node          ExecutionPointer
	Activation    uint64
	GroupKey      string
	ChildID       string
	ExecutionID   string
	ChildRevision uint64
	RequestID     string
	DecisionID    string
	Reason        string
	Evidence      string
}

type ChildCancelConfirmationRecord struct {
	RequestID      string     `json:"request_id"`
	DecisionID     string     `json:"decision_id"`
	Reason         string     `json:"reason"`
	Evidence       string     `json:"evidence"`
	PriorState     ChildState `json:"prior_state"`
	ChildRevision  uint64     `json:"child_revision"`
	SourceRevision uint64     `json:"source_revision"`
	Incarnation    uint64     `json:"incarnation"`
	At             time.Time  `json:"at"`
}

// ConfirmChildCancellation commits one addressed stop confirmation without
// codec/node/dispatch calls. Stale decisions cannot replace a committed outcome.
func (r *DurableRunner[T, E]) ConfirmChildCancellation(ctx context.Context, token ResumeToken,
	decision ChildCancelConfirmation) (ResumeToken, error) {
	if decision.Node == "" || decision.Activation == 0 || decision.GroupKey == "" || decision.ChildID == "" ||
		!validRuntimeText(string(decision.Node), decision.GroupKey, decision.ChildID, decision.ExecutionID,
			decision.RequestID, decision.DecisionID, decision.Evidence) ||
		decision.ExecutionID == "" || decision.ChildRevision == 0 || decision.RequestID == "" || decision.DecisionID == "" ||
		decision.Reason == "" || decision.Evidence == "" {
		return ResumeToken{}, ErrChildJoinInvalid
	}
	session, err := r.acquireSession(ctx, token.ThreadID)
	if err != nil {
		return ResumeToken{}, err
	}
	defer session.finish()
	source, err := r.childMutationSource(session.ctx, token)
	if err != nil {
		return ResumeToken{}, err
	}
	if source.Activation != decision.Activation {
		return ResumeToken{}, ErrChildRevision
	}
	session.ctx = restoreExecutionTelemetry(session.ctx, source)
	event := childObservation(
		source,
		LifecycleChildCancel,
		childExecutionIdentity(
			source.ExecutionID,
			decision.Node,
			decision.Activation,
			decision.GroupKey,
			"",
		),
		decision.ChildID,
	)
	event.Stage, event.ChildExecutionID, event.DecisionID = LifecycleStarted, decision.ExecutionID, decision.DecisionID
	event.Node = decision.Node
	observeLifecycle(session.ctx, event)
	event.Stage = LifecycleFailed
	defer func() { observeLifecycle(session.ctx, event) }()
	groups, err := executionChildGroups(source)
	if err != nil {
		return ResumeToken{}, err
	}
	identity := childExecutionIdentity(source.ExecutionID, decision.Node, decision.Activation, decision.GroupKey, "")
	group, exists := groups[identity]
	if !exists || !currentChildGroup(source, group) || len(group.MergedIDs) != 0 || group.CancelRequest == nil ||
		group.CancelRequest.ID != decision.RequestID {
		return ResumeToken{}, ErrChildRevision
	}
	if confirmErr := applyChildCancelConfirmation(
		&group,
		decision,
		source.Revision,
		session.lease.Incarnation,
		r.options.Clock.Now().UTC(),
	); confirmErr != nil {
		return ResumeToken{}, confirmErr
	}
	event.Code = "child_canceled"
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
	event.Stage, event.Revision = LifecycleCommitted, committed.Revision
	return ResumeToken{ThreadID: token.ThreadID, SnapshotRevision: committed.Revision}, nil
}

func (r *DurableRunner[T, E]) childMutationSource(ctx context.Context, token ResumeToken) (ExecutionEnvelope, error) {
	source, err := r.store.LoadExecution(ctx, token.ThreadID)
	if err != nil {
		return ExecutionEnvelope{}, err
	}
	if integrityErr := ValidateExecutionIntegrity(source, token.ThreadID, source.Revision); integrityErr != nil {
		return ExecutionEnvelope{}, integrityErr
	}
	if token.SnapshotRevision == 0 || token.SnapshotRevision != source.Revision {
		return ExecutionEnvelope{}, ErrConcurrencyConflict
	}
	if collectionsErr := validateExecutionCollections(source); collectionsErr != nil {
		return ExecutionEnvelope{}, collectionsErr
	}
	if descriptorErr := source.Descriptor.Check(r.descriptor); descriptorErr != nil {
		return ExecutionEnvelope{}, descriptorErr
	}
	if source.Terminal != nil {
		return ExecutionEnvelope{}, ErrChildRevision
	}
	if pointerErr := r.validatePointer(source.Progress.ExecutionPointer); pointerErr != nil {
		return ExecutionEnvelope{}, pointerErr
	}
	if source.Import != nil && (source.Import.ImporterID == "" || source.Import.Source.Validate() != nil) {
		return ExecutionEnvelope{}, ErrExecutionImportInvalid
	}
	return source, nil
}

func applyChildCancelConfirmation(group *ChildGroupRecord, decision ChildCancelConfirmation,
	sourceRevision, incarnation uint64, at time.Time) error {
	for index, child := range group.Children {
		if child.Spec.ID != decision.ChildID {
			continue
		}
		if child.ExecutionID != decision.ExecutionID || child.Revision != decision.ChildRevision ||
			child.Revision == ^uint64(0) ||
			!child.CancelRequested ||
			child.CancelConfirmed ||
			!unresolvedCancelableChild(child.State) {
			return ErrChildRevision
		}
		child.CancelConfirmation = &ChildCancelConfirmationRecord{
			RequestID:      decision.RequestID,
			DecisionID:     decision.DecisionID,
			Reason:         decision.Reason,
			Evidence:       decision.Evidence,
			PriorState:     child.State,
			ChildRevision:  child.Revision,
			SourceRevision: sourceRevision,
			Incarnation:    incarnation,
			At:             at,
		}
		child.State, child.CancelConfirmed, child.WaitID = ChildCanceled, true, ""
		child.Revision++
		if err := validateChildRecordState(child); err != nil {
			return err
		}
		group.Children[index] = child
		return nil
	}
	return ErrChildRevision
}

func unresolvedCancelableChild(state ChildState) bool {
	return state == ChildRunning || state == ChildWaiting || state == ChildUnknown
}

func validChildCancelConfirmation(child ChildRecord) bool {
	decision := child.CancelConfirmation
	if decision == nil {
		return true
	}
	return child.State == ChildCanceled && child.CancelConfirmed && child.CancelRequested && child.WaitID == "" &&
		decision.RequestID != "" && decision.DecisionID != "" && decision.Reason != "" && decision.Evidence != "" &&
		unresolvedCancelableChild(
			decision.PriorState,
		) && decision.ChildRevision > 1 && decision.ChildRevision < ^uint64(0) &&
		child.Revision == decision.ChildRevision+1 && decision.SourceRevision > 0 && decision.Incarnation > 0 && !decision.At.IsZero()
}
