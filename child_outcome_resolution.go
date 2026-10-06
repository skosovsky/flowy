package flowy

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"time"
)

// ChildOutcomeResolution addresses a host-confirmed outcome of unknown work.
// Node is the original group node, including after an explicit cursor migration.
// Authorization and the truth of Evidence belong to the host.
type ChildOutcomeResolution struct {
	Node          ExecutionPointer
	Activation    uint64
	GroupKey      string
	GroupLabel    string
	ChildID       string
	ExecutionID   string
	ChildRevision uint64
	DecisionID    string
	Reason        string
	Evidence      string
	Result        ChildResult
}

// ChildOutcomeResolutionRecord preserves a terminal decision and its prior
// ownership. RequestDigest binds the result and evidence to the original group.
type ChildOutcomeResolutionRecord struct {
	DecisionID       string     `json:"decision_id"`
	Reason           string     `json:"reason"`
	Evidence         string     `json:"evidence"`
	RequestDigest    string     `json:"request_digest"`
	PriorState       ChildState `json:"prior_state"`
	ChildRevision    uint64     `json:"child_revision"`
	PriorIncarnation uint64     `json:"prior_incarnation"`
	SourceRevision   uint64     `json:"source_revision"`
	Incarnation      uint64     `json:"incarnation"`
	At               time.Time  `json:"at"`
}

// ResolveChildOutcome atomically settles one unknown child, without node,
// codec, dispatch or merge calls. Replay requires an exact current parent token;
// stale tokens are never advanced to latest implicitly.
func (r *DurableRunner[T, E]) ResolveChildOutcome(ctx context.Context, token ResumeToken,
	resolution ChildOutcomeResolution) (ResumeToken, error) {
	if !validChildOutcomeRequest(resolution) {
		return ResumeToken{}, ErrChildJoinInvalid
	}
	resolution.Result.Payload = bytes.Clone(resolution.Result.Payload)
	session, err := r.acquireSession(ctx, token.ThreadID)
	if err != nil {
		return ResumeToken{}, err
	}
	defer session.finish()
	source, err := r.childMutationSource(session.ctx, token)
	if err != nil {
		return ResumeToken{}, err
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
	if !exists || !currentChildGroup(source, group) || len(group.MergedIDs) != 0 ||
		group.Plan.Label != resolution.GroupLabel {
		return ResumeToken{}, ErrChildRevision
	}
	replay, err := applyChildOutcomeResolution(
		&group,
		resolution,
		groups,
		source.Revision,
		session.lease.Incarnation,
		r.options.Clock.Now().UTC(),
	)
	if err != nil {
		return ResumeToken{}, err
	}
	if session.ctx.Err() != nil {
		return ResumeToken{}, context.Cause(session.ctx)
	}
	event.Code = childObservationCode(resolution.Result.State)
	if replay {
		event.Stage, event.Revision = LifecycleReplayed, source.Revision
		return token, nil
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
	event.Stage, event.Revision = LifecycleCommitted, committed.Revision
	return ResumeToken{ThreadID: token.ThreadID, SnapshotRevision: committed.Revision}, nil
}

func validChildOutcomeRequest(resolution ChildOutcomeResolution) bool {
	for _, text := range []string{string(resolution.Node), resolution.GroupKey, resolution.GroupLabel, resolution.ChildID, resolution.ExecutionID, resolution.DecisionID, resolution.Reason, resolution.Evidence} {
		if text == "" || !validRuntimeText(text) {
			return false
		}
	}
	if resolution.Activation == 0 || resolution.ChildRevision <= 1 || resolution.ChildRevision == ^uint64(0) ||
		resolution.Result.WaitID != "" ||
		!validRuntimeText(resolution.Result.Error) {
		return false
	}
	switch resolution.Result.State {
	case ChildCompleted:
		return resolution.Result.Error == ""
	case ChildFailed:
		return resolution.Result.Error != ""
	case ChildPlanned, ChildQueued, ChildRunning, ChildWaiting, ChildCanceled, ChildUnknown:
		return false
	}
	return false
}

func childOutcomeRequestDigest(resolution ChildOutcomeResolution) string {
	encoded, _ := json.Marshal(
		[]any{
			"flowy-child-outcome-v1",
			resolution.Node,
			resolution.Activation,
			resolution.GroupKey,
			resolution.GroupLabel,
			resolution.ChildID,
			resolution.ExecutionID,
			resolution.ChildRevision,
			resolution.DecisionID,
			resolution.Reason,
			resolution.Evidence,
			resolution.Result.State,
			canonicalChildOutcomePayload(resolution.Result.Payload),
			resolution.Result.Error,
			resolution.Result.WaitID,
		},
	)
	digest := sha256.Sum256(encoded)
	return hex.EncodeToString(digest[:])
}

func applyChildOutcomeResolution(group *ChildGroupRecord, resolution ChildOutcomeResolution,
	groups map[string]ChildGroupRecord, sourceRevision, incarnation uint64, at time.Time) (bool, error) {
	if at.IsZero() || incarnation == 0 {
		return false, ErrExecutionCapability
	}
	digest := childOutcomeRequestDigest(resolution)
	if replay, err := childOutcomeDecisionReplay(groups, resolution, digest); replay || err != nil {
		return replay, err
	}

	for index, child := range group.Children {
		if child.Spec.ID != resolution.ChildID {
			continue
		}
		if child.ExecutionID != resolution.ExecutionID || child.Revision != resolution.ChildRevision ||
			child.OutcomeResolution != nil ||
			child.CancelConfirmed {
			return false, ErrChildRevision
		}
		if child.State != ChildUnknown && (child.State != ChildRunning || child.Incarnation >= incarnation) {
			return false, ErrChildRevision
		}
		child.OutcomeResolution = &ChildOutcomeResolutionRecord{
			DecisionID:       resolution.DecisionID,
			Reason:           resolution.Reason,
			Evidence:         resolution.Evidence,
			RequestDigest:    digest,
			PriorState:       child.State,
			ChildRevision:    child.Revision,
			PriorIncarnation: child.Incarnation,
			SourceRevision:   sourceRevision,
			Incarnation:      incarnation,
			At:               at,
		}
		child.State, child.Result, child.Error, child.WaitID = resolution.Result.State, bytes.Clone(
			canonicalChildOutcomePayload(resolution.Result.Payload),
		), resolution.Result.Error, ""
		child.Revision++
		if err := validateChildRecordState(child); err != nil {
			return false, err
		}
		group.Children[index] = child
		return false, nil
	}
	return false, ErrChildRevision
}

func validChildOutcomeResolution(child ChildRecord) bool {
	prior := child.OutcomeResolution
	if prior == nil {
		return true
	}
	return (child.State == ChildCompleted || child.State == ChildFailed) && child.WaitID == "" &&
		!child.CancelConfirmed &&
		child.WaitResolution == nil &&
		child.CancelConfirmation == nil &&
		prior.DecisionID != "" &&
		prior.Reason != "" &&
		prior.Evidence != "" &&
		prior.RequestDigest != "" &&
		validRuntimeText(prior.DecisionID, prior.Reason, prior.Evidence, child.Error) &&
		prior.ChildRevision > 1 &&
		prior.ChildRevision < ^uint64(0) &&
		child.Revision == prior.ChildRevision+1 &&
		prior.PriorIncarnation > 0 &&
		child.Incarnation == prior.PriorIncarnation &&
		prior.SourceRevision > 0 &&
		prior.Incarnation > prior.PriorIncarnation &&
		!prior.At.IsZero() &&
		(prior.PriorState == ChildUnknown || prior.PriorState == ChildRunning)
}

func validChildOutcomeGroupProvenance(group ChildGroupRecord, child ChildRecord, revision uint64) bool {
	prior := child.OutcomeResolution
	if prior == nil {
		return true
	}
	resolution := ChildOutcomeResolution{
		Node: group.Node, Activation: group.Activation, GroupKey: group.Plan.Key, GroupLabel: group.Plan.Label,
		ChildID: child.Spec.ID, ExecutionID: child.ExecutionID, ChildRevision: prior.ChildRevision,
		DecisionID: prior.DecisionID, Reason: prior.Reason, Evidence: prior.Evidence,
		Result: ChildResult{State: child.State, Payload: child.Result, Error: child.Error, WaitID: child.WaitID},
	}
	return prior.SourceRevision < revision && validChildOutcomeRequest(resolution) &&
		childOutcomeRequestDigest(resolution) == prior.RequestDigest
}

func childOutcomeDecisionReplay(
	groups map[string]ChildGroupRecord,
	resolution ChildOutcomeResolution,
	digest string,
) (bool, error) {
	for _, priorGroup := range groups {
		for _, child := range priorGroup.Children {
			prior := child.OutcomeResolution
			if prior != nil && prior.DecisionID == resolution.DecisionID {
				if child.ExecutionID == resolution.ExecutionID && prior.RequestDigest == digest {
					return true, nil
				}
				return false, ErrChildRevision
			}
		}
	}
	return false, nil
}

func validChildOutcomeDecisionIDs(groups map[string]ChildGroupRecord) bool {
	seen := make(map[string]bool)
	for _, group := range groups {
		for _, child := range group.Children {
			if child.OutcomeResolution == nil {
				continue
			}
			id := child.OutcomeResolution.DecisionID
			if seen[id] {
				return false
			}
			seen[id] = true
		}
	}
	return true
}

// Empty opaque payloads have one wire representation (ChildRecord uses omitempty).
func canonicalChildOutcomePayload(payload []byte) []byte {
	if len(payload) == 0 {
		return nil
	}
	return payload
}
