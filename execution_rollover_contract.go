package flowy

import (
	"context"
	"errors"
	"time"
)

var (
	ErrExecutionLifecycleUnsupported = errors.New("flowy: execution lifecycle unsupported")
	ErrExecutionLifecycleUnsafe      = errors.New("flowy: unsafe execution lifecycle boundary")
	ErrExecutionLifecycleLimit       = errors.New("flowy: execution lifecycle limit")
	ErrExecutionRolloverConflict     = errors.New("flowy: conflicting execution rollover")
	ErrExecutionTransferred          = errors.New("flowy: execution continuation transferred")
)

// RolloverPolicy bounds a host-selected cycle and the fresh projected payload.
// It is not a scheduler or a limit on work admitted inside an arbitrary node.
type RolloverPolicy struct {
	Label             string `json:"label"`
	MaxRecords        int    `json:"max_records"`
	MaxAggregateBytes int    `json:"max_aggregate_bytes"`
	MaxTargetBytes    int    `json:"max_target_bytes"`
}

func (p RolloverPolicy) Validate() error {
	if p.Label == "" || !validRuntimeText(p.Label) || p.MaxRecords <= 0 || p.MaxAggregateBytes <= 0 ||
		p.MaxTargetBytes <= 0 {
		return ErrExecutionLifecycleLimit
	}
	return nil
}

// RolloverPayload is detached BYOT data. Runtime result journals are excluded.
type RolloverPayload struct {
	Progress       MigrationState
	EffectsPayload []byte
}

// RolloverRequest transfers one continuation. Projection is pure; the host
// owns opaque external-reference sanitation and labels its semantic contract.
type RolloverRequest struct {
	SourceDescriptor ExecutionDescriptor
	DecisionID       string
	TargetID         string
	Policy           RolloverPolicy
	ProjectionLabel  string
	Project          func(RolloverPayload) (RolloverPayload, error)
}

// RolloverLineage is an immutable creation anchor, independent of source data
// availability. It is not permission to bypass the target lease or descriptor.
type RolloverLineage struct {
	Source           HistoricalCheckpointReference `json:"source"`
	SourceDescriptor ExecutionDescriptor           `json:"source_descriptor"`
	TargetDescriptor ExecutionDescriptor           `json:"target_descriptor"`
	TargetID         string                        `json:"target_id"`
	DecisionID       string                        `json:"decision_id"`
	Policy           RolloverPolicy                `json:"policy"`
	ProjectionLabel  string                        `json:"projection_label"`
	RequestDigest    string                        `json:"request_digest"`
	CreatedAt        time.Time                     `json:"created_at"`
}

func (l RolloverLineage) Validate() error {
	if l.Source.Validate() != nil || l.SourceDescriptor.Validate() != nil || l.TargetDescriptor.Validate() != nil ||
		l.Policy.Validate() != nil ||
		l.TargetID == "" ||
		l.TargetID == l.Source.ExecutionID ||
		l.DecisionID == "" ||
		l.ProjectionLabel == "" ||
		!validRuntimeText(l.TargetID, l.DecisionID, l.ProjectionLabel) ||
		!validActivityDigest(l.RequestDigest) ||
		!activityScheduleTimeValid(l.CreatedAt) ||
		l.CreatedAt.Location() != time.UTC {
		return ErrExecutionCorrupt
	}
	if rolloverRequestDigest(
		l.Source,
		l.SourceDescriptor,
		l.TargetDescriptor,
		l.TargetID,
		l.DecisionID,
		l.Policy,
		l.ProjectionLabel,
	) != l.RequestDigest {
		return ErrExecutionCorrupt
	}
	return nil
}

// RolloverReceipt survives cleanup of either full payload. Replays return the
// exact initial target token; a target that advanced requires explicit loading.
type RolloverReceipt struct {
	Lineage        RolloverLineage               `json:"lineage"`
	Target         HistoricalCheckpointReference `json:"target"`
	SourceRevision uint64                        `json:"source_revision"`
}

func (r RolloverReceipt) Validate() error {
	if r.Lineage.Validate() != nil || r.Target.Validate() != nil || r.Target.ExecutionID != r.Lineage.TargetID ||
		r.Target.Revision != 1 ||
		r.Lineage.Source.Revision == ^uint64(0) ||
		r.SourceRevision != r.Lineage.Source.Revision+1 {
		return ErrExecutionCorrupt
	}
	return nil
}

// ExecutionRolloverStore atomically ends source authority and creates target.
// Both creation anchors and monotonic fences survive retention. A publication
// is all-or-nothing, including after an uncertain commit acknowledgement.
// Ordinary CommitExecution must reject forged rollover transitions.
type ExecutionRolloverStore interface {
	LoadRollover(ctx context.Context, sourceID string) (*RolloverReceipt, error)
	CommitRollover(
		ctx context.Context,
		lease ExecutionLease,
		source HistoricalCheckpointReference,
		target ExecutionEnvelope,
	) (RolloverReceipt, error)
}
