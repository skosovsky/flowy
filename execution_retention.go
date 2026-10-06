package flowy

import (
	"context"
	"slices"
)

// ExecutionRetentionPolicy is an explicit host archive/reference declaration.
// Protected revisions are sorted, unique exact addresses; opaque references
// cannot be inferred by core. KeepLast zero retains the live head unless deleted.
type ExecutionRetentionPolicy struct {
	Label              string   `json:"label"`
	KeepLast           int      `json:"keep_last"`
	DeletePayload      bool     `json:"delete_payload"`
	ProtectedRevisions []uint64 `json:"protected_revisions,omitempty"`
}

type ExecutionRetentionRequest struct {
	ExecutionID string
	Revision    uint64
	Policy      ExecutionRetentionPolicy
}

type ExecutionRetentionReceipt struct {
	ExecutionID      string
	Revision         uint64
	DeletedRevisions int
	DeletedBytes     int64
}

// ExecutionRetentionStore performs dependency-safe atomic maintenance. It
// rejects active leases and unsafe dependencies, never resets fences or reuses
// IDs. Permanent anchors remain independently checkable after payload removal.
type ExecutionRetentionStore interface {
	RetainExecution(ctx context.Context, request ExecutionRetentionRequest) (ExecutionRetentionReceipt, error)
}

func (request ExecutionRetentionRequest) Validate() error {
	p := request.Policy
	if request.ExecutionID == "" || !validRuntimeText(request.ExecutionID, p.Label) || request.Revision == 0 ||
		p.Label == "" ||
		p.KeepLast < 0 ||
		!slices.IsSorted(p.ProtectedRevisions) {
		return ErrExecutionLifecycleUnsafe
	}
	if p.DeletePayload && (p.KeepLast != 0 || len(p.ProtectedRevisions) != 0) {
		return ErrExecutionLifecycleUnsafe
	}
	var prior uint64
	for _, revision := range p.ProtectedRevisions {
		if revision == 0 || revision > request.Revision || revision == prior {
			return ErrExecutionLifecycleUnsafe
		}
		prior = revision
	}
	return nil
}

// ExecutionRevisionRetained applies the exact retention selection. Call only
// after validating the request and every protected revision's availability.
func ExecutionRevisionRetained(request ExecutionRetentionRequest, revision uint64) bool {
	if request.Policy.DeletePayload {
		return false
	}
	if revision == request.Revision || slices.Contains(request.Policy.ProtectedRevisions, revision) {
		return true
	}
	// Subtract safely: revision <= head, KeepLast is a validated nonnegative int.
	if request.Policy.KeepLast < 0 {
		return false
	}
	return revision <= request.Revision && request.Revision-revision < uint64(request.Policy.KeepLast)
}
