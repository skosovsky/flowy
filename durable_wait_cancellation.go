package flowy

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"time"
)

const durableWaitCanceledReason = "durable_wait_canceled"

// WaitCancellation supplies the host's cancellation authority/evidence. Core
// validates its address/provenance, not the truth of business permission.
type WaitCancellation struct {
	Generation string
	ID         string
	Reason     string
	Evidence   string
}

type WaitCancellationRecord struct {
	ID             string    `json:"id"`
	Reason         string    `json:"reason"`
	Evidence       string    `json:"evidence"`
	SourceRevision uint64    `json:"source_revision"`
	Incarnation    uint64    `json:"incarnation"`
	At             time.Time `json:"at"`
}

// CancelWait commits cancellation and its terminal together at the armed safe
// boundary. It invokes no host codec/node/remote cancellation and never resumes
// execution. Identical addressed cancellation is replayed without another write.
func (r *DurableRunner[T, E]) CancelWait(ctx context.Context, token ResumeToken,
	request WaitCancellation,
) (result ResumeToken, retErr error) { //nolint:nonamedreturns // Preserve committed result and cleanup error.
	if token.ThreadID == "" || token.SnapshotRevision == 0 || request.Generation == "" || request.ID == "" ||
		!validRuntimeText(token.ThreadID, request.Generation, request.ID, request.Reason, request.Evidence) ||
		request.Reason == "" || request.Evidence == "" {
		return ResumeToken{}, ErrWaitInvalid
	}
	if _, _, err := r.waitDeliverySource(ctx, token.ThreadID, request.Generation); err != nil {
		return ResumeToken{}, err
	}
	session, err := r.acquireSession(ctx, token.ThreadID)
	if err != nil {
		return ResumeToken{}, err
	}
	defer func() { retErr = errors.Join(retErr, session.finish()) }()
	source, waits, err := r.waitDeliverySource(session.ctx, token.ThreadID, request.Generation)
	if err != nil {
		return ResumeToken{}, err
	}
	session.ctx = restoreExecutionTelemetry(session.ctx, source)
	event := waitObservation(source, LifecycleWaitCancel, request.Generation, request.ID)
	observeLifecycle(session.ctx, event)
	event.Stage = LifecycleFailed
	defer func() { observeLifecycle(session.ctx, event) }()
	identity, record, err := findWaitGeneration(waits, request.Generation)
	if err != nil {
		return ResumeToken{}, err
	}
	if cancelErr := validateWaitCancellationRequest(source, record, token, request); cancelErr != nil {
		return ResumeToken{}, cancelErr
	}
	event.Code = "wait_canceled"
	if record.State == WaitCanceled {
		event.Stage, event.Revision = LifecycleReplayed, source.Revision
		return ResumeToken{ThreadID: token.ThreadID, SnapshotRevision: source.Revision}, nil
	}
	now := r.options.Clock.Now().UTC()
	if now.IsZero() {
		return ResumeToken{}, ErrWaitInvalid
	}
	record.State = WaitCanceled
	record.Cancellation = &WaitCancellationRecord{ID: request.ID, Reason: request.Reason, Evidence: request.Evidence,
		SourceRevision: source.Revision, Incarnation: session.lease.Incarnation, At: now}
	waits[identity] = record
	target := cloneExecutionEnvelope(source)
	target.WaitsPayload, err = json.Marshal(waits)
	if err != nil {
		return ResumeToken{}, err
	}
	target.Terminal = &ExecutionTerminal{Status: RunStatusFailed, Reason: durableWaitCanceledReason,
		Failure: &ExecutionFailure{Message: durableWaitCanceledReason + ": " + request.Reason}}
	markSegmentFailed(&target.RunMeta)
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

func validateWaitCancellationRequest(source ExecutionEnvelope, record DurableWaitRecord,
	token ResumeToken, request WaitCancellation,
) error {
	if token.SnapshotRevision < record.ArmRevision || token.SnapshotRevision > source.Revision {
		return ErrWaitStale
	}
	if record.State == WaitCanceled {
		prior := record.Cancellation
		if prior != nil && prior.ID != request.ID {
			return ErrWaitCanceled
		}
		if prior == nil || prior.Reason != request.Reason || prior.Evidence != request.Evidence {
			return ErrWaitConflict
		}
		return nil
	}
	if record.State == WaitExpired {
		return ErrWaitExpired
	}
	if record.State != WaitArmed || source.Terminal != nil {
		return ErrWaitConflict
	}
	if token.SnapshotRevision != source.Revision {
		return ErrWaitStale
	}
	if source.Revision == math.MaxUint64 {
		return ErrWaitInvalid
	}
	return nil
}

func validWaitCancellation(record DurableWaitRecord, revision uint64) bool {
	if record.State != WaitCanceled {
		return record.Cancellation == nil
	}
	cancel := record.Cancellation
	return cancel != nil && cancel.ID != "" && cancel.Reason != "" && cancel.Evidence != "" &&
		cancel.SourceRevision >= record.ArmRevision && cancel.SourceRevision < revision &&
		cancel.Incarnation != 0 && !cancel.At.IsZero() && validRuntimeText(cancel.ID, cancel.Reason, cancel.Evidence)
}

func validCanceledWaitTerminal(envelope ExecutionEnvelope, record DurableWaitRecord) bool {
	if record.State != WaitCanceled {
		return true
	}
	terminal := envelope.Terminal
	return record.Cancellation != nil && terminal != nil && terminal.Status == RunStatusFailed &&
		terminal.Failure != nil && terminal.Reason == durableWaitCanceledReason &&
		terminal.Failure.Message == durableWaitCanceledReason+": "+record.Cancellation.Reason &&
		envelope.Activation == record.Activation && envelope.Progress.ExecutionPointer == record.Node
}
