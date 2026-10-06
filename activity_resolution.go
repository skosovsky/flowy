package flowy

import (
	"bytes"
	"context"
	"encoding/json"
	"time"
)

// ActivityResolutionAction is an explicit operator decision, never a dispatch.
type ActivityResolutionAction string

const (
	ActivityResolveComplete ActivityResolutionAction = "complete"
	ActivityResolveFail     ActivityResolutionAction = "fail"
	ActivityResolveRetry    ActivityResolutionAction = "retry"
)

// ActivityResolution addresses one current activation and its exact persisted
// contract. Evidence and authorization belong to the host; runtime records them.
type ActivityResolution struct {
	Identity          string
	InputDigest       string
	Implementation    string
	DecisionID        string
	Action            ActivityResolutionAction
	Reason            string
	Evidence          string
	Outcome           []byte
	SafeRetryContract string
}

// ActivityResolutionRecord retains the decision without altering old attempts.
type ActivityResolutionRecord struct {
	DecisionID        string                   `json:"decision_id"`
	Attempt           int                      `json:"attempt"`
	Action            ActivityResolutionAction `json:"action"`
	PriorState        ActivityState            `json:"prior_state"`
	SourceRevision    uint64                   `json:"source_revision"`
	Incarnation       uint64                   `json:"incarnation"`
	Reason            string                   `json:"reason"`
	Evidence          string                   `json:"evidence"`
	SafeRetryContract string                   `json:"safe_retry_contract,omitempty"`
	At                time.Time                `json:"at"`
}

// ResolveActivity commits a manual decision without node, codec, reconciliation
// or dispatcher calls. It rejects stale revisions and never migrates implicitly.
func (r *DurableRunner[T, E]) ResolveActivity(
	ctx context.Context,
	token ResumeToken,
	resolution ActivityResolution,
) (ResumeToken, error) {
	if resolution.Identity == "" || resolution.InputDigest == "" || resolution.Implementation == "" ||
		!validRuntimeText(resolution.Identity, resolution.InputDigest, resolution.Implementation,
			resolution.DecisionID, resolution.Evidence, resolution.SafeRetryContract) ||
		resolution.DecisionID == "" ||
		resolution.Reason == "" ||
		resolution.Evidence == "" {
		return ResumeToken{}, ErrActivityConflict
	}
	resolution.Outcome = bytes.Clone(resolution.Outcome)
	session, err := r.acquireSession(ctx, token.ThreadID)
	if err != nil {
		return ResumeToken{}, err
	}
	defer session.finish()
	source, err := r.store.LoadExecution(session.ctx, token.ThreadID)
	if err != nil {
		return ResumeToken{}, err
	}
	if integrityErr := ValidateExecutionIntegrity(source, token.ThreadID, source.Revision); integrityErr != nil {
		return ResumeToken{}, integrityErr
	}
	if token.SnapshotRevision == 0 || token.SnapshotRevision != source.Revision {
		return ResumeToken{}, ErrConcurrencyConflict
	}
	if collectionsErr := validateExecutionCollections(source); collectionsErr != nil {
		return ResumeToken{}, collectionsErr
	}
	if descriptorErr := source.Descriptor.Check(r.descriptor); descriptorErr != nil {
		return ResumeToken{}, descriptorErr
	}
	if source.Terminal != nil {
		return ResumeToken{}, ErrActivityConflict
	}
	if pointerErr := r.validatePointer(source.Progress.ExecutionPointer); pointerErr != nil {
		return ResumeToken{}, pointerErr
	}
	if source.Import != nil && (source.Import.ImporterID == "" || source.Import.Source.Validate() != nil) {
		return ResumeToken{}, ErrExecutionImportInvalid
	}
	journal, record, recordErr := resolutionActivityRecord(source, resolution)
	if recordErr != nil {
		return ResumeToken{}, recordErr
	}
	priorState := record.State
	now := r.options.Clock.Now().UTC()
	if record.State == ActivityRunning {
		record.Attempts[len(record.Attempts)-1].State = ActivityUnknown
		record.Attempts[len(record.Attempts)-1].Classification = ActivityAmbiguous
	}
	if resolutionErr := applyActivityResolution(&record, resolution, now); resolutionErr != nil {
		return ResumeToken{}, resolutionErr
	}
	record.Resolutions = append(record.Resolutions, ActivityResolutionRecord{
		DecisionID:        resolution.DecisionID,
		Attempt:           len(record.Attempts),
		Action:            resolution.Action,
		PriorState:        priorState,
		SourceRevision:    source.Revision,
		Incarnation:       session.lease.Incarnation,
		Reason:            resolution.Reason,
		Evidence:          resolution.Evidence,
		SafeRetryContract: resolution.SafeRetryContract,
		At:                now,
	})
	journal[record.Identity] = record
	target := cloneExecutionEnvelope(source)
	target.JournalPayload, err = json.Marshal(journal)
	if err != nil {
		return ResumeToken{}, err
	}
	if session.ctx.Err() != nil {
		return ResumeToken{}, context.Cause(session.ctx)
	}
	committed, err := r.store.CommitExecution(session.ctx, source.Revision, session.lease, target)
	if err != nil {
		return ResumeToken{}, err
	}
	return ResumeToken{ThreadID: token.ThreadID, SnapshotRevision: committed.Revision}, nil
}

func resolutionActivityRecord(
	source ExecutionEnvelope,
	resolution ActivityResolution,
) (map[string]ActivityRecord, ActivityRecord, error) {
	journal, journalErr := executionActivityJournal(source)
	if journalErr != nil {
		return nil, ActivityRecord{}, journalErr
	}
	record, exists := journal[resolution.Identity]
	if !exists || record.Identity != boundActivityIdentity(source, record.Key) ||
		record.InputDigest != resolution.InputDigest ||
		record.Implementation != resolution.Implementation ||
		len(record.Attempts) == 0 {
		return nil, ActivityRecord{}, ErrActivityConflict
	}
	for _, prior := range record.Resolutions {
		if prior.DecisionID == resolution.DecisionID {
			return nil, ActivityRecord{}, ErrActivityConflict
		}
	}
	if record.State != ActivityUnknown && record.State != ActivityRunning {
		return nil, ActivityRecord{}, ErrActivityConflict
	}
	return journal, record, nil
}

func applyActivityResolution(record *ActivityRecord, resolution ActivityResolution, now time.Time) error {
	record.Classification, record.NextAttemptAt = "", time.Time{}
	switch resolution.Action {
	case ActivityResolveComplete:
		if resolution.SafeRetryContract != "" {
			return ErrActivityConflict
		}
		record.State, record.Origin, record.Outcome = ActivityCompleted, ActivityManual, bytes.Clone(resolution.Outcome)
	case ActivityResolveFail:
		if len(resolution.Outcome) != 0 || resolution.SafeRetryContract != "" {
			return ErrActivityConflict
		}
		record.State, record.Origin, record.Classification = ActivityFailed, ActivityManual, ActivityNonRetryable
	case ActivityResolveRetry:
		if len(resolution.Outcome) != 0 || validateActivityRetry(record.Retry) != nil ||
			record.Retry.SafeRetryContract == "" ||
			resolution.SafeRetryContract != record.Retry.SafeRetryContract ||
			len(record.Attempts) >= record.Retry.MaxAttempts {
			return ErrActivityConflict
		}
		record.State, record.Origin, record.Classification = ActivityPrepared, ActivityManual, ActivityRetryable
		record.NextAttemptAt = now.Add(record.Retry.Delay)
	default:
		return ErrActivityConflict
	}
	return nil
}
