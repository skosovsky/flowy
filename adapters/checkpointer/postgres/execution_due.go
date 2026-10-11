package postgres

import (
	"context"
	"errors"
	"time"

	"github.com/skosovsky/flowy"
)

const dueCandidatesFirstQuery = `
SELECT execution_id,revision,work_identity,profile_digest,kind,deadline
FROM flowy_due_candidates
WHERE profile_digest=$1 AND kind=$2 AND deadline<=$3
ORDER BY deadline,execution_id,work_identity LIMIT $4`

const dueCandidatesNextQuery = `
SELECT execution_id,revision,work_identity,profile_digest,kind,deadline
FROM flowy_due_candidates
WHERE profile_digest=$1 AND kind=$2 AND deadline<=$3
AND (deadline,execution_id,work_identity)>($4,$5,$6)
ORDER BY deadline,execution_id,work_identity LIMIT $7`

func dueSelection(cursor DiscoveryCursor, limit int) (string, []any) {
	args := []any{cursor.ProfileDigest, cursor.Kind, cursor.UpperDeadline}
	if cursor.AfterDeadline.IsZero() {
		return dueCandidatesFirstQuery, append(args, limit+1)
	}
	return dueCandidatesNextQuery, append(
		args,
		cursor.AfterDeadline,
		cursor.AfterExecutionID,
		cursor.AfterIdentity,
		limit+1,
	)
}

func (s *ExecutionStore) discoveryPosition(
	now time.Time,
	cursor DiscoveryCursor,
	limit int,
	kind string,
) (DiscoveryCursor, error) {
	if s.waitProfile == nil {
		return DiscoveryCursor{}, flowy.ErrExecutionCapability
	}
	if now.IsZero() || limit <= 0 || limit > maxWaitScanHeads {
		return DiscoveryCursor{}, flowy.ErrWaitInvalid
	}
	digest := discoveryProfileDigest(*s.waitProfile)
	var empty DiscoveryCursor
	if cursor == empty {
		return DiscoveryCursor{
			UpperDeadline:    now,
			ProfileDigest:    digest,
			Kind:             kind,
			AfterDeadline:    time.Time{},
			AfterExecutionID: "",
			AfterIdentity:    "",
		}, nil
	}
	if !cursor.UpperDeadline.Equal(now) || cursor.ProfileDigest != digest || cursor.Kind != kind ||
		cursor.AfterDeadline.IsZero() || cursor.AfterExecutionID == "" || cursor.AfterIdentity == "" || cursor.AfterDeadline.After(now) {
		return DiscoveryCursor{}, ErrDiscoveryCursor
	}
	return cursor, nil
}

func (s *ExecutionStore) readDueCandidates(
	ctx context.Context,
	cursor DiscoveryCursor,
	limit int,
) ([]dueCandidate, error) {
	var unready bool
	if err := s.db.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM flowy_executions
 WHERE revision>0 AND NOT payload_deleted AND NOT discovery_projected)`).Scan(&unready); err != nil {
		return nil, err
	}
	if unready {
		return nil, ErrDiscoveryRebuildRequired
	}
	query, args := dueSelection(cursor, limit)
	rows, err := s.db.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	candidates := make([]dueCandidate, 0, limit+1)
	for rows.Next() {
		var candidate dueCandidate
		var revision int64
		if err = rows.Scan(&candidate.ExecutionID, &revision, &candidate.Identity,
			&candidate.ProfileDigest, &candidate.Kind, &candidate.Deadline); err != nil {
			return nil, err
		}
		if revision > 0 {
			candidate.Revision = uint64(revision)
		}
		candidates = append(candidates, candidate)
	}
	return candidates, rows.Err()
}

func discoveryRecordError(err error) bool {
	return errors.Is(err, flowy.ErrExecutionCorrupt) || errors.Is(err, flowy.ErrInvalidSnapshot) ||
		errors.Is(err, flowy.ErrExecutionCheckpointUnavailable) || errors.Is(err, flowy.ErrThreadNotFound) ||
		errors.Is(err, flowy.ErrExecutionImportInvalid) || errors.Is(err, ErrDiscoveryStaleRevision) ||
		errors.Is(err, ErrDiscoveryProfileMismatch) || errors.Is(err, ErrDiscoveryProjectionMismatch)
}

func (s *ExecutionStore) validateDueCandidate(
	ctx context.Context,
	candidate dueCandidate,
) (flowy.ExecutionEnvelope, error) {
	if candidate.Revision == 0 {
		return flowy.ExecutionEnvelope{}, ErrDiscoveryProjectionMismatch
	}
	envelope, err := s.LoadExecution(ctx, candidate.ExecutionID)
	if err != nil {
		return flowy.ExecutionEnvelope{}, err
	}
	if envelope.Revision != candidate.Revision {
		return flowy.ExecutionEnvelope{}, ErrDiscoveryStaleRevision
	}
	if envelope.RuntimeProfile == nil || discoveryProfileDigest(*envelope.RuntimeProfile) != candidate.ProfileDigest {
		return flowy.ExecutionEnvelope{}, ErrDiscoveryProfileMismatch
	}

	expected, err := executionCandidates(envelope)
	if err != nil {
		return flowy.ExecutionEnvelope{}, errors.Join(flowy.ErrExecutionCorrupt, err)
	}
	for _, current := range expected {
		if current.Identity == candidate.Identity && current.Kind == candidate.Kind &&
			current.Deadline.Equal(candidate.Deadline) {
			return envelope, nil
		}
	}
	return flowy.ExecutionEnvelope{}, ErrDiscoveryProjectionMismatch
}

func (s *ExecutionStore) scanDueCandidates(ctx context.Context, now time.Time, cursor DiscoveryCursor,
	limit int, kind string, visit func(flowy.ExecutionEnvelope, dueCandidate) error,
) (DiscoveryCursor, []DiscoveryDiagnostic, bool, error) {
	position, err := s.discoveryPosition(now, cursor, limit, kind)
	if err != nil {
		return DiscoveryCursor{}, nil, false, err
	}
	candidates, err := s.readDueCandidates(ctx, position, limit)
	if err != nil {
		return DiscoveryCursor{}, nil, false, err
	}
	more := len(candidates) > limit
	if more {
		candidates = candidates[:limit]
	}
	diagnostics := make([]DiscoveryDiagnostic, 0)
	for _, candidate := range candidates {
		envelope, candidateErr := s.validateDueCandidate(ctx, candidate)
		if candidateErr != nil && !discoveryRecordError(candidateErr) {
			return DiscoveryCursor{}, nil, false, candidateErr
		}
		if candidateErr == nil {
			candidateErr = visit(envelope, candidate)
		}
		if candidateErr != nil {
			diagnostics = append(diagnostics, DiscoveryDiagnostic{
				ExecutionID: candidate.ExecutionID,
				Revision:    candidate.Revision,
				Identity:    candidate.Identity,
				Kind:        candidate.Kind,
				Reason:      candidateErr.Error(),
			})
		}
		position.AfterDeadline, position.AfterExecutionID, position.AfterIdentity = candidate.Deadline, candidate.ExecutionID, candidate.Identity
	}
	return position, diagnostics, more, nil
}
