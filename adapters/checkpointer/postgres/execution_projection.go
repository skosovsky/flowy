package postgres

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/skosovsky/flowy"
)

var ErrDiscoveryRebuildRequired = errors.New("flowy postgres: discovery projection rebuild required")
var ErrDiscoveryCursor = errors.New("flowy postgres: invalid discovery cursor")
var ErrDiscoveryStaleRevision = errors.New("flowy postgres: stale candidate revision")
var ErrDiscoveryProfileMismatch = errors.New("flowy postgres: candidate profile mismatch")
var ErrDiscoveryProjectionMismatch = errors.New("flowy postgres: candidate identity or deadline mismatch")

const discoverySchema = `
ALTER TABLE flowy_executions ADD COLUMN IF NOT EXISTS discovery_projected BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE flowy_executions ADD COLUMN IF NOT EXISTS discovery_error TEXT;
CREATE INDEX IF NOT EXISTS flowy_discovery_unready ON flowy_executions(execution_id)
WHERE revision>0 AND NOT payload_deleted AND NOT discovery_projected;
CREATE TABLE IF NOT EXISTS flowy_due_candidates (
 execution_id TEXT NOT NULL REFERENCES flowy_executions(execution_id),
 revision BIGINT NOT NULL,
 kind TEXT NOT NULL CHECK(kind IN ('wait','retry')),
 work_identity TEXT NOT NULL,
 profile_digest TEXT NOT NULL,
 deadline TIMESTAMPTZ NOT NULL,
 PRIMARY KEY(execution_id,kind,work_identity)
);
CREATE INDEX IF NOT EXISTS flowy_due_order ON flowy_due_candidates
(profile_digest,kind,deadline,execution_id,work_identity) INCLUDE(revision);`

// DiscoveryCursor binds a due-time keyset to one clock cutoff, profile and kind.
// A polling cycle starts with zero and restarts with zero after More is false.
type DiscoveryCursor struct {
	UpperDeadline    time.Time
	AfterDeadline    time.Time
	AfterExecutionID string
	AfterIdentity    string
	ProfileDigest    string
	Kind             string
}

// DiscoveryDiagnostic identifies unusable derived work; it is not dispatchable.
// Database errors remain operation errors instead of diagnostics.
type DiscoveryDiagnostic struct {
	ExecutionID string
	Revision    uint64
	Identity    string
	Kind        string
	Reason      string
}

type dueCandidate struct {
	ExecutionID   string
	Revision      uint64
	Identity      string
	ProfileDigest string
	Kind          string
	Deadline      time.Time
}

func discoveryProfileDigest(profile flowy.WaitCapabilityProfile) string {
	payload, _ := json.Marshal(profile)
	digest := sha256.Sum256(payload)
	return hex.EncodeToString(digest[:])
}

func executionCandidates(envelope flowy.ExecutionEnvelope) ([]dueCandidate, error) {
	candidates := make([]dueCandidate, 0)
	if envelope.Terminal != nil || envelope.RuntimeProfile == nil {
		return candidates, nil
	}
	waits, err := flowy.InspectExecutionWaits(envelope)
	if err != nil {
		return nil, err
	}
	retries, err := flowy.InspectPendingActivityRetries(envelope)
	if err != nil {
		return nil, err
	}
	profile := discoveryProfileDigest(*envelope.RuntimeProfile)
	for _, wait := range waits {
		if wait.State == flowy.WaitArmed {
			candidates = append(
				candidates,
				dueCandidate{
					ExecutionID:   envelope.ExecutionID,
					Revision:      envelope.Revision,
					Identity:      wait.Generation,
					ProfileDigest: profile,
					Kind:          "wait",
					Deadline:      discoveryDeadline(wait.Spec.Deadline),
				},
			)
		}
	}
	for _, retry := range retries {
		candidates = append(
			candidates,
			dueCandidate{
				ExecutionID:   envelope.ExecutionID,
				Revision:      envelope.Revision,
				Identity:      retry.Identity,
				ProfileDigest: profile,
				Kind:          "retry",
				Deadline:      discoveryDeadline(retry.Deadline),
			},
		)
	}
	return candidates, nil
}

// replaceDiscoveryProjection runs inside the same locked publication transaction.
func replaceDiscoveryProjection(ctx context.Context, tx pgx.Tx, envelope flowy.ExecutionEnvelope) error {
	candidates, err := executionCandidates(envelope)
	if err != nil {
		return err
	}
	if _, err = tx.Exec(
		ctx,
		`DELETE FROM flowy_due_candidates WHERE execution_id=$1`,
		envelope.ExecutionID,
	); err != nil {
		return err
	}
	for _, candidate := range candidates {
		if _, err = tx.Exec(
			ctx,
			`INSERT INTO flowy_due_candidates(execution_id,revision,kind,work_identity,profile_digest,deadline) VALUES($1,$2,$3,$4,$5,$6)`,
			candidate.ExecutionID,
			candidate.Revision,
			candidate.Kind,
			candidate.Identity,
			candidate.ProfileDigest,
			candidate.Deadline,
		); err != nil {
			return err
		}
	}
	_, err = tx.Exec(
		ctx,
		`UPDATE flowy_executions SET discovery_projected=true,discovery_error=NULL WHERE execution_id=$1`,
		envelope.ExecutionID,
	)
	return err
}

func removeDiscoveryProjection(ctx context.Context, tx pgx.Tx, id string, diagnostic string) error {
	if _, err := tx.Exec(ctx, `DELETE FROM flowy_due_candidates WHERE execution_id=$1`, id); err != nil {
		return err
	}
	var reason any
	if diagnostic != "" {
		reason = diagnostic
	}
	_, err := tx.Exec(
		ctx,
		`UPDATE flowy_executions SET discovery_projected=true,discovery_error=$2 WHERE execution_id=$1`,
		id,
		reason,
	)
	return err
}

// PostgreSQL timestamps have microsecond precision. Round derived deadlines up;
// authoritative revalidation retains the original deadline and prevents early delivery.
func discoveryDeadline(deadline time.Time) time.Time {
	rounded := deadline.Truncate(time.Microsecond)
	if rounded.Before(deadline) {
		rounded = rounded.Add(time.Microsecond)
	}
	return rounded
}
