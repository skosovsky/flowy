package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/skosovsky/flowy"
	"github.com/skosovsky/flowy/internal/nilvalue"
)

const (
	executionIDArgument       = "execution_id"
	executionRevisionArgument = "revision"
	executionOwnerArgument    = "owner"
	executionFenceArgument    = "fence"
)

const executionSchema = `
CREATE TABLE IF NOT EXISTS flowy_executions (
 execution_id TEXT PRIMARY KEY,
 revision BIGINT NOT NULL DEFAULT 0,
 fence BIGINT NOT NULL DEFAULT 0,
 lease_owner TEXT,
 lease_expiry TIMESTAMPTZ,
 fork_lineage JSONB,
 rollover_incoming JSONB,
 rollover_outgoing JSONB,
 payload_deleted BOOLEAN NOT NULL DEFAULT FALSE
);
ALTER TABLE flowy_executions ADD COLUMN IF NOT EXISTS fork_lineage JSONB;
ALTER TABLE flowy_executions ADD COLUMN IF NOT EXISTS rollover_incoming JSONB;
ALTER TABLE flowy_executions ADD COLUMN IF NOT EXISTS rollover_outgoing JSONB;
ALTER TABLE flowy_executions ADD COLUMN IF NOT EXISTS payload_deleted BOOLEAN NOT NULL DEFAULT FALSE;
CREATE TABLE IF NOT EXISTS flowy_execution_history (
 execution_id TEXT NOT NULL REFERENCES flowy_executions(execution_id),
 revision BIGINT NOT NULL,
 payload JSONB NOT NULL,
 PRIMARY KEY (execution_id, revision)
);`

// ExecutionSchemaSQL is the durable aggregate schema. The execution head must
// survive lease release and retention so a fencing incarnation is never reused.
func ExecutionSchemaSQL() string { return executionSchema + discoverySchema }

// ExecutionStore persists raw execution aggregates with atomic revision/fencing
// validation. Graph, codecs and domain state are supplied by the caller.
type ExecutionStore struct {
	db          DB
	waitProfile *flowy.WaitCapabilityProfile
}

// NewExecutionStore binds a durable store to a transaction-capable database.
func NewExecutionStore(db DB) (*ExecutionStore, error) {
	if nilvalue.IsNil(db) {
		return nil, flowy.ErrExecutionCapability
	}
	return &ExecutionStore{db: db, waitProfile: nil}, nil
}

func (s *ExecutionStore) LoadExecution(ctx context.Context, executionID string) (flowy.ExecutionEnvelope, error) {
	return loadAnchoredEnvelope(s.db.QueryRow(ctx, `
SELECT e.revision, h.payload, e.fork_lineage,e.rollover_incoming,e.rollover_outgoing,e.payload_deleted FROM flowy_executions e
LEFT JOIN flowy_execution_history h ON e.execution_id=h.execution_id AND e.revision=h.revision
WHERE e.execution_id=@execution_id`, pgx.NamedArgs{executionIDArgument: executionID}), executionID, 0)
}

func (s *ExecutionStore) LoadCheckpoint(
	ctx context.Context,
	executionID string,
	revision uint64,
) (flowy.ExecutionEnvelope, error) {
	if revision == 0 {
		return flowy.ExecutionEnvelope{}, flowy.ErrInvalidSnapshot
	}
	if revision > math.MaxInt64 {
		return flowy.ExecutionEnvelope{}, flowy.ErrExecutionCapability
	}
	return loadAnchoredEnvelope(s.db.QueryRow(ctx, `
SELECT @revision::bigint,h.payload,e.fork_lineage,e.rollover_incoming,e.rollover_outgoing,e.payload_deleted
FROM flowy_executions e LEFT JOIN flowy_execution_history h
ON h.execution_id=e.execution_id AND h.revision=@revision
WHERE e.execution_id=@execution_id AND e.revision>=@revision`,
		pgx.NamedArgs{executionIDArgument: executionID, executionRevisionArgument: revision}), executionID, revision)
}

func (s *ExecutionStore) CommitExecution(
	ctx context.Context,
	expectedRevision uint64,
	lease flowy.ExecutionLease,
	envelope flowy.ExecutionEnvelope,
) (flowy.ExecutionEnvelope, error) {
	if expectedRevision >= math.MaxInt64 {
		return flowy.ExecutionEnvelope{}, flowy.ErrExecutionCapability
	}
	if lease.Incarnation == 0 || lease.Incarnation > math.MaxInt64 || lease.Owner == "" ||
		envelope.ExecutionID != lease.ExecutionID {
		return flowy.ExecutionEnvelope{}, flowy.ErrLeaseLost
	}
	if validationErr := envelope.Descriptor.Validate(); validationErr != nil {
		return flowy.ExecutionEnvelope{}, validationErr
	}
	if envelope.Progress.ExecutionPointer == "" {
		return flowy.ExecutionEnvelope{}, flowy.ErrInvalidSnapshot
	}
	tx, err := s.begin(ctx)
	if err != nil {
		return flowy.ExecutionEnvelope{}, err
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	var revision uint64
	var owned bool
	err = tx.QueryRow(
		ctx,
		`
SELECT revision, COALESCE(lease_owner=@owner AND fence=@fence AND lease_expiry>clock_timestamp(),false)
FROM flowy_executions WHERE execution_id=@execution_id FOR UPDATE`,
		pgx.NamedArgs{
			executionIDArgument:    lease.ExecutionID,
			executionOwnerArgument: lease.Owner,
			executionFenceArgument: lease.Incarnation,
		},
	).Scan(&revision, &owned)
	if errors.Is(err, pgx.ErrNoRows) || (err == nil && !owned) {
		return flowy.ExecutionEnvelope{}, flowy.ErrLeaseLost
	}
	if err != nil {
		return flowy.ExecutionEnvelope{}, err
	}
	if revision != expectedRevision {
		return flowy.ExecutionEnvelope{}, flowy.ErrConcurrencyConflict
	}
	if lineageErr := validateForkPredecessor(ctx, tx, revision, envelope); lineageErr != nil {
		return flowy.ExecutionEnvelope{}, lineageErr
	}
	envelope.Revision = expectedRevision + 1
	envelope, err = flowy.SealExecutionEnvelope(envelope)
	if err != nil {
		return flowy.ExecutionEnvelope{}, err
	}
	payload, err := json.Marshal(envelope)
	if err != nil {
		return flowy.ExecutionEnvelope{}, err
	}
	var result flowy.ExecutionEnvelope
	if decodeErr := json.Unmarshal(payload, &result); decodeErr != nil {
		return flowy.ExecutionEnvelope{}, decodeErr
	}
	if _, writeErr := tx.Exec(
		ctx,
		`INSERT INTO flowy_execution_history(execution_id,revision,payload) VALUES (@execution_id,@revision,@payload::jsonb)`,
		pgx.NamedArgs{
			executionIDArgument:       envelope.ExecutionID,
			executionRevisionArgument: envelope.Revision,
			"payload":                 json.RawMessage(payload),
		},
	); writeErr != nil {
		return flowy.ExecutionEnvelope{}, writeErr
	}
	tag, err := tx.Exec(
		ctx,
		`UPDATE flowy_executions SET revision=@revision,
fork_lineage=CASE WHEN revision=0 THEN @payload::jsonb->'fork' ELSE fork_lineage END
WHERE execution_id=@execution_id AND lease_owner=@owner AND fence=@fence AND lease_expiry>clock_timestamp()`,
		pgx.NamedArgs{
			executionIDArgument:       envelope.ExecutionID,
			executionRevisionArgument: envelope.Revision,
			executionOwnerArgument:    lease.Owner,
			executionFenceArgument:    lease.Incarnation,
			"payload":                 json.RawMessage(payload),
		},
	)
	if err != nil {
		return flowy.ExecutionEnvelope{}, err
	}
	if tag.RowsAffected() != 1 {
		return flowy.ExecutionEnvelope{}, flowy.ErrLeaseLost
	}
	if projectionErr := replaceDiscoveryProjection(ctx, tx, envelope); projectionErr != nil {
		return flowy.ExecutionEnvelope{}, projectionErr
	}
	if commitErr := tx.Commit(ctx); commitErr != nil {
		return flowy.ExecutionEnvelope{}, commitErr
	}
	return result, nil
}

// Caller holds the execution head row lock and has checked fencing and OCC.
func validateForkPredecessor(ctx context.Context, tx pgx.Tx, revision uint64, next flowy.ExecutionEnvelope) error {
	if revision == 0 {
		if err := flowy.ValidateExecutionRolloverTransition(nil, next); err != nil {
			return err
		}
		return flowy.ValidateExecutionForkTransition(nil, next)
	}
	previous, err := loadAnchoredEnvelope(tx.QueryRow(ctx, `
SELECT h.revision,h.payload,e.fork_lineage,e.rollover_incoming,e.rollover_outgoing,e.payload_deleted FROM flowy_execution_history h
JOIN flowy_executions e ON e.execution_id=h.execution_id
WHERE h.execution_id=@execution_id AND h.revision=@revision`, pgx.NamedArgs{
		executionIDArgument: next.ExecutionID, executionRevisionArgument: revision,
	}), next.ExecutionID, revision)
	if errors.Is(err, flowy.ErrThreadNotFound) {
		return flowy.ErrExecutionCorrupt
	}
	if err != nil {
		return err
	}
	if err := flowy.ValidateExecutionRolloverTransition(&previous, next); err != nil {
		return err
	}
	return flowy.ValidateExecutionForkTransition(&previous, next)
}

func (s *ExecutionStore) AcquireExecution(
	ctx context.Context,
	executionID, owner string,
	ttl time.Duration,
) (flowy.ExecutionLease, error) {
	if executionID == "" || owner == "" || ttl <= 0 {
		return flowy.ExecutionLease{}, flowy.ErrInvalidResumeToken
	}
	tx, err := s.begin(ctx)
	if err != nil {
		return flowy.ExecutionLease{}, err
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if _, writeErr := tx.Exec(
		ctx,
		`INSERT INTO flowy_executions(execution_id) VALUES (@execution_id) ON CONFLICT DO NOTHING`,
		pgx.NamedArgs{executionIDArgument: executionID},
	); writeErr != nil {
		return flowy.ExecutionLease{}, writeErr
	}
	lease := flowy.ExecutionLease{ExecutionID: executionID, Owner: owner, Incarnation: 0, ExpiresAt: time.Time{}}
	err = tx.QueryRow(ctx, `
UPDATE flowy_executions SET fence=fence+1, lease_owner=@owner,
lease_expiry=clock_timestamp()+(@ttl::double precision * INTERVAL '1 second')
WHERE execution_id=@execution_id AND (lease_expiry IS NULL OR lease_expiry<=clock_timestamp())
RETURNING fence,lease_expiry`, pgx.NamedArgs{executionIDArgument: executionID, executionOwnerArgument: owner, "ttl": leaseTTLSeconds(ttl)}).Scan(&lease.Incarnation, &lease.ExpiresAt)
	if errors.Is(err, pgx.ErrNoRows) {
		var holder string
		// The refused UPDATE observed contention, but the owner may release before
		// this diagnostic read. A now-empty owner still means retry acquisition;
		// it is not authority to return a lease without another fenced UPDATE.
		if readErr := tx.QueryRow(ctx, `SELECT COALESCE(lease_owner,'') FROM flowy_executions WHERE execution_id=@execution_id`, pgx.NamedArgs{executionIDArgument: executionID}).
			Scan(&holder); readErr != nil {
			return flowy.ExecutionLease{}, readErr
		}
		if holder == owner {
			return flowy.ExecutionLease{}, flowy.ErrThreadLeaseBusy
		}
		return flowy.ExecutionLease{}, flowy.ErrLeaseHeld
	}
	if err != nil {
		var storageErr *pgconn.PgError
		if errors.As(err, &storageErr) && storageErr.Code == "22003" {
			return flowy.ExecutionLease{}, fmt.Errorf("%w: %w", flowy.ErrExecutionCapability, err)
		}
		return flowy.ExecutionLease{}, err
	}
	if commitErr := tx.Commit(ctx); commitErr != nil {
		return flowy.ExecutionLease{}, commitErr
	}
	return lease, nil
}

func (s *ExecutionStore) RenewExecution(
	ctx context.Context,
	lease flowy.ExecutionLease,
	ttl time.Duration,
) (flowy.ExecutionLease, error) {
	if ttl <= 0 || lease.Incarnation == 0 || lease.Incarnation > math.MaxInt64 || lease.ExecutionID == "" ||
		lease.Owner == "" {
		return flowy.ExecutionLease{}, flowy.ErrLeaseLost
	}
	err := s.db.QueryRow(ctx, `
UPDATE flowy_executions SET lease_expiry=clock_timestamp()+(@ttl::double precision * INTERVAL '1 second')
WHERE execution_id=@execution_id AND lease_owner=@owner AND fence=@fence AND lease_expiry>clock_timestamp()
RETURNING lease_expiry`, pgx.NamedArgs{executionIDArgument: lease.ExecutionID, executionOwnerArgument: lease.Owner, executionFenceArgument: lease.Incarnation, "ttl": leaseTTLSeconds(ttl)}).Scan(&lease.ExpiresAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return flowy.ExecutionLease{}, flowy.ErrLeaseLost
	}
	return lease, err
}

func (s *ExecutionStore) ReleaseExecution(ctx context.Context, lease flowy.ExecutionLease) error {
	if lease.Incarnation == 0 || lease.Incarnation > math.MaxInt64 || lease.ExecutionID == "" || lease.Owner == "" {
		return flowy.ErrLeaseLost
	}
	tag, err := s.db.Exec(
		ctx,
		`
UPDATE flowy_executions SET lease_owner=NULL,lease_expiry=NULL
WHERE execution_id=@execution_id AND fence=@fence AND (lease_owner=@owner OR lease_owner IS NULL)`,
		pgx.NamedArgs{
			executionIDArgument:    lease.ExecutionID,
			executionOwnerArgument: lease.Owner,
			executionFenceArgument: lease.Incarnation,
		},
	)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return flowy.ErrLeaseLost
	}
	return nil
}

func (s *ExecutionStore) begin(ctx context.Context) (pgx.Tx, error) {
	options := pgx.TxOptions{} //nolint:exhaustruct_v5 // driver defaults; explicit isolation
	options.IsoLevel = pgx.ReadCommitted
	return s.db.BeginTx(ctx, options)
}

func decodeRawEnvelope(payload []byte, id string, revision, expectedRevision uint64) (flowy.ExecutionEnvelope, error) {
	var envelope flowy.ExecutionEnvelope
	if revision == 0 && len(payload) == 0 {
		return flowy.ExecutionEnvelope{}, flowy.ErrThreadNotFound
	}
	if err := json.Unmarshal(payload, &envelope); err != nil {
		return flowy.ExecutionEnvelope{}, errors.Join(flowy.ErrExecutionCorrupt, err)
	}
	if expectedRevision != 0 && revision != expectedRevision {
		return flowy.ExecutionEnvelope{}, flowy.ErrExecutionCorrupt
	}
	if integrityErr := flowy.ValidateExecutionIntegrity(envelope, id, revision); integrityErr != nil {
		return flowy.ExecutionEnvelope{}, integrityErr
	}
	return envelope, nil
}

func loadAnchoredEnvelope(row pgx.Row, id string, expectedRevision uint64) (flowy.ExecutionEnvelope, error) {
	var payload, anchorPayload, incomingPayload, outgoingPayload []byte
	var deleted bool
	var revision uint64
	if err := row.Scan(&revision, &payload, &anchorPayload, &incomingPayload, &outgoingPayload, &deleted); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return flowy.ExecutionEnvelope{}, flowy.ErrThreadNotFound
		}
		return flowy.ExecutionEnvelope{}, err
	}
	if revision > 0 && len(payload) == 0 && (deleted || expectedRevision > 0) {
		return flowy.ExecutionEnvelope{}, flowy.ErrExecutionCheckpointUnavailable
	}
	envelope, err := decodeRawEnvelope(payload, id, revision, expectedRevision)
	if err != nil {
		return flowy.ExecutionEnvelope{}, err
	}
	if err = validateStoredForkAnchor(envelope, anchorPayload); err != nil {
		return flowy.ExecutionEnvelope{}, err
	}
	if err = validateStoredLifecycleAnchors(envelope, incomingPayload, outgoingPayload); err != nil {
		return flowy.ExecutionEnvelope{}, err
	}
	return envelope, nil
}

func validateStoredForkAnchor(envelope flowy.ExecutionEnvelope, payload []byte) error {
	var anchor *flowy.ForkLineage
	if len(payload) != 0 {
		if err := json.Unmarshal(payload, &anchor); err != nil {
			return errors.Join(flowy.ErrExecutionCorrupt, err)
		}
	}
	return flowy.ValidateExecutionForkAnchor(envelope, anchor)
}

var _ flowy.ExecutionStore = (*ExecutionStore)(nil)

// PostgreSQL timestamp/interval precision is microseconds.
func leaseTTLSeconds(ttl time.Duration) float64 {
	micros := ttl.Microseconds()
	if ttl%time.Microsecond != 0 {
		micros++
	}
	return float64(micros) / float64(time.Second/time.Microsecond)
}
