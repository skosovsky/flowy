package postgres

import (
	"context"
	"encoding/json"

	"github.com/jackc/pgx/v5"

	"github.com/skosovsky/flowy"
)

type executionHeadPage struct {
	AfterExecutionID string
	More             bool
}

func (s *ExecutionStore) scanExecutionHeads(ctx context.Context, afterID string, limit int,
	visit func(flowy.ExecutionEnvelope) error,
) (executionHeadPage, error) {
	rows, err := s.db.Query(ctx, `
SELECT e.execution_id,e.revision,h.payload,e.fork_lineage,e.rollover_incoming,e.rollover_outgoing FROM flowy_executions e
LEFT JOIN flowy_execution_history h ON e.execution_id=h.execution_id AND e.revision=h.revision
WHERE e.execution_id>@after_id AND e.revision>0 AND NOT e.payload_deleted ORDER BY e.execution_id LIMIT @scan_limit`,
		pgx.NamedArgs{"after_id": afterID, "scan_limit": limit + 1})
	if err != nil {
		return executionHeadPage{}, err
	}
	defer rows.Close()
	page := executionHeadPage{AfterExecutionID: afterID, More: false}
	for count := 0; rows.Next(); count++ {
		if count == limit {
			page.More = true
			break
		}
		var id string
		var revision uint64
		var payload, anchor, incoming, outgoing []byte
		if scanErr := rows.Scan(&id, &revision, &payload, &anchor, &incoming, &outgoing); scanErr != nil {
			return executionHeadPage{}, scanErr
		}
		var envelope flowy.ExecutionEnvelope
		if decodeErr := json.Unmarshal(payload, &envelope); decodeErr != nil {
			return executionHeadPage{}, flowy.ErrExecutionCorrupt
		}
		if integrityErr := flowy.ValidateExecutionIntegrity(envelope, id, revision); integrityErr != nil {
			return executionHeadPage{}, integrityErr
		}
		if anchorErr := validateStoredForkAnchor(envelope, anchor); anchorErr != nil {
			return executionHeadPage{}, anchorErr
		}
		if anchorErr := validateStoredLifecycleAnchors(envelope, incoming, outgoing); anchorErr != nil {
			return executionHeadPage{}, anchorErr
		}
		if visitErr := visit(envelope); visitErr != nil {
			return executionHeadPage{}, visitErr
		}
		page.AfterExecutionID = id
	}
	if rowsErr := rows.Err(); rowsErr != nil {
		return executionHeadPage{}, rowsErr
	}
	return page, nil
}
