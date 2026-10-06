package postgres

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"

	"github.com/skosovsky/flowy"
)

// DiscoveryRebuildPage advances an explicit bounded repair pass over heads.
// Diagnostics identify quarantined records; their authoritative payload is unchanged.
type DiscoveryRebuildPage struct {
	AfterExecutionID string
	More             bool
	Rebuilt          int
	Diagnostics      []DiscoveryDiagnostic
}

// RebuildDiscovery reconstructs derived work under the same head lock as writers.
// Run complete passes after schema transition; ordinary polling never scans heads.
func (s *ExecutionStore) RebuildDiscovery(
	ctx context.Context,
	afterID string,
	limit int,
) (DiscoveryRebuildPage, error) {
	if limit <= 0 || limit > maxWaitScanHeads {
		return DiscoveryRebuildPage{}, flowy.ErrWaitInvalid
	}
	ids, err := s.discoveryRebuildIDs(ctx, afterID, limit)
	if err != nil {
		return DiscoveryRebuildPage{}, err
	}
	page := DiscoveryRebuildPage{AfterExecutionID: afterID, More: len(ids) > limit,
		Diagnostics: make([]DiscoveryDiagnostic, 0), Rebuilt: 0}
	if page.More {
		ids = ids[:limit]
	}
	for _, id := range ids {
		diagnostic, rebuildErr := s.rebuildDiscoveryHead(ctx, id)
		if rebuildErr != nil {
			return DiscoveryRebuildPage{}, rebuildErr
		}
		if diagnostic != nil {
			page.Diagnostics = append(page.Diagnostics, *diagnostic)
		}
		page.Rebuilt++
		page.AfterExecutionID = id
	}
	return page, nil
}

func (s *ExecutionStore) discoveryRebuildIDs(ctx context.Context, afterID string, limit int) ([]string, error) {
	rows, err := s.db.Query(ctx, `SELECT execution_id FROM flowy_executions
 WHERE execution_id>$1 AND revision>0 ORDER BY execution_id LIMIT $2`, afterID, limit+1)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	ids := make([]string, 0, limit+1)
	for rows.Next() {
		var id string
		if err = rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

func (s *ExecutionStore) rebuildDiscoveryHead(ctx context.Context, id string) (*DiscoveryDiagnostic, error) {
	tx, err := s.begin(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	var revision uint64
	var deleted bool
	err = tx.QueryRow(ctx, `SELECT revision,payload_deleted FROM flowy_executions
 WHERE execution_id=$1 FOR UPDATE`, id).Scan(&revision, &deleted)
	if err != nil {
		return nil, err
	}
	var diagnostic *DiscoveryDiagnostic
	if deleted {
		err = removeDiscoveryProjection(ctx, tx, id, "")
	} else {
		diagnostic, err = rebuildLoadedHead(ctx, tx, id, revision)
	}
	if err != nil {
		return nil, err
	}
	if err = tx.Commit(ctx); err != nil {
		return nil, err
	}
	return diagnostic, nil
}

func rebuildLoadedHead(ctx context.Context, tx pgx.Tx, id string, revision uint64) (*DiscoveryDiagnostic, error) {
	envelope, err := loadAnchoredEnvelope(tx.QueryRow(ctx, `
 SELECT e.revision,h.payload,e.fork_lineage,e.rollover_incoming,e.rollover_outgoing,e.payload_deleted
 FROM flowy_executions e LEFT JOIN flowy_execution_history h
 ON h.execution_id=e.execution_id AND h.revision=e.revision WHERE e.execution_id=$1`, id), id, 0)
	if err == nil {
		_, err = executionCandidates(envelope)
		if err != nil {
			err = errors.Join(flowy.ErrExecutionCorrupt, err)
		}
	}
	if err == nil {
		return nil, replaceDiscoveryProjection(ctx, tx, envelope)
	}
	if !discoveryRecordError(err) {
		return nil, err
	}
	diagnostic := &DiscoveryDiagnostic{ExecutionID: id, Revision: revision, Reason: err.Error(), Identity: "", Kind: ""}
	return diagnostic, removeDiscoveryProjection(ctx, tx, id, diagnostic.Reason)
}
