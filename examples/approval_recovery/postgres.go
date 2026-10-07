package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// postgresEvidence is bounded example host persistence, not a production store.
// Revocation and effect commits serialize on the intention row, closing the final authorization/write race.
type postgresEvidence struct{ pool *pgxpool.Pool }

const evidenceSchema = `CREATE TABLE IF NOT EXISTS approval_host_intents (
 operation_id text PRIMARY KEY, mapping jsonb, revoked boolean NOT NULL DEFAULT false,
 receipt jsonb, calls integer NOT NULL DEFAULT 0, writes integer NOT NULL DEFAULT 0,
 current_attempt integer NOT NULL DEFAULT 1, captured_before boolean NOT NULL DEFAULT false, captured_after boolean NOT NULL DEFAULT false)`

func (e *postgresEvidence) Map(ctx context.Context, m mapping) error {
	raw, err := json.Marshal(m)
	if err != nil {
		return err
	}
	tag, err := e.pool.Exec(ctx, `INSERT INTO approval_host_intents(operation_id,mapping) VALUES ($1,$2)
 ON CONFLICT(operation_id) DO UPDATE SET mapping=EXCLUDED.mapping
 WHERE approval_host_intents.mapping IS NULL OR approval_host_intents.mapping=EXCLUDED.mapping`, m.OperationID, raw)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return errEvidence
	}
	return nil
}
func (e *postgresEvidence) Lookup(ctx context.Context, id string) (mapping, error) {
	var raw []byte
	err := e.pool.QueryRow(ctx, `SELECT mapping FROM approval_host_intents WHERE operation_id=$1`, id).Scan(&raw)
	if err != nil || len(raw) == 0 {
		return mapping{}, errors.Join(errEvidence, err)
	}
	var m mapping
	err = json.Unmarshal(raw, &m)
	return m, err
}
func (e *postgresEvidence) Allowed(ctx context.Context, id string) error {
	var revoked bool
	err := e.pool.QueryRow(ctx, `SELECT revoked FROM approval_host_intents WHERE operation_id=$1`, id).Scan(&revoked)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	if revoked {
		return errDenied
	}
	return nil
}
func (e *postgresEvidence) Revoke(ctx context.Context, id string) error {
	_, err := e.pool.Exec(ctx, `INSERT INTO approval_host_intents(operation_id,revoked) VALUES($1,true)
 ON CONFLICT(operation_id) DO UPDATE SET revoked=true`, id)
	return err
}
func (e *postgresEvidence) Capture(ctx context.Context, id, phase string) error {
	query := `UPDATE approval_host_intents SET captured_before=true WHERE operation_id=$1`
	if phase == "after" {
		query = `UPDATE approval_host_intents SET captured_after=true WHERE operation_id=$1 AND receipt IS NOT NULL`
	}
	tag, err := e.pool.Exec(ctx, query, id)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return errEvidence
	}
	return nil
}
func (e *postgresEvidence) Write(ctx context.Context, id string, in input, attempt int) (receipt, error) {
	tx, err := e.pool.Begin(ctx)
	if err != nil {
		return receipt{}, err
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	var revoked bool
	var current int
	var old []byte
	if err = tx.QueryRow(ctx, `SELECT revoked,receipt,current_attempt FROM approval_host_intents WHERE operation_id=$1 FOR UPDATE`, id).
		Scan(&revoked, &old, &current); err != nil {
		return receipt{}, err
	}
	if _, err = tx.Exec(ctx, `UPDATE approval_host_intents SET calls=calls+1 WHERE operation_id=$1`, id); err != nil {
		return receipt{}, err
	}
	if revoked || attempt != current || len(old) != 0 {
		if err = tx.Commit(ctx); err != nil {
			return receipt{}, err
		}
		if revoked || attempt != current {
			return receipt{}, errDenied
		}
		return receipt{}, errors.New("host: duplicate external dispatch")
	}

	r := receipt{OperationID: id, Value: in.Value, Accepted: in.Value != "reject"}
	writes := 0
	if r.Accepted {
		writes = 1
	}
	_, err = tx.Exec(
		ctx,
		`UPDATE approval_host_intents SET receipt=$2,writes=writes+$3 WHERE operation_id=$1`,
		id,
		mustJSON(r),
		writes,
	)
	if err != nil {
		return receipt{}, err
	}
	if err = tx.Commit(ctx); err != nil {
		return receipt{}, err
	}
	return r, nil
}
func (e *postgresEvidence) Receipt(ctx context.Context, id string) (receipt, error) {
	var raw []byte
	err := e.pool.QueryRow(ctx, `SELECT receipt FROM approval_host_intents WHERE operation_id=$1`, id).Scan(&raw)
	if err != nil || len(raw) == 0 {
		return receipt{}, errors.Join(errEvidence, err)
	}
	return decodeReceipt(raw)
}
func openPool(ctx context.Context, dsn, schema string) (*pgxpool.Pool, error) {
	config, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, err
	}
	config.MaxConns = 4
	config.ConnConfig.RuntimeParams["search_path"] = schema
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return nil, err
	}
	if err = pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("host database: %w", err)
	}
	return pool, nil
}

func (e *postgresEvidence) FenceAbsence(ctx context.Context, id string, attempt int) error {
	tag, err := e.pool.Exec(ctx, `UPDATE approval_host_intents SET current_attempt=$2+1
 WHERE operation_id=$1 AND current_attempt IN ($2,$2+1) AND receipt IS NULL`, id, attempt)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return errEvidence
	}
	return nil
}
