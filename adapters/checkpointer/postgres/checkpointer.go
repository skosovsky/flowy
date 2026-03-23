package postgres

import (
	"context"
	_ "embed"
	"encoding/json"
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/skosovsky/flowy/checkpoint"
)

//go:embed sql/schema.sql
var schemaSQL string

//go:embed sql/save.sql
var saveSQL string

//go:embed sql/load_latest.sql
var loadLatestSQL string

//go:embed sql/get_history.sql
var getHistorySQL string

//go:embed sql/get_history_all.sql
var getHistoryAllSQL string

// DB captures the pgx methods used by the adapter.
type DB interface {
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// Checkpointer stores checkpoints in PostgreSQL.
type Checkpointer struct {
	db DB
}

// New creates a PostgreSQL-backed checkpoint.Checkpointer.
func New(db DB) *Checkpointer {
	return &Checkpointer{db: db}
}

// SchemaSQL returns the schema expected by the adapter.
func SchemaSQL() string {
	return schemaSQL
}

func (c *Checkpointer) Save(ctx context.Context, cp checkpoint.Checkpoint) error {
	_, err := c.db.Exec(ctx, saveSQL, pgx.NamedArgs{
		"id":         cp.ID,
		"thread_id":  cp.ThreadID,
		"run_id":     cp.RunID,
		"node_name":  cp.Node,
		"next_node":  cp.Next,
		"state_data": json.RawMessage(cp.StateData),
		"created_at": cp.CreatedAt,
	})
	return err
}

func (c *Checkpointer) LoadLatest(ctx context.Context, threadID string) (checkpoint.Checkpoint, error) {
	cp, err := scanCheckpoint(c.db.QueryRow(ctx, loadLatestSQL, pgx.NamedArgs{"thread_id": threadID}))
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return checkpoint.Checkpoint{}, checkpoint.ErrNoCheckpoint
		}
		return checkpoint.Checkpoint{}, err
	}
	return cp, nil
}

func (c *Checkpointer) GetHistory(ctx context.Context, threadID string, limit int) ([]checkpoint.Checkpoint, error) {
	query := getHistoryAllSQL
	args := pgx.NamedArgs{"thread_id": threadID}
	if limit > 0 {
		query = getHistorySQL
		args["limit"] = limit
	}

	rows, err := c.db.Query(ctx, query, args)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var history []checkpoint.Checkpoint
	for rows.Next() {
		cp, scanErr := scanCheckpoint(rows)
		if scanErr != nil {
			return nil, scanErr
		}
		history = append(history, cp)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(history) == 0 {
		return nil, checkpoint.ErrNoCheckpoint
	}
	return history, nil
}

func scanCheckpoint(row interface{ Scan(dest ...any) error }) (checkpoint.Checkpoint, error) {
	var (
		cp       checkpoint.Checkpoint
		stateRaw string
	)

	err := row.Scan(
		&cp.ID,
		&cp.ThreadID,
		&cp.RunID,
		&cp.Node,
		&cp.Next,
		&stateRaw,
		&cp.CreatedAt,
	)
	if err != nil {
		return checkpoint.Checkpoint{}, err
	}

	cp.StateData = []byte(stateRaw)
	return cp, nil
}

var _ checkpoint.Checkpointer = (*Checkpointer)(nil)
