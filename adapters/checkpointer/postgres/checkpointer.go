// Package postgres provides PostgreSQL checkpointer adapter.
package postgres

import (
	"context"
	_ "embed"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/skosovsky/flowy"
	"github.com/skosovsky/flowy/checkpoint"
)

//go:embed sql/schema.sql
var schemaSQL string

//go:embed sql/save_occ.sql
var saveOccSQL string

//go:embed sql/load_latest.sql
var loadLatestSQL string

//go:embed sql/get_history.sql
var getHistorySQL string

//go:embed sql/prune.sql
var pruneSQL string

//go:embed sql/delete_if_idle.sql
var deleteIfIdleSQL string

//go:embed sql/outbox_schema.sql
var outboxSchemaSQL string

// DB captures the pgx methods used by the adapter.
type DB interface {
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// Checkpointer stores snapshots in PostgreSQL.
type Checkpointer[T, E any] struct {
	db         DB
	serializer flowy.StateSerializer[T]
}

// NewCheckpointer creates a PostgreSQL-backed checkpointer.
func NewCheckpointer[T, E any](db DB, serializer flowy.StateSerializer[T]) *Checkpointer[T, E] {
	return &Checkpointer[T, E]{db: db, serializer: serializer}
}

// SchemaSQL returns the schema expected by the adapter.
func SchemaSQL() string {
	return schemaSQL
}

// OutboxSchemaSQL returns the optional handoff outbox table for transactional SaveWithOutbox tests.
func OutboxSchemaSQL() string {
	return outboxSchemaSQL
}

func (c *Checkpointer[T, E]) Save(
	ctx context.Context,
	expectedRevision uint64,
	snapshot flowy.Snapshot[T, E],
) (uint64, error) {
	newRevision := expectedRevision + 1
	snapshot.Revision = newRevision
	stored, err := checkpoint.EncodeRecord(snapshot, c.serializer)
	if err != nil {
		return 0, err
	}
	row := c.db.QueryRow(ctx, saveOccSQL, pgx.NamedArgs{
		"thread_id":         stored.ThreadID,
		"expected_revision": expectedRevision,
		"node_id":           stored.NodeID,
		"state_payload":     stored.StatePayload,
		"run_meta":          stored.RunMeta,
		"effects":           stored.Effects,
		"updated_at":        stored.UpdatedAt,
	})
	var inserted uint64
	if err := row.Scan(&inserted); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return 0, flowy.ErrConcurrencyConflict
		}
		return 0, err
	}
	return inserted, nil
}

func (c *Checkpointer[T, E]) Load(ctx context.Context, threadID string) (flowy.Snapshot[T, E], uint64, error) {
	stored, err := scanRecord(c.db.QueryRow(ctx, loadLatestSQL, pgx.NamedArgs{"thread_id": threadID}))
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return flowy.Snapshot[T, E]{}, 0, fmt.Errorf("%w: %w", flowy.ErrThreadNotFound, checkpoint.ErrNoSnapshot)
		}
		return flowy.Snapshot[T, E]{}, 0, err
	}
	snapshot, err := checkpoint.DecodeRecord[T, E](
		stored,
		c.serializer,
		checkpoint.DecodeRecordOptions{
			ExpectedThreadID:         threadID,
			ExpectedRevision:         0,
			ExpectedExecutionPointer: "",
		},
	)
	if err != nil {
		return flowy.Snapshot[T, E]{}, 0, err
	}
	return snapshot, snapshot.Revision, nil
}

func (c *Checkpointer[T, E]) GetHistory(
	ctx context.Context,
	threadID string,
	limit int,
) ([]flowy.Snapshot[T, E], error) {
	rows, err := c.db.Query(ctx, getHistorySQL, pgx.NamedArgs{
		"thread_id": threadID,
		"limit":     limit,
	})
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]flowy.Snapshot[T, E], 0)
	for rows.Next() {
		stored, scanErr := scanRecord(rows)
		if scanErr != nil {
			return nil, scanErr
		}
		snapshot, decodeErr := checkpoint.DecodeRecord[T, E](
			stored,
			c.serializer,
			checkpoint.DecodeRecordOptions{
				ExpectedThreadID:         threadID,
				ExpectedRevision:         0,
				ExpectedExecutionPointer: "",
			},
		)
		if decodeErr != nil {
			return nil, decodeErr
		}
		out = append(out, snapshot)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func (c *Checkpointer[T, E]) Prune(ctx context.Context, threadID string, retainCount int) error {
	if retainCount <= 0 {
		_, err := c.db.Exec(ctx,
			"DELETE FROM flowy_checkpoints WHERE thread_id = @thread_id::varchar(255)",
			pgx.NamedArgs{"thread_id": threadID},
		)
		return err
	}
	_, err := c.db.Exec(ctx, pruneSQL, pgx.NamedArgs{
		"thread_id":    threadID,
		"retain_count": retainCount,
	})
	return err
}

// Delete removes checkpoints unconditionally. Prefer DeleteIfIdle for runner policies.
func (c *Checkpointer[T, E]) Delete(ctx context.Context, threadID string) error {
	_, err := c.db.Exec(ctx,
		"DELETE FROM flowy_checkpoints WHERE thread_id = @thread_id::varchar(255)",
		pgx.NamedArgs{"thread_id": threadID},
	)
	return err
}

func (c *Checkpointer[T, E]) DeleteIfIdle(ctx context.Context, threadID string) error {
	tag, err := c.db.Exec(ctx, deleteIfIdleSQL, pgx.NamedArgs{"thread_id": threadID})
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		var held bool
		row := c.db.QueryRow(
			ctx,
			"SELECT EXISTS (SELECT 1 FROM flowy_leases WHERE thread_id = @thread_id::varchar(255) AND expires_at > NOW())",
			pgx.NamedArgs{"thread_id": threadID},
		)
		if scanErr := row.Scan(&held); scanErr == nil && held {
			return flowy.ErrThreadLeaseBusy
		}
	}
	return nil
}

func scanRecord(row interface{ Scan(dest ...any) error }) (checkpoint.Record, error) {
	var (
		stored checkpoint.Record
	)

	err := row.Scan(
		&stored.ThreadID,
		&stored.Revision,
		&stored.NodeID,
		&stored.StatePayload,
		&stored.RunMeta,
		&stored.Effects,
		&stored.UpdatedAt,
	)
	if err != nil {
		return checkpoint.Record{}, fmt.Errorf("postgres checkpoint scan: %w", err)
	}
	return stored, nil
}

// NativeDeleteIfIdle marks atomic delete-if-idle in PostgreSQL storage.
func (*Checkpointer[T, E]) NativeDeleteIfIdle() {}

var _ flowy.Checkpointer[any, any] = (*Checkpointer[any, any])(nil)
var _ flowy.NativeDeleteIfIdleCheckpointer = (*Checkpointer[any, any])(nil)
