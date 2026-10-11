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
	"github.com/skosovsky/flowy/internal/nilvalue"
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

const threadIDArgument = "thread_id"

// DB captures the pgx methods used by the adapter.
type DB interface {
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	BeginTx(ctx context.Context, options pgx.TxOptions) (pgx.Tx, error)
}

// Checkpointer stores snapshots in PostgreSQL.
type Checkpointer[T, E any] struct {
	db         DB
	serializer flowy.StateSerializer[T]
}

// NewCheckpointer creates a PostgreSQL-backed checkpointer.
func NewCheckpointer[T, E any](db DB, serializer flowy.StateSerializer[T]) (*Checkpointer[T, E], error) {
	if nilvalue.IsNil(db) || nilvalue.IsNil(serializer) {
		return nil, flowy.ErrExecutionCapability
	}
	return &Checkpointer[T, E]{db: db, serializer: serializer}, nil
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
	return c.SaveWithOutbox(ctx, expectedRevision, snapshot, nil)
}

func saveCheckpointInTx[T, E any](
	ctx context.Context,
	tx pgx.Tx,
	expectedRevision uint64,
	snapshot flowy.Snapshot[T, E],
	serializer flowy.StateSerializer[T],
) (uint64, error) {
	newRevision := expectedRevision + 1
	snapshot.Revision = newRevision
	stored, err := checkpoint.EncodeRecord(snapshot, serializer)
	if err != nil {
		return 0, err
	}
	row := tx.QueryRow(ctx, saveOccSQL, pgx.NamedArgs{
		threadIDArgument:    stored.ThreadID,
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
	stored, err := scanRecord(c.db.QueryRow(ctx, loadLatestSQL, pgx.NamedArgs{threadIDArgument: threadID}))
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
		threadIDArgument: threadID,
		"limit":          limit,
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
			"DELETE FROM flowy_checkpoints WHERE thread_id = @thread_id::TEXT",
			pgx.NamedArgs{threadIDArgument: threadID},
		)
		return err
	}
	_, err := c.db.Exec(ctx, pruneSQL, pgx.NamedArgs{
		threadIDArgument: threadID,
		"retain_count":   retainCount,
	})
	return err
}

// Delete removes checkpoints unconditionally. Prefer DeleteIfIdle for runner policies.
func (c *Checkpointer[T, E]) Delete(ctx context.Context, threadID string) error {
	_, err := c.db.Exec(ctx,
		"DELETE FROM flowy_checkpoints WHERE thread_id = @thread_id::TEXT",
		pgx.NamedArgs{threadIDArgument: threadID},
	)
	return err
}

func (c *Checkpointer[T, E]) DeleteIfIdle(ctx context.Context, threadID string) error {
	options := pgx.TxOptions{} //nolint:exhaustruct_v5 // driver defaults; isolation is specified explicitly
	options.IsoLevel = pgx.ReadCommitted
	tx, err := c.db.BeginTx(ctx, options)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	// Same lock as the lease adapter. The following statement gets a fresh
	// snapshot after the lock; folding this into the DELETE would be unsafe.
	if _, lockErr := tx.Exec(
		ctx,
		`SELECT pg_advisory_xact_lock(hashtextextended(@thread_id::text, 0))`,
		pgx.NamedArgs{threadIDArgument: threadID},
	); lockErr != nil {
		return lockErr
	}
	tag, err := tx.Exec(ctx, deleteIfIdleSQL, pgx.NamedArgs{threadIDArgument: threadID})
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		var held bool
		row := tx.QueryRow(
			ctx,
			"SELECT EXISTS (SELECT 1 FROM flowy_leases WHERE thread_id = @thread_id::TEXT AND expires_at > clock_timestamp())",
			pgx.NamedArgs{threadIDArgument: threadID},
		)
		if scanErr := row.Scan(&held); scanErr != nil {
			return scanErr
		}
		if held {
			return flowy.ErrThreadLeaseBusy
		}
	}
	return tx.Commit(ctx)
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
