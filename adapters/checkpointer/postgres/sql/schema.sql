CREATE TABLE IF NOT EXISTS flowy_checkpoints (
    thread_id VARCHAR(255) NOT NULL,
    revision INT NOT NULL,
    node_id VARCHAR(255) NOT NULL,
    state_payload JSONB NOT NULL,
    run_meta JSONB NOT NULL,
    effects JSONB NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (thread_id, revision)
);

CREATE INDEX IF NOT EXISTS idx_flowy_cp_updated_at
    ON flowy_checkpoints(updated_at DESC);

CREATE INDEX IF NOT EXISTS idx_flowy_cp_thread_revision
    ON flowy_checkpoints(thread_id, revision DESC);
