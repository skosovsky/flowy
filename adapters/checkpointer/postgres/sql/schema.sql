CREATE TABLE IF NOT EXISTS flowy_checkpoints (
    thread_id VARCHAR(255) NOT NULL,
    revision BIGINT NOT NULL,
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

CREATE TABLE IF NOT EXISTS flowy_leases (
    thread_id VARCHAR(255) PRIMARY KEY,
    owner VARCHAR(255) NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_flowy_leases_expires_at
    ON flowy_leases(expires_at);
