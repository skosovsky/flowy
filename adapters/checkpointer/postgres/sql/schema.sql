CREATE TABLE IF NOT EXISTS flowy_checkpoints (
    thread_id TEXT NOT NULL,
    revision BIGINT NOT NULL,
    node_id TEXT NOT NULL,
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
    thread_id TEXT PRIMARY KEY,
    owner TEXT NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL,
    incarnation BIGINT NOT NULL CHECK (incarnation > 0)
);

-- Fence history survives release/expiry/checkpoint deletion.
CREATE TABLE IF NOT EXISTS flowy_lease_fences (
    thread_id TEXT PRIMARY KEY,
    incarnation BIGINT NOT NULL CHECK (incarnation > 0)
);

CREATE INDEX IF NOT EXISTS idx_flowy_leases_expires_at
    ON flowy_leases(expires_at);
