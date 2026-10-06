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
