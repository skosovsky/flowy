CREATE TABLE IF NOT EXISTS flowy_handoff_outbox (
    id BIGSERIAL PRIMARY KEY,
    thread_id TEXT NOT NULL,
    snapshot_revision BIGINT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (thread_id, snapshot_revision)
);
