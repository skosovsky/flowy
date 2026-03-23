CREATE TABLE IF NOT EXISTS flowy_checkpoints (
    id UUID PRIMARY KEY,
    thread_id VARCHAR(255) NOT NULL,
    run_id VARCHAR(255) NOT NULL,
    node_name VARCHAR(255) NOT NULL,
    next_node VARCHAR(255) NOT NULL,
    state_data JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_flowy_cp_thread_latest
    ON flowy_checkpoints(thread_id, created_at DESC, id DESC);
