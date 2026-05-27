INSERT INTO flowy_checkpoints (
    thread_id,
    revision,
    node_id,
    state_payload,
    run_meta,
    effects,
    updated_at
) VALUES (
    @thread_id,
    @revision,
    @node_id,
    @state_payload,
    @run_meta,
    @effects,
    @updated_at
);
