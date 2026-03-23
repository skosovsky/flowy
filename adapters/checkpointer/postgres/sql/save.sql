INSERT INTO flowy_checkpoints (
    id,
    thread_id,
    run_id,
    node_name,
    next_node,
    state_data,
    created_at
) VALUES (
    @id,
    @thread_id,
    @run_id,
    @node_name,
    @next_node,
    @state_data,
    @created_at
)
ON CONFLICT (id) DO NOTHING;
