SELECT
    id,
    thread_id,
    run_id,
    node_name,
    next_node,
    state_data::text AS state_data,
    created_at
FROM flowy_checkpoints
WHERE thread_id = @thread_id::varchar(255)
ORDER BY created_at DESC, id DESC;
