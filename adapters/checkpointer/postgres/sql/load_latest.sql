SELECT
    thread_id,
    revision,
    node_id,
    state_payload,
    run_meta,
    effects,
    updated_at
FROM flowy_checkpoints
WHERE thread_id = @thread_id
ORDER BY revision DESC
LIMIT 1;
