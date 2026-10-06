INSERT INTO flowy_checkpoints (
    thread_id,
    revision,
    node_id,
    state_payload,
    run_meta,
    effects,
    updated_at
)
SELECT
    @thread_id::TEXT,
    (@expected_revision::bigint + 1),
    @node_id::TEXT,
    @state_payload,
    @run_meta,
    @effects,
    @updated_at
WHERE COALESCE(
    (SELECT MAX(revision) FROM flowy_checkpoints WHERE thread_id = @thread_id::TEXT),
    0
) = @expected_revision::bigint
RETURNING revision;
