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
    @thread_id::varchar(255),
    (@expected_revision::bigint + 1),
    @node_id::varchar(255),
    @state_payload,
    @run_meta,
    @effects,
    @updated_at
WHERE COALESCE(
    (SELECT MAX(revision) FROM flowy_checkpoints WHERE thread_id = @thread_id::varchar(255)),
    0
) = @expected_revision::bigint
RETURNING revision;
