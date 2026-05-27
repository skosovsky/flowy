WITH boundary AS (
    SELECT revision
    FROM flowy_checkpoints
    WHERE thread_id = @thread_id
    ORDER BY revision DESC
    OFFSET @retain_count
    LIMIT 1
)
DELETE FROM flowy_checkpoints
WHERE thread_id = @thread_id
  AND revision <= (SELECT revision FROM boundary);
