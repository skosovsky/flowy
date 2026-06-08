WITH active AS (
    SELECT 1
    FROM flowy_leases
    WHERE thread_id = @thread_id::varchar(255)
      AND expires_at > NOW()
    LIMIT 1
)
DELETE FROM flowy_checkpoints
WHERE thread_id = @thread_id::varchar(255)
  AND NOT EXISTS (SELECT 1 FROM active);
