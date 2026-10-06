WITH active AS (
    SELECT 1
    FROM flowy_leases
    WHERE thread_id = @thread_id::TEXT
      AND expires_at > clock_timestamp()
    LIMIT 1
)
DELETE FROM flowy_checkpoints
WHERE thread_id = @thread_id::TEXT
  AND NOT EXISTS (SELECT 1 FROM active);
