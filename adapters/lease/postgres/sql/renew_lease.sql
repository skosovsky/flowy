UPDATE flowy_leases
SET expires_at = clock_timestamp() + (@ttl_seconds::double precision * INTERVAL '1 second')
WHERE thread_id = @thread_id
  AND owner = @owner
  AND incarnation = @incarnation
  AND expires_at > clock_timestamp();
