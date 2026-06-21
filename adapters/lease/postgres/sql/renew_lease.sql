UPDATE flowy_leases
SET expires_at = NOW() + (@ttl_seconds::int * INTERVAL '1 second')
WHERE thread_id = @thread_id
  AND owner = @owner
  AND expires_at > NOW();
