UPDATE flowy_leases
SET expires_at = NOW() + (@ttl_seconds || ' seconds')::interval
WHERE thread_id = @thread_id
  AND owner = @owner
  AND expires_at > NOW();
