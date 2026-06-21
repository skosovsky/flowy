INSERT INTO flowy_leases (thread_id, owner, expires_at)
VALUES (@thread_id, @owner, NOW() + (@ttl_seconds::int * INTERVAL '1 second'))
ON CONFLICT (thread_id) DO UPDATE
SET owner = EXCLUDED.owner,
    expires_at = EXCLUDED.expires_at
WHERE flowy_leases.expires_at <= NOW()
   OR flowy_leases.owner = EXCLUDED.owner;
