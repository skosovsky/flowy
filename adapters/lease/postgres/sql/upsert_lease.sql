WITH next_fence AS (
    INSERT INTO flowy_lease_fences (thread_id, incarnation)
    SELECT @thread_id, 1
    WHERE NOT EXISTS (
        SELECT 1 FROM flowy_leases
        WHERE thread_id = @thread_id AND expires_at > clock_timestamp()
    )
    ON CONFLICT (thread_id) DO UPDATE
    SET incarnation = flowy_lease_fences.incarnation + 1
    RETURNING incarnation
)
INSERT INTO flowy_leases (thread_id, owner, expires_at, incarnation)
SELECT @thread_id, @owner,
       clock_timestamp() + (@ttl_seconds::double precision * INTERVAL '1 second'), incarnation
FROM next_fence
ON CONFLICT (thread_id) DO UPDATE
SET owner = EXCLUDED.owner,
    expires_at = EXCLUDED.expires_at,
    incarnation = EXCLUDED.incarnation;
