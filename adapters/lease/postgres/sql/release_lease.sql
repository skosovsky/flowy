DELETE FROM flowy_leases
WHERE thread_id = @thread_id
  AND owner = @owner;
