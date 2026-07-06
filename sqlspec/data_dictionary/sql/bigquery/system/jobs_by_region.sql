-- name: jobs_by_region
-- dialect: bigquery
SELECT
    project_id,
    project_number,
    user_email,
    job_id,
    job_type,
    statement_type,
    priority,
    creation_time,
    start_time,
    end_time,
    state,
    reservation_id,
    total_bytes_processed,
    total_slot_ms,
    cache_hit,
    error_result,
    query
FROM {jobs_table}
WHERE (:user_email IS NULL OR user_email = :user_email)
  AND (:state IS NULL OR state = :state)
  AND (:created_after IS NULL OR creation_time >= :created_after)
ORDER BY creation_time DESC
LIMIT :limit;
