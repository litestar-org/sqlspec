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

-- name: reservations_by_region
-- dialect: bigquery
SELECT
    project_id,
    project_number,
    reservation_name,
    slot_capacity,
    ignore_idle_slots,
    edition,
    autoscale,
    concurrency,
    creation_time,
    update_time
FROM {reservations_table}
WHERE (:reservation_name IS NULL OR reservation_name = :reservation_name)
ORDER BY reservation_name;

-- name: sessions_by_region
-- dialect: bigquery
SELECT
    project_id,
    project_number,
    user_email,
    session_id,
    creation_time,
    expiration_time,
    is_active
FROM {sessions_table}
WHERE (:user_email IS NULL OR user_email = :user_email)
  AND (:is_active IS NULL OR is_active = :is_active)
ORDER BY creation_time DESC
LIMIT :limit;
