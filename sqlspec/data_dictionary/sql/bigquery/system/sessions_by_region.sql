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
