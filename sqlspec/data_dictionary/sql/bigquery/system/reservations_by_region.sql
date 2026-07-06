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
