-- name: by_schema
-- dialect: sqlanywhere
SELECT
    u.user_name AS schema_name,
    t.table_name AS view_name,
    v.view_def AS definition,
    0 AS is_read_only,
    1 AS is_valid
FROM SYS.SYSTAB t
JOIN SYS.SYSUSER u ON t.creator = u.user_id
LEFT JOIN SYS.SYSVIEW v ON t.object_id = v.view_object_id
WHERE t.table_type = 21
  AND u.user_name = COALESCE(:schema_name, CURRENT USER)
  AND (:view_name IS NULL OR t.table_name = :view_name)
ORDER BY u.user_name, t.table_name;
