-- name: by_schema
-- dialect: sqlanywhere
SELECT
    u.user_name AS schema_name,
    u.user_name AS owner_name,
    NULL AS create_time,
    NULL AS comment
FROM SYS.SYSUSER u
WHERE u.user_name NOT IN ('SYS', 'PUBLIC', 'dbo', 'diagnostics', 'rs_systabgroup', 'SA_DEBUG', 'SYS_SPATIAL_ADMIN_ROLE')
  AND (:schema_name IS NULL OR u.user_name = :schema_name)
ORDER BY u.user_name;

-- name: current
-- dialect: sqlanywhere
SELECT CURRENT USER AS schema_name;
