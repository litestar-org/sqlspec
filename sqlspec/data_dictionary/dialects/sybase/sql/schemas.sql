-- name: by_schema
-- dialect: sybase
SELECT
    u.name AS schema_name,
    u.name AS owner_name,
    NULL AS create_time,
    NULL AS comment
FROM dbo.sysusers u
WHERE u.uid < 16384
  AND u.name NOT IN ('public', 'probe', 'guest')
  AND (:schema_name IS NULL OR u.name = :schema_name)
ORDER BY u.name;

-- name: current
-- dialect: sybase
SELECT USER_NAME() AS schema_name;
