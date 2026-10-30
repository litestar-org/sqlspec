-- name: by_schema
-- dialect: sybase
SELECT
    u.name AS schema_name,
    o.name AS view_name,
    cm.text AS definition,
    0 AS is_read_only,
    1 AS is_valid
FROM dbo.sysobjects o
JOIN dbo.sysusers u ON o.uid = u.uid
LEFT JOIN dbo.syscomments cm ON o.id = cm.id AND cm.colid = 1
WHERE o.type = 'V'
  AND u.name = COALESCE(:schema_name, USER_NAME())
  AND (:view_name IS NULL OR o.name = :view_name)
ORDER BY u.name, o.name;
