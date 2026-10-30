-- name: by_schema
-- dialect: sybase
SELECT
    u.name AS schema_name,
    o.name AS table_name,
    'BASE TABLE' AS table_type,
    CASE
        WHEN EXISTS (
            SELECT 1
            FROM dbo.sysreferences r
            WHERE r.tableid = o.id
              AND r.reftabid <> o.id
        ) THEN 1
        ELSE 0
    END AS dependency_level,
    CASE
        WHEN EXISTS (
            SELECT 1
            FROM dbo.sysreferences r
            WHERE r.tableid = o.id
              AND r.reftabid <> o.id
        ) THEN 1
        ELSE 0
    END AS level
FROM dbo.sysobjects o
JOIN dbo.sysusers u ON o.uid = u.uid
WHERE o.type = 'U'
  AND u.name = COALESCE(:schema_name, USER_NAME())
  AND (:table_name IS NULL OR o.name = :table_name)
ORDER BY dependency_level, u.name, o.name;

-- name: all_by_schema
-- dialect: sybase
SELECT
    u.name AS schema_name,
    o.name AS table_name,
    CASE o.type
        WHEN 'U' THEN 'BASE TABLE'
        WHEN 'V' THEN 'VIEW'
        WHEN 'S' THEN 'SYSTEM TABLE'
        ELSE 'TABLE'
    END AS table_type,
    0 AS dependency_level,
    0 AS level,
    NULL AS estimated_row_count,
    NULL AS table_comment
FROM dbo.sysobjects o
JOIN dbo.sysusers u ON o.uid = u.uid
WHERE o.type IN ('U', 'V')
  AND u.name = COALESCE(:schema_name, USER_NAME())
  AND (:table_name IS NULL OR o.name = :table_name)
ORDER BY u.name, o.name;

-- name: names_by_schema
-- dialect: sybase
SELECT
    o.name AS table_name
FROM dbo.sysobjects o
JOIN dbo.sysusers u ON o.uid = u.uid
WHERE o.type = 'U'
  AND u.name = COALESCE(:schema_name, USER_NAME())
ORDER BY o.name;
