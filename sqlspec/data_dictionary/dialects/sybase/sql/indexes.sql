-- name: by_schema
-- dialect: sybase
SELECT
    u.name AS schema_name,
    o.name AS table_name,
    i.name AS index_name,
    index_col(u.name + '.' + o.name, i.indid, 1) AS column_name,
    1 AS column_position,
    CASE WHEN (i.status & 2) = 2 THEN 1 ELSE 0 END AS is_unique,
    CASE WHEN (i.status & 2048) = 2048 THEN 1 ELSE 0 END AS is_primary
FROM dbo.sysindexes i
JOIN dbo.sysobjects o ON i.id = o.id
JOIN dbo.sysusers u ON o.uid = u.uid
WHERE o.type = 'U'
  AND i.indid BETWEEN 1 AND 254
  AND u.name = COALESCE(:schema_name, USER_NAME())
  AND (:table_name IS NULL OR o.name = :table_name)
ORDER BY u.name, o.name, i.name;

-- name: by_table
-- dialect: sybase
SELECT
    u.name AS schema_name,
    o.name AS table_name,
    i.name AS index_name,
    index_col(u.name + '.' + o.name, i.indid, 1) AS column_name,
    1 AS column_position,
    CASE WHEN (i.status & 2) = 2 THEN 1 ELSE 0 END AS is_unique,
    CASE WHEN (i.status & 2048) = 2048 THEN 1 ELSE 0 END AS is_primary
FROM dbo.sysindexes i
JOIN dbo.sysobjects o ON i.id = o.id
JOIN dbo.sysusers u ON o.uid = u.uid
WHERE o.type = 'U'
  AND i.indid BETWEEN 1 AND 254
  AND o.name = :table_name
  AND u.name = COALESCE(:schema_name, USER_NAME())
ORDER BY i.name;
