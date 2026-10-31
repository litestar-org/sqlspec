-- name: by_schema
-- dialect: sybase
SELECT
    u.name AS schema_name,
    o.name AS table_name,
    i.name AS index_name,
    index_col(u.name + '.' + o.name, i.indid, k.pos) AS column_name,
    k.pos AS column_position,
    CASE WHEN (i.status & 2) = 2 THEN 1 ELSE 0 END AS is_unique,
    CASE WHEN (i.status & 2048) = 2048 THEN 1 ELSE 0 END AS is_primary
FROM dbo.sysindexes i
JOIN dbo.sysobjects o ON i.id = o.id
JOIN dbo.sysusers u ON o.uid = u.uid
CROSS JOIN (
    SELECT 1 AS pos UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4
    UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8
    UNION ALL SELECT 9 UNION ALL SELECT 10 UNION ALL SELECT 11 UNION ALL SELECT 12
    UNION ALL SELECT 13 UNION ALL SELECT 14 UNION ALL SELECT 15 UNION ALL SELECT 16
) k
WHERE o.type = 'U'
  AND i.indid BETWEEN 1 AND 254
  AND index_col(u.name + '.' + o.name, i.indid, k.pos) IS NOT NULL
  AND u.name = COALESCE(:schema_name, USER_NAME())
  AND (:table_name IS NULL OR o.name = :table_name)
ORDER BY u.name, o.name, i.name, k.pos;

-- name: by_table
-- dialect: sybase
SELECT
    u.name AS schema_name,
    o.name AS table_name,
    i.name AS index_name,
    index_col(u.name + '.' + o.name, i.indid, k.pos) AS column_name,
    k.pos AS column_position,
    CASE WHEN (i.status & 2) = 2 THEN 1 ELSE 0 END AS is_unique,
    CASE WHEN (i.status & 2048) = 2048 THEN 1 ELSE 0 END AS is_primary
FROM dbo.sysindexes i
JOIN dbo.sysobjects o ON i.id = o.id
JOIN dbo.sysusers u ON o.uid = u.uid
CROSS JOIN (
    SELECT 1 AS pos UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4
    UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8
    UNION ALL SELECT 9 UNION ALL SELECT 10 UNION ALL SELECT 11 UNION ALL SELECT 12
    UNION ALL SELECT 13 UNION ALL SELECT 14 UNION ALL SELECT 15 UNION ALL SELECT 16
) k
WHERE o.type = 'U'
  AND i.indid BETWEEN 1 AND 254
  AND index_col(u.name + '.' + o.name, i.indid, k.pos) IS NOT NULL
  AND o.name = :table_name
  AND u.name = COALESCE(:schema_name, USER_NAME())
ORDER BY i.name, k.pos;
