-- name: by_schema
-- dialect: sybase
SELECT
    u.name AS schema_name,
    o.name AS table_name,
    co.name AS constraint_name,
    CASE
        WHEN (c.status & 1) = 1 THEN 'PRIMARY KEY'
        WHEN (c.status & 2) = 2 THEN 'UNIQUE'
        WHEN (c.status & 64) = 64 THEN 'FOREIGN KEY'
        WHEN (c.status & 128) = 128 THEN 'CHECK'
        ELSE 'CONSTRAINT'
    END AS constraint_type,
    cm.text AS check_clause,
    'Y' AS is_enforced
FROM dbo.sysconstraints c
JOIN dbo.sysobjects o ON c.tableid = o.id
JOIN dbo.sysusers u ON o.uid = u.uid
JOIN dbo.sysobjects co ON c.constrid = co.id
LEFT JOIN dbo.syscomments cm ON co.id = cm.id AND cm.colid = 1
WHERE u.name = COALESCE(:schema_name, USER_NAME())
  AND (:table_name IS NULL OR o.name = :table_name)
UNION
SELECT
    u.name AS schema_name,
    o.name AS table_name,
    i.name AS constraint_name,
    CASE
        WHEN (i.status & 2048) = 2048 THEN 'PRIMARY KEY'
        WHEN (i.status & 2) = 2 THEN 'UNIQUE'
    END AS constraint_type,
    NULL AS check_clause,
    'Y' AS is_enforced
FROM dbo.sysindexes i
JOIN dbo.sysobjects o ON i.id = o.id
JOIN dbo.sysusers u ON o.uid = u.uid
WHERE o.type = 'U'
  AND ((i.status & 2048) = 2048 OR (i.status & 2) = 2)
  AND u.name = COALESCE(:schema_name, USER_NAME())
  AND (:table_name IS NULL OR o.name = :table_name)
  AND NOT EXISTS (
      SELECT 1
      FROM dbo.sysconstraints sc
      JOIN dbo.sysobjects sco ON sc.constrid = sco.id
      WHERE sc.tableid = o.id AND sco.name = i.name
  )
ORDER BY schema_name, table_name, constraint_name;

-- name: by_table
-- dialect: sybase
SELECT
    u.name AS schema_name,
    o.name AS table_name,
    co.name AS constraint_name,
    CASE
        WHEN (c.status & 1) = 1 THEN 'PRIMARY KEY'
        WHEN (c.status & 2) = 2 THEN 'UNIQUE'
        WHEN (c.status & 64) = 64 THEN 'FOREIGN KEY'
        WHEN (c.status & 128) = 128 THEN 'CHECK'
        ELSE 'CONSTRAINT'
    END AS constraint_type,
    cm.text AS check_clause,
    'Y' AS is_enforced
FROM dbo.sysconstraints c
JOIN dbo.sysobjects o ON c.tableid = o.id
JOIN dbo.sysusers u ON o.uid = u.uid
JOIN dbo.sysobjects co ON c.constrid = co.id
LEFT JOIN dbo.syscomments cm ON co.id = cm.id AND cm.colid = 1
WHERE o.name = :table_name
  AND u.name = COALESCE(:schema_name, USER_NAME())
UNION
SELECT
    u.name AS schema_name,
    o.name AS table_name,
    i.name AS constraint_name,
    CASE
        WHEN (i.status & 2048) = 2048 THEN 'PRIMARY KEY'
        WHEN (i.status & 2) = 2 THEN 'UNIQUE'
    END AS constraint_type,
    NULL AS check_clause,
    'Y' AS is_enforced
FROM dbo.sysindexes i
JOIN dbo.sysobjects o ON i.id = o.id
JOIN dbo.sysusers u ON o.uid = u.uid
WHERE o.type = 'U'
  AND ((i.status & 2048) = 2048 OR (i.status & 2) = 2)
  AND o.name = :table_name
  AND u.name = COALESCE(:schema_name, USER_NAME())
  AND NOT EXISTS (
      SELECT 1
      FROM dbo.sysconstraints sc
      JOIN dbo.sysobjects sco ON sc.constrid = sco.id
      WHERE sc.tableid = o.id AND sco.name = i.name
  )
ORDER BY constraint_name;
