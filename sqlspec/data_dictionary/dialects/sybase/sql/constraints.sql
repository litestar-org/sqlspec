-- name: by_schema
-- dialect: sybase
SELECT
    u.name AS schema_name,
    o.name AS table_name,
    co.name AS constraint_name,
    CASE WHEN (co.sysstat & 15) = 9 THEN 'FOREIGN KEY' ELSE 'CHECK' END AS constraint_type,
    CASE WHEN (co.sysstat & 15) = 10 AND (COALESCE(cm.status, 0) & 1) = 0
        THEN cm.text ELSE NULL END AS check_clause,
    'Y' AS is_enforced,
    cm.id AS _definition_id,
    cm.number AS _definition_number,
    cm.colid2 AS _definition_colid2,
    cm.colid AS _definition_colid
FROM dbo.sysconstraints c
JOIN dbo.sysobjects o ON c.tableid = o.id
JOIN dbo.sysusers u ON o.uid = u.uid
JOIN dbo.sysobjects co ON c.constrid = co.id
LEFT JOIN dbo.syscomments cm ON co.id = cm.id AND cm.texttype = 0
WHERE (co.sysstat & 15) IN (9, 10)
  AND u.name = COALESCE(:schema_name, USER_NAME())
  AND (:table_name IS NULL OR o.name = :table_name)
UNION ALL
SELECT
    u.name AS schema_name,
    o.name AS table_name,
    i.name AS constraint_name,
    CASE WHEN (i.status & 2048) = 2048 THEN 'PRIMARY KEY' ELSE 'UNIQUE' END AS constraint_type,
    NULL AS check_clause,
    'Y' AS is_enforced,
    0 AS _definition_id,
    0 AS _definition_number,
    0 AS _definition_colid2,
    0 AS _definition_colid
FROM dbo.sysindexes i
JOIN dbo.sysobjects o ON i.id = o.id
JOIN dbo.sysusers u ON o.uid = u.uid
WHERE o.type = 'U'
  AND ((i.status & 2048) = 2048 OR (i.status & 2) = 2)
  AND u.name = COALESCE(:schema_name, USER_NAME())
  AND (:table_name IS NULL OR o.name = :table_name)
ORDER BY schema_name, table_name, constraint_name, _definition_number, _definition_colid2, _definition_colid;

-- name: by_table
-- dialect: sybase
SELECT
    u.name AS schema_name,
    o.name AS table_name,
    co.name AS constraint_name,
    CASE WHEN (co.sysstat & 15) = 9 THEN 'FOREIGN KEY' ELSE 'CHECK' END AS constraint_type,
    CASE WHEN (co.sysstat & 15) = 10 AND (COALESCE(cm.status, 0) & 1) = 0
        THEN cm.text ELSE NULL END AS check_clause,
    'Y' AS is_enforced,
    cm.id AS _definition_id,
    cm.number AS _definition_number,
    cm.colid2 AS _definition_colid2,
    cm.colid AS _definition_colid
FROM dbo.sysconstraints c
JOIN dbo.sysobjects o ON c.tableid = o.id
JOIN dbo.sysusers u ON o.uid = u.uid
JOIN dbo.sysobjects co ON c.constrid = co.id
LEFT JOIN dbo.syscomments cm ON co.id = cm.id AND cm.texttype = 0
WHERE (co.sysstat & 15) IN (9, 10)
  AND u.name = COALESCE(:schema_name, USER_NAME())
  AND o.name = :table_name
UNION ALL
SELECT
    u.name AS schema_name,
    o.name AS table_name,
    i.name AS constraint_name,
    CASE WHEN (i.status & 2048) = 2048 THEN 'PRIMARY KEY' ELSE 'UNIQUE' END AS constraint_type,
    NULL AS check_clause,
    'Y' AS is_enforced,
    0 AS _definition_id,
    0 AS _definition_number,
    0 AS _definition_colid2,
    0 AS _definition_colid
FROM dbo.sysindexes i
JOIN dbo.sysobjects o ON i.id = o.id
JOIN dbo.sysusers u ON o.uid = u.uid
WHERE o.type = 'U'
  AND ((i.status & 2048) = 2048 OR (i.status & 2) = 2)
  AND u.name = COALESCE(:schema_name, USER_NAME())
  AND o.name = :table_name
ORDER BY schema_name, table_name, constraint_name, _definition_number, _definition_colid2, _definition_colid;
