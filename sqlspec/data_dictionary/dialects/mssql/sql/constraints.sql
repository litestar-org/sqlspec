-- name: by_schema
-- dialect: mssql
WITH key_constraint_columns AS (
    SELECT
        ic.object_id,
        ic.index_id,
        STRING_AGG(c.name, ',') WITHIN GROUP (ORDER BY ic.key_ordinal) AS column_name
    FROM sys.index_columns AS ic
    INNER JOIN sys.columns AS c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
    WHERE ic.is_included_column = 0
    GROUP BY ic.object_id, ic.index_id
)
SELECT
    s.name AS schema_name,
    t.name AS table_name,
    kc.name AS constraint_name,
    kc.type_desc AS constraint_type,
    CAST(kc.is_system_named AS BIT) AS is_system_named,
    kcc.column_name,
    NULL AS definition
FROM sys.key_constraints AS kc
INNER JOIN sys.tables AS t ON kc.parent_object_id = t.object_id
INNER JOIN sys.schemas AS s ON t.schema_id = s.schema_id
LEFT JOIN key_constraint_columns AS kcc ON kc.parent_object_id = kcc.object_id AND kc.unique_index_id = kcc.index_id
WHERE (:schema_name IS NULL OR s.name = :schema_name)
UNION ALL
SELECT
    s.name AS schema_name,
    t.name AS table_name,
    cc.name AS constraint_name,
    'CHECK_CONSTRAINT' AS constraint_type,
    CAST(cc.is_system_named AS BIT) AS is_system_named,
    NULL AS column_name,
    cc.definition
FROM sys.check_constraints AS cc
INNER JOIN sys.tables AS t ON cc.parent_object_id = t.object_id
INNER JOIN sys.schemas AS s ON t.schema_id = s.schema_id
WHERE (:schema_name IS NULL OR s.name = :schema_name)
UNION ALL
SELECT
    s.name AS schema_name,
    t.name AS table_name,
    dc.name AS constraint_name,
    'DEFAULT_CONSTRAINT' AS constraint_type,
    CAST(dc.is_system_named AS BIT) AS is_system_named,
    c.name AS column_name,
    dc.definition
FROM sys.default_constraints AS dc
INNER JOIN sys.tables AS t ON dc.parent_object_id = t.object_id
INNER JOIN sys.schemas AS s ON t.schema_id = s.schema_id
INNER JOIN sys.columns AS c ON dc.parent_object_id = c.object_id AND dc.parent_column_id = c.column_id
WHERE (:schema_name IS NULL OR s.name = :schema_name)
ORDER BY schema_name, table_name, constraint_name;

-- name: by_table
-- dialect: mssql
WITH key_constraint_columns AS (
    SELECT
        ic.object_id,
        ic.index_id,
        STRING_AGG(c.name, ',') WITHIN GROUP (ORDER BY ic.key_ordinal) AS column_name
    FROM sys.index_columns AS ic
    INNER JOIN sys.columns AS c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
    WHERE ic.is_included_column = 0
    GROUP BY ic.object_id, ic.index_id
)
SELECT
    s.name AS schema_name,
    t.name AS table_name,
    kc.name AS constraint_name,
    kc.type_desc AS constraint_type,
    CAST(kc.is_system_named AS BIT) AS is_system_named,
    kcc.column_name,
    NULL AS definition
FROM sys.key_constraints AS kc
INNER JOIN sys.tables AS t ON kc.parent_object_id = t.object_id
INNER JOIN sys.schemas AS s ON t.schema_id = s.schema_id
LEFT JOIN key_constraint_columns AS kcc ON kc.parent_object_id = kcc.object_id AND kc.unique_index_id = kcc.index_id
WHERE s.name = :schema_name
  AND t.name = :table_name
UNION ALL
SELECT
    s.name AS schema_name,
    t.name AS table_name,
    cc.name AS constraint_name,
    'CHECK_CONSTRAINT' AS constraint_type,
    CAST(cc.is_system_named AS BIT) AS is_system_named,
    NULL AS column_name,
    cc.definition
FROM sys.check_constraints AS cc
INNER JOIN sys.tables AS t ON cc.parent_object_id = t.object_id
INNER JOIN sys.schemas AS s ON t.schema_id = s.schema_id
WHERE s.name = :schema_name
  AND t.name = :table_name
UNION ALL
SELECT
    s.name AS schema_name,
    t.name AS table_name,
    dc.name AS constraint_name,
    'DEFAULT_CONSTRAINT' AS constraint_type,
    CAST(dc.is_system_named AS BIT) AS is_system_named,
    c.name AS column_name,
    dc.definition
FROM sys.default_constraints AS dc
INNER JOIN sys.tables AS t ON dc.parent_object_id = t.object_id
INNER JOIN sys.schemas AS s ON t.schema_id = s.schema_id
INNER JOIN sys.columns AS c ON dc.parent_object_id = c.object_id AND dc.parent_column_id = c.column_id
WHERE s.name = :schema_name
  AND t.name = :table_name
ORDER BY schema_name, table_name, constraint_name;
