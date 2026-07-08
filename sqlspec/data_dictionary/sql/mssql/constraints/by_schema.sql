-- name: by_schema
-- dialect: mssql
SELECT
    s.name AS schema_name,
    t.name AS table_name,
    kc.name AS constraint_name,
    kc.type_desc AS constraint_type,
    CAST(kc.is_system_named AS BIT) AS is_system_named,
    NULL AS column_name,
    NULL AS definition
FROM sys.key_constraints AS kc
INNER JOIN sys.tables AS t ON kc.parent_object_id = t.object_id
INNER JOIN sys.schemas AS s ON t.schema_id = s.schema_id
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
