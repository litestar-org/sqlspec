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

-- name: by_table
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

-- name: foreign_keys_by_schema
-- dialect: mssql
SELECT
    OBJECT_SCHEMA_NAME(fk.parent_object_id) AS schema_name,
    fk.name AS constraint_name,
    OBJECT_NAME(fk.parent_object_id) AS table_name,
    parent_col.name AS column_name,
    OBJECT_SCHEMA_NAME(fk.referenced_object_id) AS referenced_schema,
    OBJECT_NAME(fk.referenced_object_id) AS referenced_table,
    referenced_col.name AS referenced_column,
    fk.delete_referential_action_desc,
    fk.update_referential_action_desc,
    CAST(fk.is_not_trusted AS BIT) AS is_not_trusted
FROM sys.foreign_keys AS fk
INNER JOIN sys.foreign_key_columns AS fkc ON fk.object_id = fkc.constraint_object_id
INNER JOIN sys.columns AS parent_col
    ON fkc.parent_object_id = parent_col.object_id AND fkc.parent_column_id = parent_col.column_id
INNER JOIN sys.columns AS referenced_col
    ON fkc.referenced_object_id = referenced_col.object_id AND fkc.referenced_column_id = referenced_col.column_id
WHERE (:schema_name IS NULL OR OBJECT_SCHEMA_NAME(fk.parent_object_id) = :schema_name)
ORDER BY OBJECT_SCHEMA_NAME(fk.parent_object_id), OBJECT_NAME(fk.parent_object_id), fk.name, fkc.constraint_column_id;

-- name: foreign_keys_by_table
-- dialect: mssql
SELECT
    OBJECT_SCHEMA_NAME(fk.parent_object_id) AS schema_name,
    fk.name AS constraint_name,
    OBJECT_NAME(fk.parent_object_id) AS table_name,
    parent_col.name AS column_name,
    OBJECT_SCHEMA_NAME(fk.referenced_object_id) AS referenced_schema,
    OBJECT_NAME(fk.referenced_object_id) AS referenced_table,
    referenced_col.name AS referenced_column,
    fk.delete_referential_action_desc,
    fk.update_referential_action_desc,
    CAST(fk.is_not_trusted AS BIT) AS is_not_trusted
FROM sys.foreign_keys AS fk
INNER JOIN sys.foreign_key_columns AS fkc ON fk.object_id = fkc.constraint_object_id
INNER JOIN sys.columns AS parent_col
    ON fkc.parent_object_id = parent_col.object_id AND fkc.parent_column_id = parent_col.column_id
INNER JOIN sys.columns AS referenced_col
    ON fkc.referenced_object_id = referenced_col.object_id AND fkc.referenced_column_id = referenced_col.column_id
WHERE OBJECT_SCHEMA_NAME(fk.parent_object_id) = :schema_name
  AND OBJECT_NAME(fk.parent_object_id) = :table_name
ORDER BY fk.name, fkc.constraint_column_id;
