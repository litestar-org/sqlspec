-- name: foreign_keys_by_table
-- dialect: mssql
SELECT
    OBJECT_SCHEMA_NAME(fk.parent_object_id) AS schema_name,
    fk.name AS constraint_name,
    OBJECT_NAME(fk.parent_object_id) AS table_name,
    cpa.name AS column_name,
    OBJECT_SCHEMA_NAME(fk.referenced_object_id) AS referenced_schema,
    OBJECT_NAME(fk.referenced_object_id) AS referenced_table,
    cref.name AS referenced_column
FROM sys.foreign_keys fk
INNER JOIN sys.foreign_key_columns fkc ON fk.object_id = fkc.constraint_object_id
INNER JOIN sys.columns cpa
    ON fkc.parent_object_id = cpa.object_id AND fkc.parent_column_id = cpa.column_id
INNER JOIN sys.columns cref
    ON fkc.referenced_object_id = cref.object_id AND fkc.referenced_column_id = cref.column_id
WHERE OBJECT_SCHEMA_NAME(fk.parent_object_id) = :schema_name
  AND OBJECT_NAME(fk.parent_object_id) = :table_name
ORDER BY fk.name, fkc.constraint_column_id;

-- name: foreign_keys_by_schema
-- dialect: mssql
SELECT
    OBJECT_SCHEMA_NAME(fk.parent_object_id) AS schema_name,
    fk.name AS constraint_name,
    OBJECT_NAME(fk.parent_object_id) AS table_name,
    cpa.name AS column_name,
    OBJECT_SCHEMA_NAME(fk.referenced_object_id) AS referenced_schema,
    OBJECT_NAME(fk.referenced_object_id) AS referenced_table,
    cref.name AS referenced_column
FROM sys.foreign_keys fk
INNER JOIN sys.foreign_key_columns fkc ON fk.object_id = fkc.constraint_object_id
INNER JOIN sys.columns cpa
    ON fkc.parent_object_id = cpa.object_id AND fkc.parent_column_id = cpa.column_id
INNER JOIN sys.columns cref
    ON fkc.referenced_object_id = cref.object_id AND fkc.referenced_column_id = cref.column_id
WHERE (:schema_name IS NULL OR OBJECT_SCHEMA_NAME(fk.parent_object_id) = :schema_name)
ORDER BY OBJECT_SCHEMA_NAME(fk.parent_object_id), OBJECT_NAME(fk.parent_object_id), fk.name, fkc.constraint_column_id;
