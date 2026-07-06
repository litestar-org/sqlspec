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
