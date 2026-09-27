-- name: by_schema
-- dialect: spanner
SELECT
    TABLE_CATALOG AS table_catalog,
    TABLE_SCHEMA AS schema_name,
    TABLE_NAME AS table_name,
    TABLE_TYPE AS table_type,
    PARENT_TABLE_NAME AS parent_table_name,
    ON_DELETE_ACTION AS on_delete_action,
    SPANNER_STATE AS spanner_state,
    ROW_DELETION_POLICY_EXPRESSION AS row_deletion_policy_expression
FROM INFORMATION_SCHEMA.TABLES
WHERE (CAST(:schema_name AS STRING) IS NULL OR TABLE_SCHEMA = :schema_name)
ORDER BY TABLE_SCHEMA, TABLE_NAME;

-- name: by_table
-- dialect: spanner
SELECT
    TABLE_CATALOG AS table_catalog,
    TABLE_SCHEMA AS schema_name,
    TABLE_NAME AS table_name,
    TABLE_TYPE AS table_type,
    PARENT_TABLE_NAME AS parent_table_name,
    ON_DELETE_ACTION AS on_delete_action,
    SPANNER_STATE AS spanner_state,
    ROW_DELETION_POLICY_EXPRESSION AS row_deletion_policy_expression
FROM INFORMATION_SCHEMA.TABLES
WHERE (CAST(:schema_name AS STRING) IS NULL OR TABLE_SCHEMA = :schema_name)
  AND (CAST(:table_name AS STRING) IS NULL OR TABLE_NAME = :table_name)
ORDER BY TABLE_SCHEMA, TABLE_NAME;
