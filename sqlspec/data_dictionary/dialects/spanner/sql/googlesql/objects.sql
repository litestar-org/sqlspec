-- name: by_schema
-- dialect: spanner
SELECT
    TABLE_CATALOG AS table_catalog,
    TABLE_SCHEMA AS table_schema,
    TABLE_NAME AS table_name,
    TABLE_TYPE AS table_type
FROM INFORMATION_SCHEMA.TABLES
WHERE (CAST(:schema_name AS STRING) IS NULL OR TABLE_SCHEMA = :schema_name)
ORDER BY TABLE_SCHEMA, TABLE_NAME;
