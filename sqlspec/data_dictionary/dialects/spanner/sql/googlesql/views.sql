-- name: by_schema
-- dialect: spanner
SELECT
    TABLE_CATALOG AS table_catalog,
    TABLE_SCHEMA AS table_schema,
    TABLE_NAME AS table_name,
    VIEW_DEFINITION AS view_definition,
    CHECK_OPTION AS check_option,
    SECURITY_TYPE AS security_type
FROM INFORMATION_SCHEMA.VIEWS
WHERE (CAST(:schema_name AS STRING) IS NULL OR TABLE_SCHEMA = :schema_name)
  AND (CAST(:view_name AS STRING) IS NULL OR TABLE_NAME = :view_name)
ORDER BY TABLE_SCHEMA, TABLE_NAME;
