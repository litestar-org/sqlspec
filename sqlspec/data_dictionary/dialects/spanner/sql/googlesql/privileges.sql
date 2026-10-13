-- name: by_schema
-- dialect: spanner
SELECT
    TABLE_CATALOG AS table_catalog,
    TABLE_SCHEMA AS table_schema,
    TABLE_NAME AS table_name,
    PRIVILEGE_TYPE AS privilege_type,
    GRANTEE AS grantee,
    IS_GRANTABLE AS is_grantable
FROM INFORMATION_SCHEMA.TABLE_PRIVILEGES
WHERE (CAST(:schema_name AS STRING) IS NULL OR TABLE_SCHEMA = :schema_name)
  AND (CAST(:table_name AS STRING) IS NULL OR TABLE_NAME = :table_name)
ORDER BY TABLE_SCHEMA, TABLE_NAME, PRIVILEGE_TYPE, GRANTEE;
