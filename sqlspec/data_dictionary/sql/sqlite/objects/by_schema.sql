-- name: by_schema
-- dialect: sqlite
SELECT
    :schema_name AS schema_name,
    name AS object_name,
    type AS object_type,
    tbl_name AS table_name,
    rootpage,
    sql AS native_sql
FROM {schema_prefix}sqlite_schema
WHERE type IN ('table', 'index', 'view', 'trigger')
  AND name NOT LIKE 'sqlite_%'
ORDER BY type, name;
