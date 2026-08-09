-- name: by_schema
-- dialect: sqlite
SELECT
    :schema_name AS schema_name,
    name AS view_name,
    tbl_name AS table_name,
    sql AS native_sql
FROM {schema_prefix}sqlite_schema
WHERE type = 'view'
ORDER BY name;
