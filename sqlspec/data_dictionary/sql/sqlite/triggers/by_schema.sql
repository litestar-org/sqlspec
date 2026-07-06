-- name: by_schema
-- dialect: sqlite
SELECT
    :schema_name AS schema_name,
    name AS trigger_name,
    tbl_name AS table_name,
    sql AS native_sql
FROM {schema_prefix}sqlite_schema
WHERE type = 'trigger'
ORDER BY name;
