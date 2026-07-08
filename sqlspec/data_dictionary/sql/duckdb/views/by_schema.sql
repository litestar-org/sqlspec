-- name: by_schema
-- dialect: duckdb
SELECT
    database_name,
    schema_name,
    view_name,
    comment,
    tags,
    internal,
    temporary,
    sql AS native_sql
FROM duckdb_views()
WHERE schema_name = COALESCE(:schema_name, current_schema())
  AND NOT internal
ORDER BY view_name;
