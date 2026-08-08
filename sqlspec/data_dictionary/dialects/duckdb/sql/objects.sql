-- name: by_schema
-- dialect: duckdb
SELECT
    database_name,
    schema_name,
    table_name AS object_name,
    'table' AS object_type,
    comment,
    tags,
    internal,
    temporary,
    sql AS native_sql
FROM duckdb_tables()
WHERE schema_name = COALESCE(:schema_name, current_schema())
  AND NOT internal
UNION ALL
SELECT
    database_name,
    schema_name,
    view_name AS object_name,
    'view' AS object_type,
    comment,
    tags,
    internal,
    temporary,
    sql AS native_sql
FROM duckdb_views()
WHERE schema_name = COALESCE(:schema_name, current_schema())
  AND NOT internal
ORDER BY object_type, object_name;
