-- name: list
-- dialect: duckdb
SELECT
    database_name,
    schema_name,
    internal,
    sql AS native_sql
FROM duckdb_schemas()
WHERE NOT internal
ORDER BY database_name, schema_name;
