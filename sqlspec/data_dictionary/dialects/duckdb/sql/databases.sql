-- name: list
-- dialect: duckdb
SELECT
    database_name,
    path,
    type,
    readonly,
    internal
FROM duckdb_databases()
WHERE NOT internal
ORDER BY database_name;
