-- name: logs
-- dialect: duckdb
SELECT *
FROM duckdb_logs();

-- name: memory
-- dialect: duckdb
SELECT *
FROM duckdb_memory();

-- name: settings
-- dialect: duckdb
SELECT
    name,
    value,
    description,
    input_type,
    scope,
    aliases
FROM duckdb_settings()
ORDER BY name;
