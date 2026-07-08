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
