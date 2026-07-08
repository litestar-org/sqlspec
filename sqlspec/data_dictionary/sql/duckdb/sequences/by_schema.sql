-- name: by_schema
-- dialect: duckdb
SELECT
    database_name,
    schema_name,
    sequence_name,
    temporary,
    start_value,
    min_value,
    max_value,
    increment_by,
    cycle,
    last_value,
    comment,
    tags,
    sql AS native_sql
FROM duckdb_sequences()
WHERE schema_name = COALESCE(:schema_name, current_schema())
ORDER BY sequence_name;
